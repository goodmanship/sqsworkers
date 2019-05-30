import itertools as it
import logging
import time
from concurrent import futures
from functools import partial
from threading import Thread
from typing import *

from sqsworkers import MessageMetadata
from sqsworkers import interfaces
from sqsworkers.base import BaseListener


class Crew(interfaces.CrewInterface):
    """Provide the top-level interface to Crew."""

    def __init__(self, *args, listener=None, bulk_mode=False, **kwargs):
        """Instantiate a daemon thread with either a regular or bulk listener."""
        logging.info(
            "instantiating background thread with {} listener".format(
                "non-bulk" if not bulk_mode else "bulk"
            )
        )

        self.listener = (
            BaseListener(*args, **kwargs)
            if not bulk_mode
            else BulkListener(*args, **kwargs)
        ) if listener is None else listener

        self._thread = Thread(
            name=self.listener.name, target=self.listener.start, daemon=True
        )

    def start(self):
        """Start listener in background thread."""
        logging.info("starting background listener thread")
        self._thread.start()

    def join(self, timeout=None):
        logging.info("waiting on background thread to finish")
        self._thread.join(timeout=timeout)

    def stop(self, timeout=None):
        self.join(timeout=timeout)


class BulkListener(BaseListener):
    """
    This class is identical to the regular crew, with the exception
    that it will pass a list of messages to its message processor, as
    opposed to each message individually.
    """

    def __init__(
        self,
        *args,
        minimum_messages: Optional[int] = 10,
        timeout: int = 30,
        **kwargs,
    ):
        """

        Args:
            minimum_messages: The minimum number of messages we want to pass to the message processor
            timeout: if we set minimum messages, this is how many seconds we'll keep trying to poll on sqs to get
                that number of messages
        """
        super().__init__(*args, **kwargs)

        self.minimum_messages = minimum_messages
        self.timeout = timeout

        if minimum_messages and self.wait_time > self.timeout:
            self.logger.warning(
                f"the wait time ({self.wait_time}) is longer than the timeout ({self.timeout}) "
                f"meaning sqs will be long-polled only once to attempt to get the minimum number "
                f"of messages ({minimum_messages}"
            )

    def start(self):
        while True:

            if self.minimum_messages:
                messages = []
                start = time.perf_counter()

                while (
                    len(messages) < self.minimum_messages
                    and (time.perf_counter() - start) < self.timeout
                ):

                    self.logger.info(
                        "length of messages ({}) below minimum set to pass to message processor ({}). polling".format(
                            len(messages), self.minimum_messages
                        )
                    )

                    messages += self.queue.receive_messages(
                        AttributeNames=["All"],
                        MessageAttributeNames=["All"],
                        MaxNumberOfMessages=self.max_number_of_messages,
                        WaitTimeSeconds=self.wait_time,
                    )

            else:

                messages = self.queue.receive_messages(
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=self.max_number_of_messages,
                    WaitTimeSeconds=self.wait_time,
                )

            if messages:

                metadatas = [MessageMetadata(m) for m in messages]

                self.logger.info(
                    f"processing the following {len(messages)} messages in bulk: {metadatas}"
                )

                task: futures.Future = self._executor.submit(
                    self.message_processor(messages).start
                )

                task.add_done_callback(
                    partial(self._task_complete, messages=messages)
                )

                self.statsd.increment(
                    "process.record.start", len(messages), tags=[]
                )

            time.sleep(self.polling_interval)

    def _task_complete(self, f: futures.Future, messages: Iterable[Any]):
        """Clean up after task and do any necessary logging."""

        messages = list(messages)

        exception = f.exception()

        if exception is not None:
            metadata: List[MessageMetadata] = [
                MessageMetadata(m) for m in messages
            ]

            self.logger.error(
                "{exception} raised on the following group of messages: {messages}".format(
                    exception=repr(exception), messages=metadata
                )
            )

            self.statsd.increment(
                "process.record.failure", len(messages), tags=[]
            )

        else:

            self.statsd.increment(
                "process.record.success", len(messages), tags=[]
            )

            # make sure we don't try to delete more than 10 messages
            # at a time or we'll get an error from boto3
            while messages:

                messages = iter(messages)

                self.queue.delete_messages(
                    Entries=[
                        {
                            "Id": message.message_id,
                            "ReceiptHandle": message.receipt_handle,
                        }
                        for message in it.islice(messages, 10)
                    ]
                )

                messages = list(messages)
