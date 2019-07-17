import itertools as it
import logging
import time
from concurrent import futures
from functools import partial
from threading import Thread
from typing import *

import psutil

from sqsworkers import MessageMetadata
from sqsworkers import interfaces
from sqsworkers.base import BaseListener


class Crew(interfaces.CrewInterface):
    """Provide the top-level interface to Crew."""

    def __init__(
        self,
        *args,
        worker_limit: int = 32,
        listeners=None,
        bulk_mode=False,
        executor=None,
        **kwargs,
    ):
        """Instantiate a daemon thread with either a regular or bulk listener."""
        logging.info(
            "instantiating background thread with {} listener".format(
                "non-bulk" if not bulk_mode else "bulk"
            )
        )

        self.executor = executor

        self._listeners = (
            [
                (
                    BaseListener(*args, executor=executor, **kwargs)
                    if not bulk_mode
                    else BulkListener(*args, executor=executor, **kwargs)
                )
                for _ in range(worker_limit)
            ]
            if listeners is None
            else listeners
        )

        self._daemons = [
            Thread(name=listener.name, target=listener.start, daemon=True)
            for listener in self._listeners
        ]

    def start(self):
        """Start listener in background thread."""
        logging.info("starting background listener thread")
        for daemon in self._daemons:
            daemon.start()

    def join(self, timeout=0.1):
        logging.info("waiting on background thread to finish")
        for daemon in self._daemons:
            daemon.join(timeout=timeout)

    def stop(self, timeout=0.1):
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
        minimum_messages: Optional[int] = None,
        max_number_of_messages: Optional[int] = None,
        timeout: int = 30,
        **kwargs,
    ):
        """

        Args:
            minimum_messages: The minimum number of messages we want to pass to the message processor
            timeout: if we set minimum messages, this is how many seconds we'll keep trying to poll on sqs to get
                that number of messages
            max_number_of_messages: passed to self.queue.receive_messages(MaxNumberOfMessages=...)
        """
        if max_number_of_messages is None and minimum_messages is None:
            max_number_of_messages = 10
        elif minimum_messages:
            max_number_of_messages = minimum_messages

        super().__init__(
            *args, max_number_of_messages=max_number_of_messages, **kwargs
        )

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

            cpu_usage_percent = psutil.cpu_percent()
            memory_usage_percent = psutil.virtual_memory().percent

            if cpu_usage_percent >= 85 or memory_usage_percent >= 85:
                logging.debug(
                    "(cpu,memory) usage at ({cpu_usage_percent},{memory_usage_percent}) -- skipping poll on sqs".format(
                        **locals()
                    )
                )
                continue

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

                metadata = [MessageMetadata(m) for m in messages]

                self.logger.info(
                    f"processing the following {len(messages)} messages in bulk: {metadata}"
                )

                task: futures.Future = self._executor.submit(
                    self.message_processor(messages).start
                )

                task.add_done_callback(
                    partial(
                        self._task_complete,
                        messages=messages,
                        metadata=metadata,
                    )
                )

                self.statsd.increment(
                    "process.record.start", len(messages), tags=[]
                )

            time.sleep(self.polling_interval)

    def _task_complete(
        self,
        f: futures.Future,
        messages: Iterable[Any],
        metadata=Optional[List[MessageMetadata]],
    ):
        """Clean up after task and do any necessary logging."""

        messages = list(messages)

        metadata: List[MessageMetadata] = [
            MessageMetadata(m) for m in messages
        ] if metadata is None else metadata

        exception = f.exception()

        if exception is not None:

            self.logger.error(
                "failed processing {messages} with the following exception: {exception}".format(
                    exception=repr(exception), messages=metadata
                ),
                exc_info=exception,
            )

            self.statsd.increment(
                "process.record.failure", len(messages), tags=[]
            )

        else:

            result = f.result()

            self.logger.info(
                "successfully processed {} messages: {metadata}".format(
                    len(result.succeeded),
                    metadata=[MessageMetadata(m) for m in result.succeeded],
                )
            )

            self.statsd.increment(
                "process.record.success", len(result.succeeded), tags=[]
            )

            if result.failed:

                self.logger.error(
                    "failed to process {} messages: {metadata}".format(
                        len(result.failed),
                        metadata=[MessageMetadata(m) for m in result.failed],
                    )
                )

                self.statsd.increment(
                    "process.record.failure", len(result.failed), tags=[]
                )

            # make sure we don't try to delete more than 10 messages
            # at a time or we'll get an error from boto3

            successfully_processed_messages = result.succeeded

            while successfully_processed_messages:

                successfully_processed_messages = iter(
                    successfully_processed_messages
                )

                self._executor.submit(
                    partial(
                        self.queue.delete_messages,
                        Entries=[
                            {
                                "Id": message.message_id,
                                "ReceiptHandle": message.receipt_handle,
                            }
                            for message in it.islice(
                                successfully_processed_messages, 10
                            )
                        ],
                    )
                )

                successfully_processed_messages = list(
                    successfully_processed_messages
                )
