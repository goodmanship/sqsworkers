import atexit
import itertools as it
import json
import logging
import os
import time
import warnings
from concurrent import futures
from functools import partial, wraps
from threading import Thread
from typing import *

import boto3
from boto3.resources.base import ServiceResource
from dataclasses import dataclass, asdict, InitVar

from sqsworkers import interfaces
from sqsworkers.base import StatsDBase


@dataclass
class MessageMetadata:
    """
    Defines the metadata received from a message's body.
    """

    message: InitVar[Any]
    event_id: str
    event_type: str
    event_schema: Any

    def __post_init__(self, message: Any):
        body: dict = json.loads(message.body)
        self.event_id = body.get("eventId")
        self.event_type = body.get("type")
        self.schema = body.get("schema")


class Crew(interfaces.CrewInterface):
    """Provide the top-level interface to Crew."""

    def __init__(self, *args, bulk_mode=False, **kwargs):
        """Instantiate a daemon thread with either a regular or bulk listener."""
        logging.info(
            "instantiating background thread with {} listener".format(
                "non-bulk" if not bulk_mode else "bulk"
            )
        )
        self.listener = (
            Listener(*args, **kwargs)
            if not bulk_mode
            else BulkListener(*args, **kwargs)
        )
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


class Listener(interfaces.CrewInterface):
    """
    This class polls on an sqs queue, delegating the work to a message processor that runs in a threadpool.
    """

    def __new__(cls, *args, worker_limit: Optional[int] = None, **kwargs):
        """
        Ensures we only create one threadpool executor per class.

        Also ensures our start method propogates exceptions to sentry.
        """

        def alert_sentry(method, sentry=None):
            """
            Wraps a method such that if it raises an exception, sentry is alerted.
            """

            @wraps(method)
            def inner(*args, **kwargs):
                if sentry is None:
                    logging.warning(
                        f"sentry is None. exceptions raised from {method.__name__} will not be sent to sentry"
                    )
                    return method(*args, **kwargs)
                else:
                    try:
                        return method(*args, **kwargs)
                    except Exception:
                        sentry.captureException()
                        raise

            return inner

        if not hasattr(cls, "_executor"):
            cls._executor = futures.ThreadPoolExecutor(
                max_workers=worker_limit
            )
            atexit.register(cls._executor.shutdown)

        if not hasattr(cls, "_count"):
            cls._count = it.count(1)

        obj = super().__new__(cls)

        obj.start = alert_sentry(obj.start, sentry=kwargs.get("sentry"))

        return obj

    def __init__(
        self,
        sqs_session: boto3.Session,
        logger: logging.Logger,
        MessageProcessor,
        # the following arguments are deprecated and will be ignored
        workers=None,
        supervisor=None,
        exception_handler_function=None,
        bulk_mode=None,
        # end of deprecated arguments
        queue_name: Optional[str] = None,
        sqs_resource: Optional[ServiceResource] = None,
        name: Optional[str] = None,
        statsd=None,
        sentry=None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        polling_interval: Union[int, float] = 1,
        **kwargs,
    ):
        """

        Args:
            sqs_session: boto3 Session object
            logger: for logging
            MessageProcessor: transforms/loads the data from sqs
            workers: deprecated
            supervisor: deprecated
            exception_handler_function: deprecated
            bulk_mode: deprecated
            queue_name: the name of the queue we want to listen to if not given an sqs_resource
            sqs_resource: a boto3 sqs Queue object
            name: the name of the Crew, for logging
            statsd: statsd client
            sentry: sentry client
            max_number_of_messages: passed to self.queue.receive_messages(MaxNumberOfMessages=...)
            wait_time: passed to self.queue.receive_messages(WaitTimeSeconds=...)
            polling_interval: How long to wait in between polls on sqs
        """

        deprecated = [
            "workers",
            "supervisor",
            "exception_handler_function",
            "bulk_mode",
        ]

        for d in deprecated:
            if locals().get(d) is not None:
                warnings.warn(
                    f"The {d} argument is deprecated and will be ignored.",
                    DeprecationWarning,
                )

        self.sqs_session = sqs_session

        assert not (
            sqs_resource is None and queue_name is None
        ), "You must pass either the queue name or sqs_resource"

        self.queue = self.sqs_resource = (
            sqs_session.resource(
                "sqs", region_name=sqs_session.region_name
            ).get_queue_by_name(QueueName=queue_name)
            if sqs_resource is None
            else sqs_resource
        )

        self.name = name or "crew-{}-{}-{}".format(
            os.getpid(), (queue_name or next(self._count)), time.time()
        )

        self.logger = logging.LoggerAdapter(
            logger, extra={"extra": {"crew.name": self.name}}
        )

        assert issubclass(
            MessageProcessor, interfaces.CrewInterface
        ), f"{MessageProcessor.__name__} does not conform to {interfaces.CrewInterface.__name__}"

        self.message_processor = self.MessageProcessor = MessageProcessor

        statsd = StatsDBase(logger=logger) if statsd is None else statsd

        assert issubclass(
            statsd.__class__, interfaces.StatsDInterface
        ), f"{statsd.__class__} does not conform to {interfaces.StatsDInterface.__name__}"

        self.statsd = statsd

        self.sentry = sentry
        self.max_number_of_messages = max_number_of_messages
        self.wait_time = wait_time
        self.polling_interval = polling_interval

    def start(self):

        while True:

            messages = self.queue.receive_messages(
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=self.max_number_of_messages,
                WaitTimeSeconds=self.wait_time,
            )

            if messages:

                self.logger.info(
                    f"processing the following {len(messages)} messages: {messages}"
                )

                for message in messages:

                    task: futures.Future = self._executor.submit(
                        self.message_processor(message).start
                    )

                    task.add_done_callback(
                        partial(self._task_complete, message=message)
                    )

                    self.statsd.increment("process.record.start", 1, tags=[])

            time.sleep(self.polling_interval)

    def _task_complete(self, f: futures.Future, message):
        """
        Clean up after the task and do any necessary logging.
        """

        exception = f.exception()

        if exception is not None:
            metadata = MessageMetadata(message)
            self.logger.error(
                "{exception} raised on the following message: {message}".format(
                    exception=exception, message=asdict(metadata)
                )
            )
            self.statsd.increment("process.record.failure", 1, tags=[])
        else:
            self.statsd.increment("process.record.success", 1, tags=[])
            self.queue.delete_messages(
                Entries=[
                    {
                        "Id": message.message_id,
                        "ReceiptHandle": message.receipt_handle,
                    }
                ]
            )


class BulkListener(Listener):
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
            timeout: if we set minimum messages, this is how long we'll keep trying to poll on sqs to get
                that number of messages
        """
        super().__init__(*args, **kwargs)
        self.minimum_messages = minimum_messages
        self.timeout = timeout

        if minimum_messages and self.wait_time > self.timeout:
            logging.warning(
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
                self.logger.info(
                    f"processing the following {len(messages)} messages in bulk: {messages}"
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

    def _task_complete(self, f: futures.Future, messages):
        """Clean up after task and do any necessary logging."""

        exception = f.exception()

        if exception is not None:
            metadata: List[MessageMetadata] = [
                MessageMetadata(m) for m in messages
            ]
            self.logger.error(
                "{exception} raised on the following group of messages: {messages}".format(
                    exception=exception, messages=[asdict(m) for m in metadata]
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
