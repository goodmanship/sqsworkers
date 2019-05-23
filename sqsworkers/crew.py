import atexit
import itertools as it
import json
import logging
import os
import time
import warnings
from concurrent import futures
from functools import partial, wraps
from typing import *

import boto3
from boto3.resources.base import ServiceResource
from dataclasses import dataclass, asdict, InitVar

from sqsworkers import interfaces
from sqsworkers.base import StatsDBase


def _alert_sentry(method):
    """
    Wraps a method such that if it raises an exception, sentry is alerted.

    The decorated method must be an instance method.

    The instance must also have a sentry attribute.
    """

    @wraps(method)
    def inner(self, *args, **kwargs):
        if self.sentry is None:
            return method(self, *args, **kwargs)
        else:
            try:
                return method(self, *args, **kwargs)
            except Exception:
                self.sentry.captureException()
                raise

    return inner


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
    """
    This class polls on an sqs queue, delegating the work to a message processor that runs in a threadpool.
    """

    def __new__(cls, *args, worker_limit: Optional[int] = None, **kwargs):
        """
        Ensures we only create one threadpool executor per class.
        """
        if not hasattr(cls, "_executor"):
            cls._executor = futures.ThreadPoolExecutor(
                max_workers=worker_limit
            )
            atexit.register(cls._executor.shutdown)

        if not hasattr(cls, "_count"):
            cls._count = it.count(1)

        return super().__new__(cls)

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
        polling_interval: Union[int, float] = 1.5,
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

    @_alert_sentry
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
                        self.message_processor, message
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


class BulkCrew(Crew):
    """
    This class is identical to the regular crew, with the exception
    that it will pass a list of messages to its message processor, as
    opposed to each message individually.
    """

    def __init__(
        self,
        minimum_messages: Optional[int] = 10,
        timeout: int = 30,
        *args,
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

    @_alert_sentry
    def start(self):
        while True:
            if self.minimum_messages:
                messages = []
                start = time.perf_counter()
                while (
                    len(messages) < self.minimum_messages
                    and (time.perf_counter() - start) < self.timeout
                ):
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
                    self.message_processor, messages
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
