import atexit
import itertools as it
import json
import logging
import os
import time
import warnings
from concurrent import futures
from functools import partial
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
    """
    Poll on sqs queue and do stuff.
    """

    def __new__(cls, worker_limit: Optional[int] = None, *args, **kwargs):
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

        return super().__new__(cls, worker_limit=worker_limit, *args, **kwargs)

    def __init__(
        self,
        sqs_session: boto3.Session,
        logger: logging.Logger,
        MessageProcessor,
        # the following arguments are deprecated and will be ignored
        workers=None,
        supervisor=None,
        exception_handler_function=None,
        # end of deprecated arguments
        queue_name: Optional[str] = None,
        sqs_resource: Optional[ServiceResource] = None,
        name: Optional[str] = None,
        statsd=None,
        sentry=None,
        max_number_of_messages: int = 1,
        wait_time: int = 20,
        # bulk_mode: bool = False,
        # chunks_per_bulk: Optional[int] = None,
        polling_interval: Union[int, float] = 1.5,
    ):

        deprecated = ["workers", "supervisor", "exception_handler_function"]

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

        assert f.done()

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
        wait_time: int = 10,
        *args,
        **kwargs,
    ):
        super().__init__(wait_time=wait_time, *args, **kwargs)
        self.minimum_messages = minimum_messages
        self.timeout = timeout

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

    def _task_complete(self, f: futures.Future, messages):
        """Clean up after task and do any necessary logging."""

        assert f.done()

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
            self.queue.delete_messages(
                Entries=[
                    {
                        "Id": message.message_id,
                        "ReceiptHandle": message.receipt_handle,
                    }
                    for message in messages
                ]
            )
