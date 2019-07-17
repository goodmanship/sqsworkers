import atexit
import itertools as it
import logging
import os
import time
import warnings
from concurrent import futures
from functools import partial, wraps
from typing import *

import boto3
import psutil
from boto3.resources.base import ServiceResource

from sqsworkers import MessageMetadata
from sqsworkers import interfaces


class BaseListener(interfaces.CrewInterface):
    """
    This class polls on an sqs queue, delegating the work to a message processor that runs in a threadpool.
    """

    # > Warning the first time this function is called with interval = 0.0 or None
    # it will return a meaningless 0.0 value which you are supposed to ignore.
    psutil.cpu_percent()

    def __new__(cls, *args, executor=None, **kwargs):
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
            cls._executor = (
                futures.ThreadPoolExecutor() if executor is None else executor
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
        # the following arguments are deprecated
        workers=None,
        supervisor=None,
        exception_handler_function=None,
        MessageProcessor: Optional[Callable[[Any], Optional[Any]]] = None,
        # end of deprecated arguments
        message_processor: Optional[Callable[[Any], Any]] = None,
        logger: Optional[logging.Logger] = None,
        queue_name: Optional[str] = None,
        sqs_resource: Optional[ServiceResource] = None,
        name: Optional[str] = None,
        statsd=None,
        sentry=None,
        max_number_of_messages: int = 10,
        wait_time: int = 20,
        polling_interval: Union[int, float] = 0,
        log_level: Optional[int] = None,
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
            log_level: the logging level for this instance's logger
        """

        xor_msg_proc = bool(MessageProcessor) ^ bool(message_processor)

        xor_error_msg = f"message_processor and MessageProcessor arguments are mutually exclusive"

        assert xor_msg_proc, xor_error_msg

        message_processor = message_processor or MessageProcessor

        assert issubclass(
            message_processor, interfaces.CrewInterface
        ), f"{message_processor.__name__} does not conform to {interfaces.CrewInterface.__name__}"

        self.message_processor = message_processor

        if MessageProcessor is not None:
            warnings.warn(
                os.linesep.join(
                    [
                        "MessageProcessor argument will be deprecated in future version",
                        "please use message_processor instead",
                    ]
                ),
                DeprecationWarning,
            )

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

        log_level = (
            log_level
            if log_level is not None
            else (logging.INFO if logger is None else logger.level)
        )

        self.logger = logging.LoggerAdapter(
            (logging.getLogger() if logger is None else logger),
            extra={"extra": {"crew.name": self.name}},
        )

        self.logger.setLevel(log_level)

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

            cpu_usage_percent = psutil.cpu_percent()
            memory_usage_percent = psutil.virtual_memory().percent

            if cpu_usage_percent >= 85 or memory_usage_percent >= 85:
                logging.debug(
                    "(cpu,memory) usage at ({cpu_usage_percent},{memory_usage_percent}) -- skipping poll on sqs".format(
                        **locals()
                    )
                )
                continue

            messages = self.queue.receive_messages(
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=self.max_number_of_messages,
                WaitTimeSeconds=self.wait_time,
            )

            if messages:

                metadatas = [MessageMetadata(m) for m in messages]

                self.logger.info(
                    f"processing the following {len(messages)} messages: {metadatas}"
                )

                for message in messages:
                    metadata = MessageMetadata(message)

                    self.logger.info(f"processing message: {metadata}")

                    task: futures.Future = self._executor.submit(
                        self.message_processor(message).start
                    )

                    self.logger.info(f"processing task: {task}")

                    task.add_done_callback(
                        partial(
                            self._task_complete,
                            message=message,
                            metadata=metadata,
                        )
                    )

                    self.statsd.increment("process.record.start", 1, tags=[])

            time.sleep(self.polling_interval)

    def _task_complete(
        self,
        f: futures.Future,
        message: Any,
        metadata: Optional[MessageMetadata] = None,
    ):
        """
        Clean up after the task and do any necessary logging.
        """

        metadata = MessageMetadata(message) if metadata is None else metadata

        exception: Optional[Exception] = f.exception()

        if exception is not None:

            self.logger.error(
                "failed processing {message} with the following exception: {exception}".format(
                    exception=repr(exception), message=metadata
                ),
                exc_info=exception,
            )

            self.statsd.increment("process.record.failure", 1, tags=[])

        else:

            self.logger.info(
                "successfully processed {message}".format(message=metadata)
            )

            self.statsd.increment("process.record.success", 1, tags=[])

            self._executor.submit(message.delete)


class StatsDBase(interfaces.StatsDInterface):
    """Implements the base statsd interface."""

    def __init__(self, logger=None):
        self.logger = logging.getLogger() if logger is None else logger

    def increment(self, *args, **kwargs):
        self.logger.info(f"statsd increment invoked: {args}, {kwargs}")
