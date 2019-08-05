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
from sqsworkers.bounded_executor import BoundedThreadPoolExecutor


class BaseListener(interfaces.CrewInterface):
    """
    This class polls on an sqs queue, delegating the work to a message processor that runs in a threadpool.
    """

    # > Warning the first time this function is called with interval = 0.0 or None
    # it will return a meaningless 0.0 value which you are supposed to ignore.
    psutil.cpu_percent()

    def __new__(cls, executor=None, *args, **kwargs):
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
                executor
                if executor is not None
                else BoundedThreadPoolExecutor()
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
        executor=None,
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
            executor: a concurrent.futures.Executor-like instance (must have a submit method that
                                                                   returns a future
                                                                   )
        """
        if executor is not None:
            self._executor = executor

        xor_msg_proc = bool(MessageProcessor) ^ bool(message_processor)

        xor_error_msg = f"message_processor and MessageProcessor arguments are mutually exclusive"

        assert xor_msg_proc, xor_error_msg

        message_processor = message_processor or MessageProcessor

        assert callable(message_processor) or issubclass(
            message_processor, interfaces.CrewInterface
        ), f"{message_processor.__name__} does not conform to {interfaces.CrewInterface.__name__} and isn't callable"

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

                    function = (
                        self.message_processor(message).start
                        if hasattr(self.message_processor, "start")
                        else partial(self.message_processor, message)
                    )

                    task: futures.Future = self._executor.submit(function)

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

            self.queue.delete_messages(
                Entries=[
                    {
                        "Id": message.message_id,
                        "ReceiptHandle": message.receipt_handle,
                    }
                ]
            )


class BulkListener(BaseListener):
    """
    This class is identical to the regular crew, with the exception
    that it will pass a list of messages to its message processor, as
    opposed to each message individually.
    """

    def __init__(
        self,
        minimum_messages: Optional[int] = None,
        max_number_of_messages: Optional[int] = None,
        timeout: int = 30,
        *args,
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

                function = (
                    self.message_processor(messages).start
                    if hasattr(self.message_processor, "start")
                    else partial(self.message_processor, messages)
                )

                task: futures.Future = self._executor.submit(function)

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

                self.queue.delete_messages(
                    Entries=[
                        {
                            "Id": message.message_id,
                            "ReceiptHandle": message.receipt_handle,
                        }
                        for message in it.islice(
                            successfully_processed_messages, 10
                        )
                    ]
                )

                successfully_processed_messages = list(
                    successfully_processed_messages
                )


class StatsDBase(interfaces.StatsDInterface):
    """Implements the base statsd interface."""

    def __init__(self, logger=None):
        self.logger = logging.getLogger() if logger is None else logger

    def increment(self, *args, **kwargs):
        self.logger.info(f"statsd increment invoked: {args}, {kwargs}")
