import json
from types import SimpleNamespace
from unittest import mock

import pytest

from sqsworkers.base import StatsDBase
from sqsworkers.crew import Crew, BaseListener, BulkListener
from sqsworkers.interfaces import CrewInterface


@pytest.fixture
def polling_interval():
    return 0.1


@pytest.fixture
def timeout():
    return 0.1


@pytest.fixture
def message():
    ns = SimpleNamespace(
        message_id="message_id",
        receipt_handle="receipt_handle",
        body=json.dumps(
            {"eventId": "event_id", "type": "type", "schema": "schema"}
        ),
    )

    ns.delete = lambda: None

    return ns


@pytest.fixture
def messages(message, length=10):
    return [message] * length


@pytest.fixture(params=["message", "messages", Exception("derp")])
def future(request, message, messages):

    with mock.patch("concurrent.futures.Future", autospec=True) as Future:
        future = Future()
        exception = (
            request.param if isinstance(request.param, Exception) else None
        )
        future.exception.return_value = exception
        future.add_done_callback.side_effect = lambda callable: callable(
            future
        )
        if request.param == "messages":
            future.result.return_value = SimpleNamespace(
                succeeded=[message] * 2, failed=[message] * 3
            )
        yield future


@pytest.fixture
def executor(future):
    with mock.patch(
        "concurrent.futures.ThreadPoolExecutor", autospec=True
    ) as ThreadPoolExecutor:
        executor = ThreadPoolExecutor()
        executor.submit.return_value = future
        yield executor


@pytest.fixture
def statsd():
    return mock.Mock(spec=StatsDBase)


@pytest.fixture
def sqs_session():
    return mock.MagicMock()


@pytest.fixture
def sqs_resource(message):
    _mock_ = mock.MagicMock()
    _mock_.receive_messages = mock.Mock(return_value=[message])
    return _mock_


@pytest.fixture
def message_processor():
    class MessageProcessor(CrewInterface):
        def __init__(self, argument):
            """"""

        def start(self):
            """"""

    _mock_ = mock.MagicMock(spec=MessageProcessor)
    _mock_.__mro__ = [MessageProcessor]
    return _mock_


@pytest.fixture
def base_listener(
    sqs_session,
    message_processor,
    sqs_resource,
    statsd,
    executor,
    polling_interval,
):

    return BaseListener(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        executor=executor,
        polling_interval=polling_interval,
    )


@pytest.fixture(params=[10, None])
def bulk_listener(
    request,
    sqs_session,
    message_processor,
    sqs_resource,
    statsd,
    executor,
    polling_interval,
):
    minimum_messages = request.param

    return BulkListener(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        executor=executor,
        minimum_messages=minimum_messages,
        polling_interval=polling_interval,
    )


@pytest.fixture(params=["bulk_mode", ""])
def listener(request, base_listener, bulk_listener):
    bulk_mode = request.param == "bulk_mode"
    return base_listener if not bulk_mode else bulk_listener


@pytest.fixture
def listeners(listener, length=2):
    return [listener] * length


@pytest.fixture
def crew(listeners):
    return Crew(listeners=listeners)


def test_crew_starts_and_executes_successfully(
    crew, future, statsd, sqs_resource, timeout, listener
):

    crew.start()

    crew.stop(timeout=timeout)

    statsd.increment.assert_called()


def test_exception_in_listener_threadpool(
    listener, message, messages, future, caplog
):

    future.exception.return_value = Exception("derp")

    bulk_mode = isinstance(listener, BulkListener)

    listener._task_complete(future, (messages if bulk_mode else message))

    assert any(
        "exception: Exception('derp',)" in r.msg for r in caplog.records
    )

    message_count = 1 if not bulk_mode else len(messages)

    listener.statsd.increment.assert_called_with(
        "process.record.failure", message_count, tags=[]
    )


def test_crew_stops_successfully(crew):
    crew.start()
    crew.stop()


def test_bulk_listener_timeout_warning(
    sqs_session, sqs_resource, message_processor, caplog
):
    Crew(
        sqs_session=sqs_session,
        sqs_resource=sqs_resource,
        message_processor=message_processor,
        bulk_mode=True,
        wait_time=2,
        timeout=1,
        minimum_messages=1,
    )

    assert any(
        "wait time (2) is longer than the timeout (1)" in record.msg
        for record in caplog.records
    )


@pytest.mark.parametrize(
    "arg,",
    [
        "workers",
        "supervisor",
        "exception_handler_function",
        "MessageProcessor",
    ],
)
def test_deprecation_warnings(
    arg, sqs_session, sqs_resource, message_processor
):
    with pytest.warns(DeprecationWarning):
        kwargs = (
            {"message_processor": message_processor, arg: 1}
            if arg != "MessageProcessor"
            else {"MessageProcessor": message_processor}
        )
        Crew(sqs_session=sqs_session, sqs_resource=sqs_resource, **kwargs)


def test_exceptions_captured_by_sentry(
    sqs_session, sqs_resource, message_processor
):
    sentry = mock.MagicMock()

    class ExceptionalListener(BaseListener):
        def start(self):
            raise Exception("derp")

    listener = ExceptionalListener(
        sqs_session=sqs_session,
        sqs_resource=sqs_resource,
        message_processor=message_processor,
        sentry=sentry,
    )

    with pytest.raises(Exception, match="derp"):
        listener.start()

    sentry.captureException.assert_called()
