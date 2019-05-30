import json
from types import SimpleNamespace
from unittest import mock

import pytest

from sqsworkers.base import StatsDBase
from sqsworkers.crew import Crew, BaseListener, BulkListener
from sqsworkers.interfaces import CrewInterface


@pytest.fixture
def message():
    return SimpleNamespace(
        message_id="message_id",
        receipt_handle="receipt_handle",
        body=json.dumps(
            {"eventId": "event_id", "type": "type", "schema": "schema"}
        ),
    )


@pytest.fixture
def messages(message, length=10):
    return [message] * length


@pytest.fixture(params=[None, Exception("derp")])
def executor_future(request, message):
    with mock.patch(
        "concurrent.futures.ThreadPoolExecutor", autospec=True
    ) as ThreadPoolExecutor, mock.patch(
        "concurrent.futures.Future", autospec=True
    ) as Future:
        executor = ThreadPoolExecutor()
        future = Future()
        exception = request.param
        future.exception.return_value = exception
        executor.submit.return_value = future
        future.add_done_callback.side_effect = lambda callable: callable(
            future
        )
        yield executor, future


@pytest.fixture
def sentry():
    return mock.MagicMock()


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
    sentry,
    executor_future,
):
    ex, _ = executor_future

    return BaseListener(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        sentry=sentry,
        executor=ex,
        daemon=False,
    )


@pytest.fixture
def bulk_listener(
    sqs_session,
    message_processor,
    sqs_resource,
    statsd,
    sentry,
    executor_future,
):
    ex, _ = executor_future

    return BulkListener(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        sentry=sentry,
        executor=ex,
        daemon=False,
    )


@pytest.fixture(params=["bulk_mode", ""])
def crew(request, base_listener, bulk_listener):
    bulk_mode = request.param == "bulk_mode"
    listener = base_listener if not bulk_mode else bulk_listener
    return Crew(listener=listener)


@pytest.fixture
def listener(crew):
    return crew.listener


def test_crew_start(crew, executor_future):
    _, future = executor_future

    crew.start()

    crew.stop(timeout=1)

    crew.listener._executor.submit.assert_called()
    crew.listener.statsd.increment.assert_called()

    if future.exception() is not None:
        crew.listener.queue.delete_messages.assert_called()


def test_crew_stop(crew, timeout=1):
    crew.start()
    crew.stop(timeout=timeout)


def test_timeout_warning(sqs_session, sqs_resource, message_processor, caplog):
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


def test_exception(listener, message, messages, executor_future, caplog):
    _, future = executor_future

    future.exception.return_value = Exception("derp")

    bulk_mode = isinstance(listener, BulkListener)

    listener._task_complete(future, (messages if bulk_mode else message))

    assert any("Exception('derp',) raised" in r.msg for r in caplog.records)

    message_count = 1 if not bulk_mode else len(messages)

    listener.statsd.increment.assert_called_with(
        "process.record.failure", message_count, tags=[]
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
