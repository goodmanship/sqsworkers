from unittest import mock
import json

import pytest

from sqsworkers.base import StatsDBase
from sqsworkers.crew import Crew, BaseListener, BulkListener
from sqsworkers.interfaces import CrewInterface


@pytest.fixture(autouse=True)
def monkeypatch_thread():
    """Monkeypatch threading.Thread ."""
    with mock.patch("threading.Thread", autospec=True) as Thread:
        target = None

        def set_target(t):
            nonlocal target
            target = t

        Thread.side_effect = lambda *args, target, **kwargs: set_target(target)


@pytest.fixture
def message():
    msg = mock.Mock()
    msg.message_id = "message_id"
    msg.receipt_handle = "receipt_handle"
    msg.body = json.dumps(
        {"eventId": "event_id", "type": "type", "schema": "schema"}
    )
    return msg


@pytest.fixture
def executor_future(message):
    """Monkey patch concurrent.futures.ThreadPoolExecutor to do basically nothing."""

    with mock.patch(
        "concurrent.futures.ThreadPoolExecutor", autospec=True
    ) as ThreadPoolExecutor, mock.patch(
        "concurrent.futures.Future", autospec=True
    ) as Future:
        executor = ThreadPoolExecutor()
        future = Future()
        future.exception.return_value = None
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
def non_bulk_crew(
    sqs_session,
    message_processor,
    sqs_resource,
    statsd,
    sentry,
    executor_future,
):
    ex, f = executor_future
    return Crew(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        sentry=sentry,
        executor=ex,
        daemon=False,
    )


@pytest.fixture
def bulk_crew(
    sqs_session,
    message_processor,
    sqs_resource,
    statsd,
    sentry,
    executor_future,
):
    ex, f = executor_future
    return Crew(
        sqs_session=sqs_session,
        sqs_resource=sqs_resource,
        MessageProcessor=message_processor,
        statsd=statsd,
        sentry=sentry,
        bulk_mode=True,
        executor=ex,
        daemon=False,
    )


@pytest.fixture(params=["bulk_crew", "non_bulk_crew"])
def crew(request, non_bulk_crew, bulk_crew):
    """Returns a bulk or non-bulk crew based on the request param."""
    return locals().get(request.param)


@pytest.fixture
def listener(crew):
    return crew.listener


def test_crew_instantiation(non_bulk_crew, bulk_crew):

    assert isinstance(non_bulk_crew.listener, BaseListener)
    assert not isinstance(non_bulk_crew, BulkListener)

    assert isinstance(bulk_crew.listener, BulkListener)

    non_bulk_crew.start()

    non_bulk_crew.listener._executor.submit.assert_called()

    non_bulk_crew.listener.statsd.increment.assert_called()


@pytest.mark.parametrize("crew_,", ["bulk_crew", "non_bulk_crew"])
def test_crew_start(crew_, bulk_crew, non_bulk_crew):
    crew = locals().get(crew_)
    crew.start()

    crew.stop(timeout=1)

    crew.listener._executor.submit.assert_called()
    crew.listener.statsd.increment.assert_called()


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

    assert any("longer" in record.msg for record in caplog.records)


# def test_exception(crew, executor_future, caplog):
#     _, future = executor_future
#
#     future.exception.return_value = Exception("derp")
#
#     crew.start()
#
#     crew.stop(timeout=1)
#
#     assert any("derp raised" in r.msg for r in caplog.records)
