from unittest import mock

import pytest

from sqsworkers.base import StatsDBase
from sqsworkers.crew import Crew, BaseListener, BulkListener
from sqsworkers.interfaces import CrewInterface


@pytest.fixture(autouse=True)
def patch_futures():
    """Monkey patch concurrent.futures.ThreadPoolExecutor to do basically nothing."""
    with mock.patch(
        "concurrent.futures.ThreadPoolExecutor", autospec=True
    ) as obj:
        yield obj


@pytest.fixture
def sentry():
    return mock.MagicMock()


@pytest.fixture
def statsd():
    return mock.Mock(spec=StatsDBase)


@pytest.fixture
def message():
    msg = mock.Mock()
    msg.message_id = "message_id"
    msg.receipt_handle = "receipt_handle"
    msg.body = {"eventId": "event_id", "type": "type", "schema": "schema"}
    return msg


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
    sqs_session, message_processor, sqs_resource, statsd, sentry
):
    return Crew(
        sqs_session=sqs_session,
        MessageProcessor=message_processor,
        sqs_resource=sqs_resource,
        statsd=statsd,
        sentry=sentry,
    )


@pytest.fixture
def bulk_crew(sqs_session, message_processor, sqs_resource, statsd, sentry):
    return Crew(
        sqs_session=sqs_session,
        sqs_resource=sqs_resource,
        MessageProcessor=message_processor,
        statsd=statsd,
        sentry=sentry,
        bulk_mode=True,
    )


@pytest.fixture(params=["bulk_crew", "non_bulk_crew"])
def crew(request, non_bulk_crew, bulk_crew):
    """Returns a bulk or non-bulk crew based on the request param."""
    return locals().get(request.param)


def test_crew_instantiation(non_bulk_crew, bulk_crew):

    assert isinstance(non_bulk_crew.listener, BaseListener)
    assert not isinstance(non_bulk_crew, BulkListener)

    assert isinstance(bulk_crew.listener, BulkListener)

    non_bulk_crew.start()

    non_bulk_crew.listener._executor.submit.assert_called()

    non_bulk_crew.listener.statsd.increment.assert_called()


def test_crew_start(crew):
    crew.start()
    crew.listener._executor.submit.assert_called()
    crew.listener.statsd.increment.assert_called()
