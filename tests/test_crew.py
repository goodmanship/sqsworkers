from datadog import statsd
import logging
import os
import pytest
import mock
import sys

from setups import mock_sqs_session
from setups import MockAWSAccount
from setups import MsgProcessor
from setups import BulkMsgProcessor

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sqsworkers')))
import crew

def test_crew_with_all_args():
    optionals = {
        'sqs_session': 'fake session',
        'queue_name': 'something',
        'MessageProcessor': MsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd,
        'sentry': None,
        'worker_limit': 9
    }

    assert crew.Crew(**optionals).worker_limit == 9

def test_crew_with_no_optionals():
    required_only = {
        'sqs_session': 'fake session',
        'queue_name': 'something',
        'MessageProcessor': MsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd
    }

    assert crew.Crew(**required_only).worker_limit == 10

def test_crew_with_bulk_msg_processor():
    required_only = {
        'sqs_session': 'fake session',
        'queue_name': 'something',
        'MessageProcessor': BulkMsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd,
        'bulk_mode': True
    }

    assert crew.Crew(**required_only).MessageProcessor == BulkMsgProcessor

def test_crew_with_resource():
    with_resource = {
        'sqs_resource': 'resource',
        'MessageProcessor': MsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd
    }

    assert crew.Crew(**with_resource).sqs_resource == 'resource'

def test_crew_without_sqs():
    no_sqs = {
        'MessageProcessor': MsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd
    }

    with pytest.raises(TypeError):
        crew.Crew(**no_sqs)


@mock_sqs_session(n_msgs=10)
def test_bulk_start(sqs_session=None, sqs_queue_name=None, mock_=None, *args, **kwargs):
    import time
    import setups
    logger = logging.getLogger('default')
    required_only = {
        'sqs_session': sqs_session,
        'queue_name': sqs_queue_name,
        'MessageProcessor': BulkMsgProcessor,
        'logger': logger,
        'statsd': statsd,
        'worker_limit': 1,
        'max_number_of_messages': 10,
        'bulk_mode': True
    }

    c = crew.Crew(**required_only)
    c.start()
    assert(setups.false_aws.receive_count == 1)
    time.sleep(1)
    c.stop()
    assert(setups.false_aws.delete_count == 1)


# TODO: this test needs an sqs queue to work
# def test_start():
#     required_only = {
#         'sqs_session': None,
#         'queue_name': 'something',
#         'MessageProcessor': MsgProcessor,
#         'logger': logging.getLogger('default'),
#         'statsd': statsd
#     }

#     c = crew.Crew(**required_only)
#     c.start()
