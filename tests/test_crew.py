from datadog import statsd
import logging
import os
import pytest
import sys

 # from helpers import aws_adapter_for_testing
from helpers import BulkMsgProcessor
from helpers import mock_sqs_session
from helpers import MsgProcessor

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

    with pytest.raises(ValueError):
        crew.Crew(**no_sqs)

@mock_sqs_session(n_msgs=10)
def test_bulk_start_10_msgs(sqs_session=None, sqs_queue_name=None, mock_=None, *args, **kwargs):
    import time
    logger = logging.getLogger('default')
    required_only = {
        'sqs_session': sqs_session,
        'queue_name': sqs_queue_name,
        'MessageProcessor': BulkMsgProcessor,
        'logger': logger,
        'statsd': statsd,
        'worker_limit': 1,
        'wait_time': 20,
        'max_number_of_messages': 10,
        'bulk_mode': True
    }

    c = crew.Crew(**required_only)
    # When you really want to be sure about what code you are hitting
    # assert(sqs_session.using_real_aws == False)
    c.start()
    time.sleep(15)
    try:
        assert(sqs_session.delete_count == sqs_session.receive_count)
        assert(sqs_session.delete_count == 10)
    except:
        logging.exception('Exception from test_bulk_start_10_msgs')
        c.stop()
        raise
    else:
        c.stop()
    time.sleep(10)

@mock_sqs_session(n_msgs=15)
def test_bulk_start_15_msgs(sqs_session=None, sqs_queue_name=None, mock_=None, *args, **kwargs):
    import time
    logger = logging.getLogger('default')
    required_only = {
        'sqs_session': sqs_session,
        'queue_name': sqs_queue_name,
        'MessageProcessor': BulkMsgProcessor,
        'logger': logger,
        'statsd': statsd,
        'worker_limit': 1,
        'wait_time': 20,
        'max_number_of_messages': 10,
        'bulk_mode': True
    }

    c = crew.Crew(**required_only)
    # When you really want to be sure about what code you are hitting
    # assert(sqs_session.using_real_aws == False)
    c.start()
    time.sleep(25)
    try:
        assert(sqs_session.delete_count == sqs_session.receive_count)
        assert(sqs_session.delete_count == 15)
    except:
        logging.exception('Exception from test_bulk_start_15_msgs')
        c.stop()
        raise
    else:
        c.stop()
    time.sleep(10)

@mock_sqs_session(n_msgs=7, n_failed_processing=3)
def test_bulk_start_proc_fails(sqs_session=None, sqs_queue_name=None, mock_=None, *args, **kwargs):
    import time
    logger = logging.getLogger('default')
    required_only = {
        'sqs_session': sqs_session,
        'queue_name': sqs_queue_name,
        'MessageProcessor': BulkMsgProcessor,
        'logger': logger,
        'statsd': statsd,
        'worker_limit': 1,
        'wait_time': 20,
        'max_number_of_messages': 4,
        'bulk_mode': True
    }

    c = crew.Crew(**required_only)
    # When you really want to be sure about what code you are hitting
    # assert(sqs_session.using_real_aws == False)
    c.start()
    time.sleep(15)
    try:
        assert(sqs_session.receive_count >= 7)
        time.sleep(5)
        assert(sqs_session.delete_count == 4)
    except:
        logging.exception('Exception from test_bulk_start_proc_fails')
        c.stop()
        raise
    else:
        c.stop()


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
