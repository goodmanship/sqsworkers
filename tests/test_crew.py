from datadog import statsd
import logging
import os
import pytest
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sqsworkers')))

import crew

class MsgProcessor():
    def __init__(self):
        logging.getLogger('default').info('msg processor instantiated')

    def start(self):
        logging.getLogger('default').info('processed')

class BulkMsgProcessor:
    def __init__(self):
        logging.getLogger('default').info('bulk msg processor instantiated')

    def start(self):
        logging.getLogger('default').info('processed')
        return [True, False]


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
