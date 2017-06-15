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


def test_crew_with_all_args():
    optionals = {
        'sqs_session': None,
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
        'sqs_session': None,
        'queue_name': 'something',
        'MessageProcessor': MsgProcessor,
        'logger': logging.getLogger('default'),
        'statsd': statsd
    }

    assert crew.Crew(**required_only).worker_limit == 10

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
