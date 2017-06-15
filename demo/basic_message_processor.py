from datadog import statsd

import boto3
import logging
import os
import sys
import time

from sqsworkers.crew import Crew

statsd.namespace = 'dtech.sqs.workers'
statsd.constant_tags.append('sqs-workers-test')
statsd.host = 'statsd.domain.dev.atl-inf.io'

msg_logger = logging.getLogger('message')
msg_logger.setLevel(logging.DEBUG)
std_logger = logging.StreamHandler(sys.stdout)
std_logger.setLevel(logging.DEBUG)
msg_logger.addHandler(std_logger)

app_logger = logging.getLogger('default')

class MsgProcessor():
    def __init__(self, msg):
        self.msg = msg
        msg_logger.error('--------------------')
        msg_logger.error('msg processor instantiated')
        msg_logger.error('--------------------')

    def start(self):
        msg_logger.error('--------------------')
        msg_logger.error('message body:')
        msg_logger.error('"' + self.msg.body + '"')
        msg_logger.error('message processed')
        msg_logger.error('--------------------')
        self.msg.delete()


def test_crew_with_one_worker():
    sqs_session = boto3.session.Session(
        region_name='us-east-1',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN')
    )

    options = {
        'sqs_session': sqs_session,
        'queue_name': 'hydra-ddev-test-queue',
        'MessageProcessor': MsgProcessor,
        'logger': msg_logger,
        'statsd': statsd,
        'sentry': None,
        'worker_limit': 1
    }
    c = Crew(**options)
    c.start()
    time.sleep(3)
    c.stop()

test_crew_with_one_worker()