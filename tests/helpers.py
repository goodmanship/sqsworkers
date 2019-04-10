import os
from functools import wraps
import decorator
import logging

import boto3

class ObjectGen:
    def __init__(self, v_dict={}):
        for k,v in v_dict.items():
            setattr(self,k,v)

class MockAWSAccount():
    def __init__(self, **kwargs):
        self.using_real_aws = False
        self.delete_count = 0
        self.receive_count = 0
        self.region_name = 'dummy_region'
        self.attributes = {
                'ApproximateNumberOfMessages': 10
                }
        self.n_msg = None
        # determine if real aws is available
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] if 'AWS_SECRET_ACCESS_KEY' in os.environ else None
        aws_security_token = os.environ['AWS_SECURITY_TOKEN'] if 'AWS_SECURITY_TOKEN' in os.environ else None
        aws_session_token = os.environ['AWS_SESSION_TOKEN'] if 'AWS_SESSION_TOKEN' in os.environ else None
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] if 'AWS_ACCESS_KEY_ID' in os.environ else None
        sqs_queue_name = os.environ['AWS_TEST_QUEUE'] if 'AWS_TEST_QUEUE' in os.environ else None
        sqs_region = os.environ['SQS_REGION'] if 'SQS_REGION' in os.environ else None

        if all([aws_secret_access_key, aws_security_token, aws_session_token,
                aws_access_key_id, sqs_queue_name, sqs_region]):
            self.using_real_aws = True
            self.sqs_session = boto3.session.Session(region_name=sqs_region)
            self.sqs_queue_res = self.sqs_session.resource('sqs')
            self.sqs_queue = self.sqs_queue_res.get_queue_by_name(QueueName=sqs_queue_name)
            self.test_message='''{
                "eventId": "blah123",
                "payload": {
                    "env": "dev",
                    "messageId": "blah456",
                },
                "time": "2017-12-06T00:00:00.595Z",
            }'''
            if 'n_msgs' in kwargs:
                n_msgs = kwargs['n_msgs']
            else:
                n_msgs = 10
            for _ in range(n_msgs):
                self.sqs_queue.send_message(MessageBody=self.test_message)

    def reset(self):
        self.delete_count = 0
        self.receive_count = 0
        self.region_name = 'dummy_region'
        self.attributes = {
                'ApproximateNumberOfMessages': 10
                }
        self.n_msg = None

    def delete_messages(self, *args):
        self.delete_count += 1
        if self.using_real_aws:
            entries = args
            retval = self.sqs_queue.delete_messages(entries)
        else:
            if self.n_msg is None:
                self.n_msg = 10
            retval = {'Successful' : ['msg_id_%02d' % i
                                      for i in range(self.n_msg)],
                      'Failed' : []}
        return retval

    def receive_messages(self, **kwargs):
        self.receive_count += 1
        if self.n_msg is None:
            self.n_msg = 10
        if self.using_real_aws:
            retval = self.sqs_queue.receive_messages(kwargs)
        else:
            retval = [ObjectGen({'message_id': 'msg_id_%02d' % i,
                                 'receipt_handle': 'rcpt_handle_%03d' % i})
                                 for i in range(self.n_msg)]
        return retval

    def resource(self, some_arg, *args, **kwargs):
        return self

    def get_queue_by_name(self, *args, **kwargs):
        return self

    def __len__(self, *args, **kwargs):
        return 10 if self.n_msg is None else self.n_msg

class MsgProcessor():
    def __init__(self, *args, **kwargs):
        logging.getLogger('default').info('msg processor instantiated')

    def start(self):
        logging.getLogger('default').info('processed')

class BulkMsgProcessor:
    def __init__(self, *args, **kwargs):
        logging.getLogger('default').info('bulk msg processor instantiated')

    def start(self):
        logging.getLogger('default').info('processed')
        return [True] * 10

aws_adapter_for_testing=MockAWSAccount()

def mock_sqs_session(**kwargs):
    if 'n_msgs' in kwargs:
        n_msgs = kwargs['n_msgs']
    else:
        n_msgs = 10

    def f(client_func):
        @wraps(client_func)
        def client_wrapper(func, *args, **kwargs):
            sqs_queue_name = 'dummy'
            aws_adapter_for_testing.reset()
            aws_adapter_for_testing.n_msg=n_msgs
            return func(aws_adapter_for_testing, sqs_queue_name, *args, **kwargs)
        return decorator.decorator(client_wrapper, client_func)
    return f
