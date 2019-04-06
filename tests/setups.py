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
        print('Mock Queue inited')
        self.delete_count = 0
        self.receive_count = 0
        self.region_name = 'dummy_region'
        self.attributes = {
                'ApproximateNumberOfMessages': 10
                }
        self.n_msg = None

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
        if self.n_msg is None:
            self.n_msg = 10
        retval = {'Successful' : ['msg_id_%02d' % i
                                  for i in range(self.n_msg)]}
        return retval

    def receive_messages(self, *args, **kwargs):
        self.receive_count += 1
        if self.n_msg is None:
            self.n_msg = 10
        #retval = [ObjectGen({'message_id': 'msg_id_%02d' % i})
                    #for i in range(self.n_msg)]
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

false_aws=MockAWSAccount()

def mock_sqs_session(**kwargs):
    if 'n_msgs' in kwargs:
        n_msgs = kwargs['n_msgs']
    else:
        n_msgs = 10

    def f(client_func):
        @wraps(client_func)
        def client_wrapper(func, *args, **kwargs):
            sqs_queue_name='dummy'
            false_aws.reset()
            false_aws.n_msg=n_msgs
            return func(false_aws, sqs_queue_name, *args, **kwargs)
        return decorator.decorator(client_wrapper, client_func)
    return f
