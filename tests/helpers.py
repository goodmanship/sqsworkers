import boto3
import decorator
import logging
import os
import sys
from types import SimpleNamespace
from functools import wraps

class MockAWSAccount():
    def __init__(self, **kwargs):
        self.logger = logging.LoggerAdapter(logging.getLogger('default'),
                                            extra={'helpers_py': 'AWS API Adaptor'})
        self.using_real_aws = False
        self.delete_count = 0
        self.receive_count = 0
        self.region_name = 'dummy_region'
        self.n_msgs = None
        if 'n_msgs' in kwargs:
            self.n_msgs = kwargs['n_msgs']
        else:
            self.n_msgs = 10

        if 'n_msgs' in kwargs:
            BulkMsgProcessor.n_proc_results = kwargs['n_msgs']

        if 'n_failed_processing' in kwargs:
            BulkMsgProcessor.n_failed_processing = kwargs['n_failed_processing']

        self.attributes = {
            'ApproximateNumberOfMessages': self.n_msgs
        }

        # determine if real aws is available
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
        aws_security_token = os.environ.get('AWS_SECURITY_TOKEN', None)
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN', None)
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', None)
        sqs_queue_name = os.environ.get('AWS_TEST_QUEUE', None)
        sqs_region = os.environ.get('SQS_REGION', None)

        if all([aws_secret_access_key, aws_security_token, aws_session_token,
                aws_access_key_id, sqs_queue_name, sqs_region]):
            self.using_real_aws = True
            self.sqs_session = boto3.session.Session(region_name=sqs_region)
            self.sqs_queue_res = self.sqs_session.resource('sqs')
            self.sqs_queue = self.sqs_queue_res.get_queue_by_name(QueueName=sqs_queue_name)
            self.test_message = '''{
                "eventId": "blah123",
                "payload": {
                    "env": "dev",
                    "messageId": "blah456",
                },
                "time": "2017-12-06T00:00:00.595Z",
            }'''

    def insert_messages(self, n_msgs):
        self.logger.info('preparing Q with {} messages\n\n'.format(n_msgs))
        for _ in range(n_msgs):
            self.sqs_queue.send_message(MessageBody=self.test_message)

    def reset(self):
        self.delete_count = 0
        self.receive_count = 0
        self.region_name = 'dummy_region'
        self.attributes = {
            'ApproximateNumberOfMessages': 10
        }
        #global aws_adapter_for_testing
        #aws_adapter_for_testing = MockAWSAccount()
        if self.using_real_aws:
            while True:
                messages = self.sqs_queue.receive_messages(
                    AttributeNames=['All'],
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=2)
                if len(messages) <= 0:
                    break
                entries = []
                n = 0
                self.logger.info('Received {} messages'.format(len(messages)))
                for message in messages:
                    entries.append({
                        'Id': message.message_id,
                        'ReceiptHandle': message.receipt_handle
                        })
                    n += 1
                    if n > 9:
                        self.logger.info('batch removing {} messages'.format(n))
                        self.sqs_queue.delete_messages(Entries=entries)
                        entries.clear()
                        n = 0
                if len(entries) > 0:
                    self.logger.info('batch removing remaining {} messages'.format(n))
                    self.sqs_queue.delete_messages(Entries=entries)
                    entries.clear()

    def delete_messages(self, **kwargs):
        import traceback
        self.logger.error('in fake delete_messages\n')
        if self.using_real_aws:
            try:
                self.logger.debug('\n\ndlt -sendval: {}\n\n'.format(kwargs))
                retval = self.sqs_queue.delete_messages(**kwargs)
                self.delete_count += len(retval['Successful'])
            except:
                traceback.print_exc()
                raise
            self.logger.debug('\n\ndlt -retval: {}\n\n'.format(retval))
        else:
            received_for_delete = len(kwargs['Entries'])
            retval = {'Successful' : ['msg_id_%02d' % i
                                      for i in range(received_for_delete)],
                      'Failed' : []}
            self.delete_count += received_for_delete
            self.n_msgs = max(self.n_msgs - received_for_delete, 0)
        return retval

    def receive_messages(self, **kwargs):

        self.logger.error('helpers:recv_msgs n_msgs = %d\n' % self.n_msgs)
        if self.using_real_aws:
            self.logger.error('helpers:recv_msgs using_real_aws')
            retval = self.sqs_queue.receive_messages(**kwargs)
            n_procs = len(retval)
            BulkMsgProcessor.send_count = n_procs
            self.logger.debug('received %d messages from aws queue\n' % len(retval))
            self.receive_count += n_procs
        else:
            self.logger.error('helpers:recv_msgs NOT using_real_aws')
            batch_size = min(self.n_msgs, 10)
            retval = [SimpleNamespace(message_id='msg_id_%02d' % i,
                                      receipt_handle='rcpt_handle_%03d' % i)
                      for i in range(batch_size)]
            self.receive_count += batch_size
            BulkMsgProcessor.send_count = batch_size
        return retval

    def resource(self, some_arg, *args, **kwargs):
        return self

    def get_queue_by_name(self, *args, **kwargs):
        return self

    def __len__(self, *args, **kwargs):
        return 10 if self.n_msgs is None else self.n_msgs

class MsgProcessor():
    def __init__(self, *args, **kwargs):
        logging.getLogger('default').info('msg processor instantiated')

    def start(self):
        logging.getLogger('default').info('processed')

class BulkMsgProcessor:
    n_proc_results = 10
    n_failed_processing = 0
    send_count = 0
    to_send = None
    def __init__(self, *args, **kwargs):
        self.logger = logging.LoggerAdapter(logging.getLogger('default'),
                                            extra={'helper_py': 'fake MulkMsgProcessor'})
        self.logger.info('bulk msg processor instantiated')

    def start(self):
        retval = []
        if not BulkMsgProcessor.n_proc_results:
            BulkMsgProcessor.n_proc_results = 10
        if not BulkMsgProcessor.n_failed_processing:
            BulkMsgProcessor.n_failed_processing = 0
        if BulkMsgProcessor.to_send is None:
            BulkMsgProcessor.to_send = [True] * (BulkMsgProcessor.n_proc_results - BulkMsgProcessor.n_failed_processing) + \
                                       [False] * BulkMsgProcessor.n_failed_processing
            self.logger.error('bmp will send {}'.format(BulkMsgProcessor.to_send))
        sending = BulkMsgProcessor.to_send[:BulkMsgProcessor.send_count]
        BulkMsgProcessor.to_send = BulkMsgProcessor.to_send[BulkMsgProcessor.send_count:]
        return sending

aws_adapter_for_testing = MockAWSAccount()

def mock_sqs_session(**kwargs):
    if 'n_msgs' in kwargs:
        n_msgs = kwargs['n_msgs']
    else:
        n_msgs = 10

    if 'n_failed_processing' in kwargs:
        n_failed_processing = kwargs['n_failed_processing']
    else:
        n_failed_processing = 0


    def f(client_func):
        @wraps(client_func)
        def client_wrapper(func, *args, **kwargs):
            sqs_queue_name = 'dummy'
            aws_adapter_for_testing.reset()
            if aws_adapter_for_testing.using_real_aws:
                aws_adapter_for_testing.insert_messages(n_msgs)
            aws_adapter_for_testing.n_msgs = n_msgs
            BulkMsgProcessor.n_proc_results = n_msgs
            BulkMsgProcessor.n_failed_processing = n_failed_processing
            return func(aws_adapter_for_testing, sqs_queue_name, *args, **kwargs)
        return decorator.decorator(client_wrapper, client_func)
    return f
