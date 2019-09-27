import logging
import os
import pytest
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sqsworkers')))
import crew
import unittest
import json
from copy import deepcopy
from typing import List


# Dummy Bulk Message Processor
class BulkMsgProcessor:
    def __init__(self):
        logging.getLogger('default').info('bulk msg processor instantiated')

    def start(self):
        """
        Mock Start method of bulk processor for successful records
        """
        logging.getLogger('default').info('processed')
        response:List[crew.MessageProcessorResult] = [
            crew.MessageProcessorResult(self.messages[0], self.success_statuses[0]),
            crew.MessageProcessorResult(self.messages[1], self.success_statuses[1])]
        return response

    def start_partial_failures(self):
        """
        Mock Start method of bulk processor where there are parital failures
        """
        logging.getLogger('default').info('processed')
        response:List[crew.MessageProcessorResult] = [
            crew.MessageProcessorResult(self.messages[0], self.partial_statuses[0]),
            crew.MessageProcessorResult(self.messages[1], self.partial_statuses[1])]
        return response

    def start_failures(self):
        """
        Mock Start method of bulk processor where all records fail
        """
        logging.getLogger('default').info('processed')
        response:List[crew.MessageProcessorResult] = [
            crew.MessageProcessorResult(self.messages[0], self.failed_statuses[0]),
            crew.MessageProcessorResult(self.messages[1], self.failed_statuses[1])]
        return response

class TestWorker:
    """
    Mocking Worker Class and bulk_poll_queue method
    """
    def bulk_poll_queue(results):
        # lists to store failed and successful entries per worker thread
        failed_entries = []
        successful_entries = []
        for msg_result in results:
            if msg_result.status.name != 'SUCCESS':
                failed_entries.append({json.dumps(msg_result.message.body)})
                # continue with the next message and do not delete
                continue
            else:
                successful_entries.append({json.dumps(msg_result.message.body)})
        # delete successful entries from the queue
        if len(successful_entries) > 0:
            return True
        return False

class TestBulkProcessing(unittest.TestCase):
    # Test Data
    messages: List[crew.Message] = [
        crew.Message(["avi:event:type:test"], {"msg_name": "msg1"}, {"data": 1, "name": "Bob"}),
        crew.Message(["avi:event:type:test"], {"msg_name": "msg2"}, {"data": 2, "name": "John"})
    ]
    success_statuses: List[crew.ResultStatus]= [crew.ResultStatus.SUCCESS, crew.ResultStatus.SUCCESS]
    partial_statuses: List[crew.ResultStatus]= [crew.ResultStatus.SUCCESS, crew.ResultStatus.ERROR]
    failed_statuses: List[crew.ResultStatus]= [crew.ResultStatus.ERROR, crew.ResultStatus.ERROR]
    
    # Test Cases
    def test_crew_with_bulk_msg_processor(self):
        """
        Assert that MessageProcessor param equals to BulkMsgProcessor class defined
        """
        required_only = {
            'sqs_session': 'fake session',
            'queue_name': 'something',
            'MessageProcessor': BulkMsgProcessor,
            'logger': logging.getLogger('default'),
            'bulk_mode': True
        }

        assert crew.Crew(**required_only).MessageProcessor == BulkMsgProcessor

    def test_bulk_poll_queue_invoked(self):
        """
        Assert that bulk_mode param is set to True
        """
        required_only = {
            'sqs_session': 'fake session',
            'queue_name': 'something',
            'sqs_resource': 'resource',
            'MessageProcessor': BulkMsgProcessor,
            'logger': logging.getLogger('default'),
            'bulk_mode': True
        }

        c = crew.Crew(**required_only)
        assert c.bulk_mode == True
        
    def test_poll_queue_invoked(self):
        """
        Assert that bulk_mode param is set to False
        """
        required_only = {
            'sqs_session': 'fake session',
            'queue_name': 'something',
            'sqs_resource': 'resource',
            'MessageProcessor': BulkMsgProcessor,
            'logger': logging.getLogger('default'),
            'bulk_mode': False
        }

        c = crew.Crew(**required_only)
        assert c.bulk_mode == False
    
    def test_bulk_poll_queue_success(self):
        """
        Assert output from BulkMsgprocessor and TestWorker for all successful records
        """
        results: List[crew.MessageProcessorResult] = BulkMsgProcessor.start(self)
        expected_msg1: crew.Message = deepcopy(self.messages[0])
        assert results[0] == crew.MessageProcessorResult(expected_msg1, crew.ResultStatus.SUCCESS)
        expected_msg2: crew.Message = deepcopy(self.messages[1])
        assert results[1] == crew.MessageProcessorResult(expected_msg2, crew.ResultStatus.SUCCESS)
        # Assert entries are deleted 
        delete_entries = TestWorker.bulk_poll_queue(results)
        assert delete_entries == True

    def test_bulk_poll_queue_partial_success(self):
        """
        Assert output from BulkMsgprocessor and TestWorker for partial failures
        """
        results: List[crew.MessageProcessorResult] = BulkMsgProcessor.start_partial_failures(self)
        expected_msg1: crew.Message = deepcopy(self.messages[0])
        assert results[0] == crew.MessageProcessorResult(expected_msg1, crew.ResultStatus.SUCCESS)
        expected_msg2: crew.Message = deepcopy(self.messages[1])
        assert results[1] == crew.MessageProcessorResult(expected_msg2, crew.ResultStatus.ERROR)
        delete_entries = TestWorker.bulk_poll_queue(results)
        # Assert entries are deleted 
        assert delete_entries == True

    def test_bulk_poll_queue_failures(self):
        """
        Assert output from BulkMsgprocessor and TestWorker for failed records
        """
        results: List[crew.MessageProcessorResult] = BulkMsgProcessor.start_failures(self)
        expected_msg1: crew.Message = deepcopy(self.messages[0])
        assert results[0] == crew.MessageProcessorResult(expected_msg1, crew.ResultStatus.ERROR)
        expected_msg2: crew.Message = deepcopy(self.messages[1])
        assert results[1] == crew.MessageProcessorResult(expected_msg2, crew.ResultStatus.ERROR)
        delete_entries = TestWorker.bulk_poll_queue(results)
        # Assert entries are not deleted since all records have failed
        assert delete_entries == False