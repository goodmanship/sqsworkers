import logging
import os
import re
from threading import Thread, currentThread
import time
import traceback
import json
import sys


# logging util function
def log_uncaught_exception(e, logger=None, context=None):
    if logger is None:
        logger = logging.getLogger('default')
    if context is None:
        context = {}

    context['exception_type'] = type(e)
    context['exception_value'] = e
    context['exception_traceback'] = ''.join(traceback.format_tb(e.__traceback__))

    logger.error('Uncaught Exception: %s' % (e), extra={'extra': context})


# dummy class in case statsd obj is not provided
class DummyStatsd():
    def __init__(self, logger):
        self.logger = logger

    def increment(self, *args, **kwargs):
        self.logger.info("datadog metric incremented")

    def gauge(self, *args, **kwargs):
        self.logger.info("datadog gauge reported")


class Crew():
    # options = {
    #  @  'sqs_session': None,
    #  @  'queue_name': 'name',
    #  @  'sqs_resource': 'resource',
    #  *  'MessageProcessor': MsgProcessor,
    #  *  'logger': logging.getLogger('default'),
    #     'statsd': Dummy,
    #     'sentry': Default,
    #     'worker_limit': 10,
    #     'max_number_of_messages': 1,
    #     'wait_time': 5
    #
    # }
    # * = required
    # @ = required: (session + name) or (url)

    def __init__(self, **kwargs):
        self.workers = []
        self.supervisor = None
        self.sqs_session = kwargs['sqs_session'] if 'sqs_session' in kwargs else None
        self.queue_name = kwargs['queue_name'] if 'queue_name' in kwargs else None
        self.sqs_resource = kwargs['sqs_resource'] if 'sqs_resource' in kwargs else None
        self.MessageProcessor = kwargs['MessageProcessor']
        self.name = self.make_name(self.queue_name, self.sqs_resource)
        self.logger = logging.LoggerAdapter(kwargs['logger'], extra={'extra': {'crew.name': self.name}})
        self.statsd = kwargs['statsd'] if 'statsd' in kwargs else DummyStatsd(self.logger)
        self.sentry = kwargs['sentry'] if 'sentry' in kwargs else None
        self.worker_limit = kwargs['worker_limit'] if 'worker_limit' in kwargs else 10
        self.max_number_of_messages = kwargs['max_number_of_messages'] if 'max_number_of_messages' in kwargs else 1
        self.wait_time = kwargs['wait_time'] if 'wait_time' in kwargs else 20
        self.exception_handler_function = kwargs['exception_handler'] if 'exception_handler' in kwargs else \
                                          'default_exception_handler'

        if not ((self.sqs_session and self.queue_name) or self.sqs_resource):
            raise TypeError('Required arguments not provided.  Either provide (sqs_session + queue_name) or sqs_resource.')

    def make_name(self, name, url):
        try:
            if name:
                return 'crew-%s-%s-%s' % (os.getpid(), name, str(time.time()))
            else:
                return 'crew-%s-%s-%s' % (os.getpid(), re.split('http:\/\/|\.',url)[-2], str(time.time()))
        except:
            return 'crew-%s-%s-%s' % (os.getpid(), 'noname', str(time.time()))

    def start(self):
        try:
            for i in range(self.worker_limit):
                worker = Worker(self)
                self.workers.append(worker)
                worker.start()
            self.supervisor = Supervisor(self)
            self.supervisor.start()
        except Exception as e:
            self.sentry.captureException() if self.sentry else None
            raise

    def stop(self):
        self.supervisor.fire()
        for worker in self.workers:
            worker.fire()
        self.logger.info('Crew stopped')

    def check_workers(self):
        i = 0
        for worker in self.workers:
            if worker.is_alive():
                i += 1
        return i


class CrewMember(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.employed = True

    def fire(self):
        self.employed = False


class Worker(CrewMember):
    def __init__(self, crew):
        self.crew = crew
        self.sqs_session = self.crew.sqs_session
        self.sqs_resource = self.crew.sqs_resource
        self._real_run = self.run
        self.run = self._wrap_run
        self.worker_name = 'worker-%s-%s-%s' % (os.getpid(), currentThread().getName(), str(time.time()))
        self.queue_name = self.crew.queue_name
        self.max_number_of_messages = self.crew.max_number_of_messages
        self.wait_time = self.crew.wait_time
        self.crew.logger.info('new worker starting with name: %s' % (self.worker_name))
        self.logger = logging.LoggerAdapter(self.crew.logger, extra={'extra': {'worker_name': self.worker_name, 'crew.name': self.crew.name}})
        self.logger = self.crew.logger
        self.exception_handler_function = self.crew.exception_handler_function
        CrewMember.__init__(self)

    def _wrap_run(self):
        try:
            self._real_run()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.crew.sentry.captureException() if self.crew.sentry else None
            log_uncaught_exception(e, logger=self.logger, context={'worker_name': self.worker_name, 'crew.name': self.crew.name})

    def run(self):
        self.logger.info('thread %s starting now' % self.worker_name)
        if self.sqs_resource == None:
            sqs_connection = self.sqs_session.resource('sqs', region_name=self.sqs_session.region_name)
            self.queue = sqs_connection.get_queue_by_name(QueueName=self.queue_name)
        else:
            self.queue = self.crew.sqs_resource

        self.logger.info(
            'connected to sqs, approx. %s msgs on queue' %
            self.queue.attributes['ApproximateNumberOfMessages']
        )

        self.executor = (
            executor if executor is not None else BoundedThreadPoolExecutor()
        )

        self._listeners = (
            [
                (
                    BaseListener(*args, executor=executor, **kwargs)
                    if not bulk_mode
                    else BulkListener(*args, executor=executor, **kwargs)
                )
                for _ in range(worker_limit)
            ]
            if listeners is None
            else listeners
        )

        self._daemons = [
            Thread(name=listener.name, target=listener.start, daemon=True)
            for listener in self._listeners
        ]

    def start(self):
        """Start listener in background thread."""
        logging.info("starting background listener thread")
        for daemon in self._daemons:
            daemon.start()

    def join(self, timeout=0.1):
        logging.info("waiting on background thread to finish")
        for daemon in self._daemons:
            daemon.join(timeout=timeout)

    def stop(self, timeout=0.1):
        self.join(timeout=timeout)
