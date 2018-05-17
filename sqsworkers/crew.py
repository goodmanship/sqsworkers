import logging
import os
import re
from threading import Thread, currentThread
import time
import traceback


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
        self.poll_queue()

    def poll_queue(self):
        while self.employed:
            messages = self.queue.receive_messages(
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=self.max_number_of_messages,
                WaitTimeSeconds=self.wait_time)

            if len(messages) > 0:
                for message in messages:
                    self.logger.info('processing %s messages %s' % (len(messages), messages))
                    processor = self.crew.MessageProcessor(message)
                    self.crew.statsd.increment('process.record.start', 1, tags=[])
                    processed = processor.start()
                    if processed:
                        deleted = self.queue.delete_messages(
                            Entries=[{
                                'Id': message.message_id,
                                'ReceiptHandle': message.receipt_handle
                            }]
                        )
                        self.crew.statsd.increment('process.record.success', 1, tags=[])
                        self.logger.info('%s messages processed successfully and deleted %s' % (len(messages), deleted))
                    else:
                        self.crew.statsd.increment('process.record.failure', 1, tags=[])


class Supervisor(CrewMember):
    def __init__(self, crew):
        self._real_run = self.run
        self.run = self._wrap_run
        self.supervisor_name = 'supervisor-%s-%s' % (os.getpid(), str(time.time()))
        self.crew = crew
        CrewMember.__init__(self)

    def _wrap_run(self):
        try:
            self._real_run()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.crew.sentry.captureException() if self.crew.sentry else None
            log_uncaught_exception(e, logger=self.crew.logger, context={'supervisor_name': self.supervisor_name, 'crew.name': self.crew.name})
            self._wrap_run()

    def run(self):
        while self.employed:
            self.supervise()
            time.sleep(30)

    def supervise(self):
        good_workers = []
        self.crew.logger.info('supervising %s worker on workers: %s' % (self.supervisor_name, self.crew.workers))
        for worker in self.crew.workers:
            if worker.is_alive():
                self.crew.logger.info('worker %s is alive' % (worker.worker_name))
                good_workers.append(worker)
            else:
                self.crew.logger.warn('worker %s is dead, hiring a new one...' % (worker.worker_name))
                self.crew.statsd.increment('workers.dead', 1, tags=[self.crew.name])
                new_worker = Worker(self.crew)
                self.crew.logger.info('hired a new worker %s, staring it up...' % (new_worker.worker_name))
                new_worker.start()
                self.crew.logger.info('started a new worker %s' % (new_worker.worker_name))
                good_workers.append(new_worker)
        self.crew.logger.info('total number of workers after supervision: %s' % (str(len(good_workers))))
        self.crew.statsd.gauge('workers.employed', len(good_workers), tags=[self.crew.name])
        self.crew.workers = good_workers
