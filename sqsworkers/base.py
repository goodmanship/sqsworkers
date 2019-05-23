import logging

from sqsworkers import interfaces


class StatsDBase(interfaces.StatsDInterface):
    """Implements the base statsd interface."""

    def __init__(self, logger=None):
        self.logger = logging.getLogger() if logger is None else logger

    def increment(self, *args, **kwargs):
        self.logger.info(f"statsd increment invoked: {args}, {kwargs}")

    def gauge(self, *args, **kwargs):
        self.logger.info(f"statsd gauge invoked: {args}, {kwargs}")
