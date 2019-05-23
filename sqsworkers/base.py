from sqsworkers import interfaces


class StatsDBase(interfaces.StatsDInterface):
    """Implements the base statsd interface."""

    def __init__(self, logger=None):
        super().__init__(logger=logger)

    def increment(self, *args, **kwargs):
        return super().increment(*args, **kwargs)

    def gauge(self, *args, **kwargs):
        return super().gauge(*args, **kwargs)
