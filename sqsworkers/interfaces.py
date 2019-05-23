from abc import ABCMeta, abstractmethod
import logging


class StatsDInterface(metaclass=ABCMeta):
    """
    Defines the interface for the statsd.

    Implements default methods.
    """

    @abstractmethod
    def __init__(self, logger=None):
        self.logger = logging.getLogger() if logger is None else logger

    @abstractmethod
    def increment(self, *args, **kwargs):
        self.logger.info(f"statsd increment invoked: {args}, {kwargs}")

    @abstractmethod
    def gauge(self, *args, **kwargs):
        self.logger.info(f"statsd gauge invoked: {args}, {kwargs}")

    @classmethod
    def __subclasshook__(cls, C):
        """
        This method guarantees that any class that implements this class' abstract methods
        will be considered a subclass.
        """
        if cls is StatsDInterface:
            for method in cls.__abstractmethods__:
                if not any(method in c.__dict__ for c in C.__mro__):
                    logging.error(
                        f"{C.__name__} fails to implement {method} method"
                    )
                    return False
            return True
        return NotImplemented


class CrewInterface:
    """
    This defines the interface for something that will read from an sqs queue
    and fan out messages received to a threadpool that will attempt to do something
    with those messages.
    """

    @abstractmethod
    def __init__(self, MessageProcessor, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def start(self):
        raise NotImplementedError
