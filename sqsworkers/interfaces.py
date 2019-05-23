from abc import ABCMeta, abstractmethod
import logging


class CrewInterface:
    """
    This defines the interface for something that will read from an sqs queue
    in its start method, delegating the work to a MessageProcessor
    """

    @abstractmethod
    def __init__(self, MessageProcessor, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def start(self):
        raise NotImplementedError


class StatsDInterface(metaclass=ABCMeta):
    """
    Defines the interface for the statsd client.
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def increment(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def gauge(self, *args, **kwargs):
        raise NotImplementedError

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
