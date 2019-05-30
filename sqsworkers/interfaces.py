import logging
from abc import ABCMeta, abstractmethod


class CrewInterface(metaclass=ABCMeta):
    """
    Any class with a start method will be a subclass of this interface.
    """

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, C):
        """
        This method guarantees that any class that implements this class' abstract methods
        will be considered a subclass.
        """
        if cls is CrewInterface:
            for method in cls.__abstractmethods__:
                if not any(method in c.__dict__ for c in C.__mro__):
                    logging.error(
                        f"{C.__name__} fails to implement {method} method"
                    )
                    return False
            return True
        return NotImplemented


class StatsDInterface(metaclass=ABCMeta):
    """
    Defines the interface for the statsd client.
    """

    @abstractmethod
    def increment(self, *args, **kwargs):
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
