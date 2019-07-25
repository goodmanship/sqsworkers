import logging
from threading import Thread

from sqsworkers import interfaces
from sqsworkers.bounded_executor import BoundedThreadPoolExecutor
from sqsworkers.listeners import BaseListener, BulkListener


class Crew(interfaces.CrewInterface):
    """Provide the top-level interface to Crew."""

    def __init__(
        self,
        worker_limit: int = 32,
        listeners=None,
        bulk_mode=False,
        executor=None,
        *args,
        **kwargs,
    ):
        """Instantiate a daemon thread with either a regular or bulk listener."""
        logging.info(
            "instantiating background thread with {} listener".format(
                "non-bulk" if not bulk_mode else "bulk"
            )
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
