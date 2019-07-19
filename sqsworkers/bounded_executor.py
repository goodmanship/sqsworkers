from concurrent import futures
from threading import BoundedSemaphore


class BoundedThreadPoolExecutor:
    """BoundedExecutor behaves as a ThreadPoolExecutor which will block on
    calls to submit() once the limit given as "bound" work items are queued for
    execution.
    """

    def __init__(self, bounded_semaphore=None, *args, **kwargs):
        self.executor = futures.ThreadPoolExecutor(*args, **kwargs)
        self.semaphore = (
            bounded_semaphore
            if bounded_semaphore is not None
            else BoundedSemaphore(value=self.executor._max_workers * 10)
        )

    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except Exception:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    def shutdown(self, wait=True):
        self.executor.shutdown(wait)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()
