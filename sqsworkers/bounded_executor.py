from concurrent import futures
from threading import BoundedSemaphore


class BoundedThreadPoolExecutor:
    """
    This executor implements a subset of the concurrent.futures.Executor API.

    The main difference is that one can choose to set a limit on the executor's
    worker queue -- making it so that calls to .submit will block one that
    limit is reached.

    By default; 10 messages can be enqueued per-thread.
    """

    def __init__(
        self,
        bounded_semaphore=None,
        thread_pool_executor=None,
        *args,
        **kwargs
    ):
        self.executor = (
            thread_pool_executor
            if thread_pool_executor is not None
            else futures.ThreadPoolExecutor(*args, **kwargs)
        )
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
