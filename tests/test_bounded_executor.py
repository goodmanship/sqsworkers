import random
import string

import pytest

from sqsworkers.bounded_executor import BoundedThreadPoolExecutor


@pytest.fixture
def max_workers():
    return 1


@pytest.fixture
def bounded_executor(max_workers):
    with BoundedThreadPoolExecutor(max_workers=max_workers) as ex:
        yield ex


@pytest.fixture
def strings():
    return [
        "".join(random.choices(string.ascii_letters, k=5)) for _ in range(500)
    ]


def test_semaphore_is_set_correctly(bounded_executor, max_workers):
    assert bounded_executor.semaphore._value == max_workers * 10


def test_executor_function(bounded_executor, strings):
    for s in strings:
        bounded_executor.submit(print, s)
