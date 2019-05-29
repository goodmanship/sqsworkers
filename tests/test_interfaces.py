from sqsworkers.interfaces import CrewInterface, StatsDInterface

import pytest


@pytest.fixture
def valid_crew_subclass():
    class Crew:
        def __init__(self):
            """"""

        def start(self):
            """"""

        def foo(self):
            """"""

    return Crew


@pytest.fixture
def invalid_crew_subclass():
    class Crew:
        """"""

        def __init__(self):
            """"""

    return Crew


@pytest.fixture
def valid_statsd_interface():
    class StatsD:
        def __init__(self):
            """"""

        def increment(self):
            """"""

        def gauge(self):
            """"""

        def foo(self):
            """"""

    return StatsD


@pytest.fixture
def invalid_statsd_interface():
    class StatsD:
        def increment(self):
            """"""

    return StatsD


def test_crew_interface(valid_crew_subclass, invalid_crew_subclass):
    """"""
    assert issubclass(valid_crew_subclass, CrewInterface)
    assert not issubclass(invalid_crew_subclass, CrewInterface)


def test_statsd_interface(valid_statsd_interface, invalid_statsd_interface):
    assert issubclass(valid_statsd_interface, StatsDInterface)
    assert not issubclass(invalid_statsd_interface, StatsDInterface)


def test_invalid_statsd_init():
    with pytest.raises(TypeError):

        class Invalid(StatsDInterface):
            """"""

        Invalid()


def test_invalid_crew_init():
    with pytest.raises(TypeError):

        class Invalid(CrewInterface):
            """"""

        Invalid()
