import os
from setuptools import setup, find_packages

VERSION = "0.2.0"
DEVELOPMENT_REQUIREMENTS = [
    "pytest",
    "coverage",
    "pygments",
    "pytest-cov",
    "mypy",
    "pytest-mypy",
    "black",
    "pytest-black",
]


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="sqsworkers",
    version=VERSION,
    packages=find_packages(exclude=("tests")),
    license="Apache License 2.0",
    long_description=read("README.rst"),
    url="https://github.com/goodmanship/sqsworkers",
    author="Rio Goodman",
    author_email="riogoodman@gmail.com",
    install_requires=["boto3", "dataclasses"],
    extras_require={
        "dev": DEVELOPMENT_REQUIREMENTS,
        "test": DEVELOPMENT_REQUIREMENTS,
    },
)
