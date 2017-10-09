import os
from setuptools import setup, find_packages

VERSION = '0.1.7'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='sqsworkers',
    version=VERSION,
    packages=find_packages(exclude=('tests')),
    license='Apache License 2.0',
    long_description=read('README.md'),
    url='https://bitbucket.org/atlassian/sqsworkers/overview',
    author='Rio Goodman',
    author_email='rgoodman@atlassian.com',
    install_requires=[
        'boto3',
        'datadog',
        'raven'
    ]
)