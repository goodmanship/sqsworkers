import os
from setuptools import setup, find_packages

VERSION = '0.1.11'

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='sqsworkers',
    version=VERSION,
    packages=find_packages(exclude=('tests')),
    license='Apache License 2.0',
    long_description=read('README.rst'),
    url='https://github.com/goodmanship/sqsworkers',
    author='Rio Goodman',
    author_email='riogoodman@gmail.com',
    install_requires=[]
)