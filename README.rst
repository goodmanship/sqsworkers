SQS Workers
===========

SQS Workers gives the ability to process messages off an SQS queue using
multiple worker threads. It uses python multithreading to optimize CPU
usage since a good portion of time is spent requesting messages and
polling the SQS queue. It can be used similarly to an AWS Lambda which
cannot (as of right now) be used in conjunction with SQS.


Usage
=====

A basic call to SQS Workers would look like this:

.. code:: python

    from sqsworkers.crew import Crew


    crew = Crew(
        sqs_session=session,
        queue_name=queue_name,
        message_processor=lambda msg: print(f"hello, {msg}")
    )

    crew.start()

The `tests <tests/test_crew.py>`__ should be informative

Installation
============

Add SQS Workers to your package dependencies

*requirements.txt*

::

    sqsworkers

And then install using pip ``pip install -r requirements.txt``

Tests
=====

Ensure tests pass

::

    git clone https://github.com/goodmanship/sqsworkers
    cd sqsworkers
    python3 -m venv venv
    . venv/bin/activate
    pip install -e .[dev]
    pytest --cov sqsworkers --mypy --black tests/


Contributors
============

Pull requests, issues and comments welcome. For pull requests:

-  Add tests for new features and bug fixes
-  Follow the existing style
-  Separate unrelated changes into multiple pull requests

See the existing issues for things to start contributing.

For bigger changes, make sure you start a discussion first by creating
an issue and explaining the intended change.

Atlassian requires contributors to sign a Contributor License Agreement,
known as a CLA. This serves as a record stating that the contributor is
entitled to contribute the code/documentation/translation to the project
and is willing to have it used in distributions and derivative works (or
is willing to transfer ownership).

Prior to accepting your contributions we ask that you please follow the
appropriate link below to digitally sign the CLA. The Corporate CLA is
for those who are contributing as a member of an organization and the
individual CLA is for those contributing as an individual.

-  `CLA for corporate
   contributors <https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=e1c17c66-ca4d-4aab-a953-2c231af4a20b>`__
-  `CLA for
   individuals <https://na2.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=3f94fbdc-2fbe-46ac-b14c-5d152700ae5d>`__

Versions
========

- 0.2.0 - rewrite implemention on top of concurrent.futures; adds tests
- 0.1.13 - support bulk message processor
- 0.1.12 - adding exception handler
- 0.1.11 - customize queue polling
- 0.1.10 - increase to max wait time for polling
- 0.1.9 - get messageattributenames
- 0.1.8 - tweaks for public pypi
- 0.1.7 - bugfix for thread naming
- 0.1.6 - bugfix for emptry sentry client
- 0.1.5 - bugfix for pip install
- 0.1.4 - support for elasticmq
- 0.1.2 - initial version

License
=======

Copyright (c) 2019 Atlassian and others. Apache 2.0 licensed, see
`LICENSE <LICENSE>`__ file.
