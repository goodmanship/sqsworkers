SQS Workers
===========

SQS Workers gives the ability to process messages off an SQS queue using
multiple worker threads. It uses python multithreading to optimize CPU
usage since a good portion of time is spent requesting messages and
polling the SQS queue. It can be used similarly to an AWS Lambda which
cannot (as of right now) be used in conjunction with SQS.

Versions
========

::
    0.1.11 - customize queue polling
    0.1.10 - increase to max wait time for polling
    0.1.9 - get messageattributenames 
    0.1.8 - tweaks for public pypi
    0.1.7 - bugfix for thread naming
    0.1.6 - bugfix for emptry sentry client
    0.1.5 - bugfix for pip install
    0.1.4 - support for elasticmq
    0.1.2 - initial version

Usage
=====

A basic call to SQS Workers would look like this:

.. code:: python

    options = {
        'sqs_session': sqs_session,
        'queue_name': 'ddev-test-queue',
        'sqs_resource': sqs_resource,
        'MessageProcessor': MsgProcessor,
        'logger': msg_logger,
        'statsd': statsd,
        'sentry': None,
        'worker_limit': 1
      }
      c = Crew(**options)

You can see a simple demo app `here <demo/basic_message_processor.py>`__

Installation
============

Add SQS Workers to your package dependencies

*requirements.txt*

::

    sqsworkers

And then install using pip ``pip install -r requirements.txt``

Tests
=====

Make sure tests pass: ``pytest tests/test_crew.py``

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

License
=======

Copyright (c) 2017 Atlassian and others. Apache 2.0 licensed, see
`LICENSE <LICENSE>`__ file.
