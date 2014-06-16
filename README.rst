Redis
~~~~~

.. image:: https://travis-ci.org/zonyitoo/redis-py.svg?branch=master
    :target: https://travis-ci.org/zonyitoo/redis-py

Redis is an in-memory database that persists on disk. The data model is key-value, but many different kind of values are supported: Strings, Lists, Sets, Sorted Sets, Hashes

Documents could be found in http://redis.io.

*Yet another redis implementation. This version of Redis must not replace the official one.*

Requirements
------------

* Python >= 3.4.1

Usage
-----

.. code:: bash

    $ pyvenv .venv
    $ . .venv/bin/activate
    $ python setup.py install
    $ redis-server
