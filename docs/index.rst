.. photon-pump documentation master file, created by
   sphinx-quickstart on Fri Apr 28 23:45:35 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Photon-Pump Documentation
=========================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   `Installation`_
   api

Photon-pump is a fast, user-friendly client for Eventstore_.

It emphasis a modular design, hidden behind an interface that's written for humans.

Installation
------------

Photon pump is available on the `cheese shop`_. ::

    pip install photonpump

You will need to install lib-protobuf 3.2.0 or above.

Basic Usage
-----------

Usually you will want to interact with photon pump via the :class:`~photonpump.Connection` class.

First you will need to create a connection: 

    >>> import asyncio
    >>> from photonpump import connect
    >>>
    >>>  loop = asyncio.get_event_loop()
    >>>
    >>> async with connect(loop=loop) as c:
    >>> await c.ping()

The :func:`photonpump.connect` function returns an async context manager so that the connection will be automatically closed when you are finished. Alternatively, you can create a connection directly:

    >>> import photonpump
    >>> conn = photonpump.Connection(loop=loop)
    >>> await conn.connect()

The :class:`~photonpump.Connection` is a full-duplex client that can handle many requests and responses in parallel. It is recommended that you create a single connection per application. A connection can be used for both reading and writing events. You can publish a single event with the :meth:`~photonpump.Connection.publish_event` method:

    >>> # When publishing events, you must provide the stream name.
    >>> stream = 'ponies'
    >>> event_type = 'PonyJumped'
    >>>
    >>> result = await conn.publish_event(stream, event_type, body={
    >>>     'Pony': 'Derpy Hooves',
    >>>     'Height': 10,
    >>>     'Distance': 13
    >>>     })

We can fetch a single event with the complementary :meth:`~photonpump.Connection.get_event` method if we know its `event number` and the stream where it was published:

    >>> event = await conn.get_event(stream, result.last_event_number)

Assuming that your event was published as json, you can load the body with the :meth:`~photonpump.messages.Event.json` method:

    >>> data = event.json()
    >>> assert data['Pony'] == 'Derpy Hooves'


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Eventstore: http://geteventstore.com
.. _cheese shop: https://pypi.python.org/pypi/photon-pump
