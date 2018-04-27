Photon-pump is a fast, user-friendly client for Eventstore_.

It emphasises a modular design, hidden behind an interface that's written for humans.

Installation
------------

Photon pump is available on the `cheese shop`_. ::

    pip install photonpump

You will need to install lib-protobuf 3.2.0 or above.

Basic Usage
-----------

Working with connections
~~~~~~~~~~~~~~~~~~~~~~~~

Usually you will want to interact with photon pump via the :class:`~photonpump.Connection` class. The :class:`~photonpump.Connection` is a full-duplex client that can handle many requests and responses in parallel. It is recommended that you create a single connection per application. 

First you will need to create a connection: 

    >>> import asyncio
    >>> from photonpump import connect
    >>>
    >>> loop = asyncio.get_event_loop()
    >>>
    >>> async with connect(loop=loop) as c:
    >>> await c.ping()


The :func:`photonpump.connect` function returns an async context manager so that the connection will be automatically closed when you are finished. Alternatively, you can create a connection directly:

    >>> import photonpump
    >>> conn = photonpump.Connection(loop=loop)
    >>> await conn.connect()

Reading and Writing single events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A connection can be used for both reading and writing events. You can publish a single event with the :meth:`~photonpump.Connection.publish_event` method:

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

    >>> event_number = result.last_event_number
    >>> event = await conn.get_event(stream, event_number)

Assuming that your event was published as json, you can load the body with the :meth:`~photonpump.messages.Event.json` method:

    >>> data = event.json()
    >>> assert data['Pony'] == 'Derpy Hooves'

Reading and Writing in Batches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can read and write several events in a request using the :meth:`~photonpump.Connection.get` and :meth:`~photonpump.Connection.publish` methods of our :class:`~photonpump.Connection`. the :func:`photonpump.message.NewEvent` function is a helper for constructing events.

    >>> stream = 'more_ponies'
    >>> events = [
    >>>     NewEvent('PonyJumped',
    >>>              data={
    >>>                 'Pony': 'Peculiar Hooves',
    >>>                 'Height': 9,
    >>>                 'Distance': 13
    >>>              }),
    >>>     NewEvent('PonyJumped',
    >>>              data={
    >>>                 'Pony': 'Sparkly Hooves',
    >>>                 'Height': 12,
    >>>                 'Distance': 12
    >>>              }),
    >>>     NewEvent('PonyJumped',
    >>>              data={
    >>>                 'Pony': 'Sparkly Hooves',
    >>>                 'Height': 11,
    >>>                 'Distance': 14
    >>>              })]
    >>>
    >>> await conn.publish(stream, events)

We can get events from a stream in slices by setting the `from_event_number` and `max_count` arguments. We can read events from either the front or back of the stream.

    >>> import StreamDirection from photonpump.messages
    >>>
    >>> all_events = await conn.get(stream)
    >>> assert len(all_events) == 3
    >>>
    >>> first_event = await conn.get(stream, max_count=1)[0].json()
    >>> assert first_event['Pony'] == 'Peculiar Hooves'
    >>>
    >>> second_event = await conn.get(stream, max_count=1, from_event_number=1)[0].json()
    >>> assert second_event['Pony'] == 'Sparkly Hooves'
    >>>
    >>> reversed_events = await conn.get(stream, direction=StreamDirection.backward)
    >>> assert len(reversed_events) == 3
    >>> assert reversed_events[2] == first_event

Reading with Asynchronous Generators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can page through a stream manually by using the `from_event_number` argument of :meth:`~photonpump.Connection.get`, but it's simpler to use the :meth:`~photonpump.Connection.iter` method, which returns an asynchronous generator. By default, `iter` will read from the beginning to the end of a stream, and then stop. As with `get`, you can set the :class:`~photon.messages.StreamDirection`, or use `from_event` to control the result:

    >>> async for event in conn.iter(stream):
    >>>     print (event)

This extends to asynchronous comprehensions:

    >>> async def feet_to_metres(jumps):
    >>>    async for jump in jumps:
    >>>         data = jump.json()
    >>>         data['Height'] *= 0.3048
    >>>         data['Distance'] *= 0.3048
    >>>         yield data
    >>>
    >>> jumps = (event async for event in conn.iter('ponies') 
    >>>             if event.type == 'PonyJumped')
    >>> async for jump in feet_to_metres(jumps):
    >>>     print (event)



.. _Eventstore: http://geteventstore.com
.. _cheese shop: https://pypi.python.org/pypi/photon-pump
