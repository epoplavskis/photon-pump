

Photon-pump is a fast, user-friendly client for Eventstore_.

It emphasises a modular design, hidden behind an interface that's written for humans.

Installation
------------

Photon pump is available on the `cheese shop`_. ::

    pip install photon-pump

You will need to install lib-protobuf 3.2.0 or above.

Documentation is available on `Read the docs`_.

Basic Usage
-----------

Working with connections
~~~~~~~~~~~~~~~~~~~~~~~~

Usually you will want to interact with photon pump via the `~photonpump.Client` class. The `~photonpump.Client` is a full-duplex client that can handle many requests and responses in parallel. It is recommended that you create a single connection per application.

First you will need to create a connection:

    >>> import asyncio
    >>> from photonpump import connect
    >>>
    >>> loop = asyncio.get_event_loop()
    >>>
    >>> async with connect(loop=loop) as c:
    >>>     await c.ping()


The `photonpump.connect` function returns an async context manager so that the connection will be automatically closed when you are finished. Alternatively you can create a client and manage its lifetime yourself.

    >>> import asyncio
    >>> from photonpump import connect
    >>>
    >>> loop = asyncio.get_event_loop()
    >>>
    >>> client = connect(loop=loop)
    >>> await client.connect()
    >>> await client.ping()
    >>> await client.close()

Reading and Writing single events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A connection can be used for both reading and writing events. You can publish a single event with the `~photonpump.Client.publish_event` method:

    >>> # When publishing events, you must provide the stream name.
    >>> stream = 'ponies'
    >>> event_type = 'PonyJumped'
    >>>
    >>> result = await conn.publish_event(stream, event_type, body={
    >>>     'Pony': 'Derpy Hooves',
    >>>     'Height': 10,
    >>>     'Distance': 13
    >>>     })

We can fetch a single event with the complementary `~photonpump.Client.get_event` method if we know its `event number` and the stream where it was published:

    >>> event_number = result.last_event_number
    >>> event = await conn.get_event(stream, event_number)

Assuming that your event was published as json, you can load the body with the `~photonpump.messages.Event.json` method:

.. code-block:: python

    async def write_an_event():
        async with photonpump.connect() as conn:
            await conn.publish_event('pony_stream', 'pony.jumped', body={
                'name': 'Applejack',
                'height_m': 0.6
            })


    async def read_an_event(conn):
        event = await conn.get_event('pony_stream', 1)
        print(event)


    async def write_two_events(conn):
        await conn.publish('pony_stream', [
            NewEvent('pony.jumped', body={
                'name': 'Rainbow Colossus',
                'height_m': 0.6
            },
            NewEvent('pony.jumped', body={
                'name': 'Sunshine Carnivore',
                'height_m': 1.12
            })
        ])


    async def read_two_events(conn):
        events = await conn.get('pony_stream', max_count=2, from_event=0)
        print(events[0])


    async def stneve_owt_daer(conn):
        events = await conn.get('pony_stream', direction=StreamDirection.backward, max_count=2)
        print(events[0])


    async def ticker(delay):
        while True:
            yield NewEvent('tick', body{ 'tick': i})
            i += 1
            await asyncio.sleep(delay)


    async def write_an_infinite_number_of_events(conn):
        await conn.publish('ticker_stream', ticker(1000))


    async def read_an_infinite_number_of_events(conn):
        async for event in conn.stream('ticker_stream'):
            print(event)


    >>> data = event.json()
    >>> assert data['Pony'] == 'Derpy Hooves'

Reading and Writing in Batches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can read and write several events in a request using the `~photonpump.Client.get` and `~photonpump.Client.publish` methods of our `~photonpump.Client`. the `photonpump.message.NewEvent` function is a helper for constructing events.

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

We can page through a stream manually by using the `from_event_number` argument of `~photonpump.Client.get`, but it's simpler to use the `~photonpump.Client.iter` method, which returns an asynchronous generator. By default, `iter` will read from the beginning to the end of a stream, and then stop. As with `get`, you can set the `~photon.messages.StreamDirection`, or use `from_event` to control the result:

    >>> async for event in conn.iter(stream):
    >>>     print (event)

This extends to asynchronous comprehensions:

    >>> async def feet_to_metres(jumps):
    >>>    async for jump in jumps:
    >>>         data = jump.json()
    >>>         data['Height'] = data * 0.3048
    >>>         data['Distance'] = data * 0.3048
    >>>         yield data
    >>>
    >>> jumps = (event async for event in conn.iter('ponies')
    >>>             if event.type == 'PonyJumped')
    >>> async for jump in feet_to_metres(jumps):
    >>>     print (event)


Persistent Subscriptions
~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes we want to watch a stream continuously and be notified when a new event occurs. Eventstore supports volatile and persistent subscriptions for this use case.

A persistent subscription stores its state on the server. When your application restarts, you can connect to the subscription again and continue where you left off. Multiple clients can connect to the same persistent subscription to support competing consumer scenarios. To support these features, persistent subscriptions have to run against the master node of an Eventstore cluster.

Firstly, we need to `create the subscription <photonpump.connection.Client.create_subscription>`.

    >>> async def create_subscription(subscription_name, stream_name, conn):
    >>>     await conn.create_subscription(subscription_name, stream_name)

Once we have a subscription, we can `connect to it <photonpump.connection.Client.connect_subscription>` to begin receiving events. A persistent subscription exposes an `events` property, which acts like an asynchronous iterator.

    >>> async def read_events_from_subscription(subscription_name, stream_name, conn):
    >>>     subscription = await conn.connect_subscription(subscription_name, stream_name)
    >>>     async for event in subscription.events:
    >>>         print(event)
    >>>         await subscription.ack(event)

Eventstore will send each event to one consumer at a time. When you have handled the event, you must acknowledge receipt. Eventstore will resend messages that are unacknowledged.


Volatile Subscriptions
~~~~~~~~~~~~~~~~~~~~~~

In a Volatile Subscription, state is stored by the client. When your application restarts, you must re-subscribe to the stream. There is no support in Eventstore for competing consumers to a volatile subscription. Volatile subscriptions can run against any node in a cluster.

Volatile subsciptions do not support event acknowledgement.

    >>> async def subscribe_to_stream(stream, conn):
    >>>     subscription = await conn.subscribe_to(stream)
    >>>     async for event in subscription.events:
    >>>         print(event)


High-Availability Scenarios
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Eventstore supports an HA-cluster deployment topology. In this scenario, Eventstore runs a master node and multiple slaves. Some operations, particularly persistent subscriptions and projections, are handled only by the master node. To connect to an HA-cluster and automatically find the master node, photonpump supports cluster discovery.

The cluster discovery interrogates eventstore gossip to find the active master. You can provide the IP of a maching in the cluster, or a DNS name that resolves to some members of the cluster, and photonpump will discover the others.

    >>> async def connect_to_cluster(hostname_or_ip, port=2113):
    >>>     with connect(discovery_host=hostname_or_ip, discovery_port=2113) as c:
    >>>         await c.ping()

If you provide both a `host` and `discovery_host`, photonpump will prefer discovery.

.. _Eventstore: http://geteventstore.com
.. _cheese shop: https://pypi.python.org/pypi/photon-pump
.. _Read the docs: http://photon-pump.readthedocs.io/en/latest/


Debugging
~~~~~~~~~

If you want to step through code that uses photonpump, it's helpful to be aware that Event Store's TCP API (which photonpump uses) makes use of a 'heartbeat' to ensure that connections are not left open. This means that if you're sitting at a debugger (e.g. pdb) prompt -- and therefore not running the event loop for tens of seconds at a time -- you'll find that you get disconnected. To prevent that, you can run it with Event Store's heartbeat timeouts set to high values -- e.g. with a `Dockerfile` `like this <http://github.com/jlee1-made/resting-eventstore>`_.
