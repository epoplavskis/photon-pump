from asyncio import Future

import pytest

from photonpump import StreamingIterator


@pytest.mark.asyncio
async def test_a_single_batch(event_loop):

    fut = Future(loop=event_loop)
    gen = StreamingIterator(3)
    fut.add_done_callback(gen.enqueue_items)
    gen.finished = True

    fut.set_result([1, 2, 3])

    expected = 1

    async for i in gen:
        assert i == expected
        expected += 1


@pytest.mark.asyncio
async def test_multiple_batches(event_loop):

    batches = [
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9]
    ]

    fut = Future(loop=event_loop)
    gen = StreamingIterator(3)
    fut.add_done_callback(gen.enqueue_items)
    gen.finished = True

    fut.set_result([1, 2, 3])
    expected = 1

    async for i in gen:
        if batches:
            gen.enqueue_items(batches.pop())

        assert i == expected
        expected += 1
