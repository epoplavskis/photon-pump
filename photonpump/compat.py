import asyncio

# python 3.6 doesn't have "get_running_loop"
get_running_loop = getattr(asyncio, "get_running_loop", asyncio.get_event_loop)
