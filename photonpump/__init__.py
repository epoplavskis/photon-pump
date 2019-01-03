import logging

from .connection import *  # noqa
from .exceptions import *  # noqa
from .messages import *  # noqa

INSANE_LEVEL_NUM = 5
TRACE_LEVEL_NUM = 9
logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")
logging.addLevelName(INSANE_LEVEL_NUM, "INSANE")


def trace(self, message, *args, **kws):
    if self.isEnabledFor(TRACE_LEVEL_NUM):
        self._log(TRACE_LEVEL_NUM, message, args, **kws)


def insane(self, message, *args, **kws):
    if self.isEnabledFor(INSANE_LEVEL_NUM):
        self._log(INSANE_LEVEL_NUM, message, args, **kws)


def get_named_logger(cls, *names):
    if names:
        return logging.getLogger(
            "%s.%s.%s"
            % (
                cls.__module__,
                cls.__name__,
                ".".join([str(name) for name in names if name is not None]),
            )
        )

    return logging.getLogger("%s.%s" % (cls.__module__, cls.__name__))


logging.Logger.trace = trace
logging.Logger.insane = insane
logging.get_named_logger = get_named_logger
logging.TRACE = TRACE_LEVEL_NUM
logging.INSANE = INSANE_LEVEL_NUM

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
