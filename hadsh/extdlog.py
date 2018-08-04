#!/usr/bin/env python

"""
Add TRACE and AUDIT levels to the standard Python logging module if not present.
"""

import logging

# Import logging levels from logging so we can access them all in one place.
CRITICAL = logging.CRITICAL
ERROR = logging.ERROR
WARNING = logging.WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG

# Define our own if not defined in the base class
try:
    TRACE = logging.TRACE
except AttributeError:
    TRACE = int(DEBUG/2)
    if logging.getLevelName(TRACE) != 'TRACE':
        logging.addLevelName(TRACE, 'TRACE')

try:
    AUDIT = logging.AUDIT
except AttributeError:
    AUDIT = int(TRACE/2)
    if logging.getLevelName(AUDIT) != 'AUDIT':
        logging.addLevelName(AUDIT, 'AUDIT')

BaseLogger = logging.getLoggerClass()
if not (hasattr(BaseLogger, 'trace') and \
        hasattr(BaseLogger, 'audit')):

    class ExtendedLogger(BaseLogger):
        if not hasattr(BaseLogger, 'trace'):
            def trace(self, msg, *args, **kwargs):
                self.log(TRACE, msg, *args, **kwargs)

        if not hasattr(BaseLogger, 'audit'):
            def audit(self, msg, *args, **kwargs):
                self.log(AUDIT, msg, *args, **kwargs)

    logging.setLoggerClass(ExtendedLogger)
