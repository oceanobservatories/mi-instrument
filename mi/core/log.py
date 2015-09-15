"""
MI logging can be configured using a combination of two of four files.
there is first a "base" configuration, and then a "local" set of overrides.

the base configuration is from the file specified in the environment variable MI_LOGGING_CONFIG
or res/config/mi-logging.yml (ie, users can set MI-specific configuration for drivers run from pycc container)
or config/logging.yml from within the MI egg (default to use if no mi-logging.yml was created)

then the local override may be res/config/mi-logging.local.yml (for overrides specific to MI),
or if this is not found, then res/config/logging.local.yml,
or if this is not found then no overrides.

The get_logger function is obsolete but kept to simplify transition to the ooi.logging code.

USAGE:
to configure logging from the standard MI configuration files:

    from mi.core.log import LoggerManager
    LoggerManager()

to create a logger automatically scoped with the calling package and ready to use:

    from ooi.logging import log    # no longer need get_logger at all

"""
import inspect
import logging
import os
import sys
import yaml
import pkg_resources
from types import FunctionType
from functools import wraps

from mi.core.common import Singleton
from ooi.logging import config, log

LOGGING_CONFIG_ENVIRONMENT_VARIABLE="MI_LOGGING_CONFIG"

LOGGING_PRIMARY_FROM_FILE='res/config/mi-logging.yml'
LOGGING_PRIMARY_FROM_EGG='mi-logging.yml'
LOGGING_MI_OVERRIDE='res/config/mi-logging.local.yml'
LOGGING_CONTAINER_OVERRIDE='res/config/logging.local.yml'


"""Basic pyon logging (with or without container)

   NOTE: the functionality of this module has moved to ooi.logging.config.
         currently this module is maintained for API compatability, but is implemented using the new package.
"""

import logging
from ooi.logging import config

DEFAULT_LOGGING_PATHS = ['res/config/logging.yml', 'res/config/logging.local.yml']
logging_was_configured = False

def configure_logging(logging_conf_paths, logging_config_override=None):
    """
    Public call to configure and initialize logging.
    @param logging_conf_paths  List of paths to logging config YML files (in read order)
    @param config_override  Dict with config entries overriding files read
    """
    global logging_was_configured
    logging_was_configured = True

    for path in logging_conf_paths:
        try:
            config.add_configuration(path)
        except Exception, e:
            print 'WARNING: could not load logging configuration file %s: %s' % (path, e)
    if logging_config_override:
        try:
            config.add_configuration(logging_config_override)
        except Exception,e:
            print 'WARNING: failed to apply logging override %r: %e' % (logging_config_override,e)

    # direct warnings mechanism to loggers
    logging.captureWarnings(True)


def is_logging_configured():
    """ allow caller to determine if logging has already been configured in this container """
    global logging_was_configured
    return logging_was_configured or config.get_configuration()



class LoggerManager(Singleton):
    """
    Logger Manager.  Provides an interface to configure logging at runtime.
    """
    def init(self, debug=False):
        """Initialize logging for MI.  Because this is a singleton it will only be initialized once."""
        path = os.environ[LOGGING_CONFIG_ENVIRONMENT_VARIABLE] if LOGGING_CONFIG_ENVIRONMENT_VARIABLE in os.environ else None
        haveenv = path and os.path.isfile(path)
        if path and not haveenv:
            print >> os.stderr, 'WARNING: %s was set but %s was not found (using default configuration files instead)' % (LOGGING_CONFIG_ENVIRONMENT_VARIABLE, path)
        if path and haveenv:
            config.replace_configuration(path)
            if debug:
                print >> sys.stderr, str(os.getpid()) + ' configured logging from ' + path
        elif os.path.isfile(LOGGING_PRIMARY_FROM_FILE):
            config.replace_configuration(LOGGING_PRIMARY_FROM_FILE)
            if debug:
                print >> sys.stderr, str(os.getpid()) + ' configured logging from ' + LOGGING_PRIMARY_FROM_FILE
        else:
            logconfig = pkg_resources.resource_string('mi', LOGGING_PRIMARY_FROM_EGG)
            parsed = yaml.load(logconfig)
            config.replace_configuration(parsed)
            if debug:
                print >> sys.stderr, str(os.getpid()) + ' configured logging from config/' + LOGGING_PRIMARY_FROM_FILE

        if os.path.isfile(LOGGING_MI_OVERRIDE):
            config.add_configuration(LOGGING_MI_OVERRIDE)
            if debug:
                print >> sys.stderr, str(os.getpid()) + ' supplemented logging from ' + LOGGING_MI_OVERRIDE
        elif os.path.isfile(LOGGING_CONTAINER_OVERRIDE):
            config.add_configuration(LOGGING_CONTAINER_OVERRIDE)
            if debug:
                print >> sys.stderr, str(os.getpid()) + ' supplemented logging from ' + LOGGING_CONTAINER_OVERRIDE


class LoggingMetaClass(type):
    _log_level = 'trace'

    def __new__(mcs, class_name, bases, class_dict):
        wrapped_set_name = '__wrapped'
        wrapper = log_method(class_name=class_name, log_level=mcs._log_level)
        new_class_dict = {}

        wrapped = class_dict.get(wrapped_set_name, set())

        # wrap all methods, unless they have been previously wrapped
        for attributeName, attribute in class_dict.items():
            if attributeName not in wrapped and type(attribute) == FunctionType:
                attribute = wrapper(attribute)
                wrapped.add(attributeName)
            new_class_dict[attributeName] = attribute

        new_class_dict[wrapped_set_name] = wrapped
        return type.__new__(mcs, class_name, bases, new_class_dict)


class DebugLoggingMetaClass(LoggingMetaClass):
    _log_level = 'debug'


class InfoLoggingMetaClass(DebugLoggingMetaClass):
    _log_level = 'info'


class WarnLoggingMetaClass(InfoLoggingMetaClass):
    _log_level = 'warn'


class ErrorLoggingMetaClass(WarnLoggingMetaClass):
    _log_level = 'error'


def get_logging_metaclass(log_level='trace'):
    class_map = {
        'trace': LoggingMetaClass,
        'debug': DebugLoggingMetaClass,
        'info': InfoLoggingMetaClass,
        'warn': WarnLoggingMetaClass,
        'error': ErrorLoggingMetaClass,
    }

    return class_map.get(log_level, LoggingMetaClass)


def log_method(class_name=None, log_level='trace'):
    name = "UNKNOWN_MODULE_NAME"
    stack = inspect.stack()
    # step through the stack until we leave mi.core.log
    for frame in stack:
        module = inspect.getmodule(frame[0])
        if module:
            name = module.__name__
            if name != 'mi.core.log':
                break
    logger = logging.getLogger(name)

    def wrapper(func):
        if class_name is not None:
            func_name = '%s.%s' % (class_name, func.__name__)
        else:
            func_name = func.__name__

        @wraps(func)
        def inner(*args, **kwargs):
            getattr(logger, log_level)('entered %s | args: %r | kwargs: %r', func_name, args, kwargs)
            r = func(*args, **kwargs)
            getattr(logger, log_level)('exiting %s | returning %r', func_name, r)
            return r
        return inner

    return wrapper


def get_logger():
    return log


manager = LoggerManager()
