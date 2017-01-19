#!/usr/bin/env python

"""
@package ion.services.mi.exceptions Exception classes for MI work
@file ion/services/mi/exceptions.py
@author Edward Hunter
@brief Common exceptions used in the MI work. Specific ones can be subclassed
in the driver code.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import inspect
import sys

from mi.core.log import get_logger

log = get_logger()

from mi.exception import ApplicationException

class IonException(ApplicationException):
    status_code = -1

    def __init__(self, *a, **b):
        super(IonException, self).__init__(*a, **b)

    def get_status_code(self):
        return self.status_code

    def get_error_message(self):
        return self.message

    def __str__(self):
        return str(self.get_status_code()) + " - " + str(self.get_error_message())

class StreamException(IonException):

    def __init__(self, *a, **b):
        super(StreamException, self).__init__(*a, **b)

BadRequest = 400
Unauthorized = 401
NotFound = 404
Timeout = 408
Conflict = 409
Inconsistent = 410
FilesystemError = 411
StreamingError = 412
CorruptionError = 413
ServerError = 500
ServiceUnavailable = 503
ConfigNotFound = 540
ContainerError = 550
ContainerConfigError = 551
ContainerStartupError = 553
ContainerAppError = 554
IonInstrumentError = 600
InstConnectionError = 610
InstNotImplementedError = 620
InstParameterError = 630
InstProtocolError = 640
InstSampleError = 650
InstStateError = 660
InstUnknownCommandError = 670
InstDriverError = 680
InstTimeoutError = 690
InstDriverClientTimeoutError = 691
ResourceError = 700


# must appear after ServerError in python module
class ExceptionFactory(object):
    def __init__(self, default_type=ServerError):
        self._default = default_type
        self._exception_map = {}
        for name, obj in inspect.getmembers(sys.modules[__name__]):
            if inspect.isclass(obj):
                if hasattr(obj, "status_code"):
                    self._exception_map[str(obj.status_code)] = obj

    def create_exception(self, code, message, stacks=None):
        """ build IonException from code, message, and optionally one or more stack traces """
        if str(code) in self._exception_map:
            out = self._exception_map[str(code)](message)
        else:
            out = self._default(message)
# TEMPORARY: disable adding stacks here until JIRA OOIION-1093 fixed to avoid memory leak
#        if stacks:
#            for label, stack in stacks:
#                out.add_stack(label, stack)
        return out

class InstrumentException(ApplicationException):
    """Base class for an exception related to physical instruments or their
    representation in ION.
    """
    def __init__ (self, msg=None, error_code=ResourceError):
        super(InstrumentException, self).__init__()
        self.error_code = error_code
        self.msg = msg

    def get_triple(self):
        """ get exception info without depending on MI exception classes """
        return self.error_code, "%s: %s" % (self.__class__.__name__, self.msg), self._stacks[-1:]

class InstrumentConnectionException(InstrumentException):
    """Exception related to connection with a physical instrument"""

class InstrumentProtocolException(InstrumentException):
    """Exception related to an instrument protocol problem

    These are generally related to parsing or scripting of what is supposed
    to happen when talking at the lowest layer protocol to a device.
    @todo Add partial result property?
    """

class InstrumentStateException(InstrumentException):
    """Exception related to an instrument state of any sort"""
    def __init__ (self, msg=None):
        super(InstrumentStateException,self).__init__(msg=msg, error_code=Conflict)

class InstrumentTimeoutException(InstrumentException):
    """Exception related to a command, request, or communication timing out"""
    def __init__ (self, msg=None):
        super(InstrumentTimeoutException,self).__init__(msg=msg, error_code=Timeout)

class InstrumentDataException(InstrumentException):
    """Exception related to the data returned by an instrument or developed
    along the path of handling that data"""

class TestModeException(InstrumentException):
    """Attempt to run a test command while not in test mode"""

class InstrumentCommandException(InstrumentException):
    """A problem with the command sent toward the instrument"""

class InstrumentParameterException(InstrumentException):
    """A required parameter is not supplied"""
    def __init__ (self, msg=None):
        super(InstrumentParameterException,self).__init__(msg=msg, error_code=BadRequest)

class InstrumentParameterExpirationException(InstrumentException):
    """An instrument parameter expired"""
    def __init__(self, msg=None, error_code=None, value=None):
        super(InstrumentParameterExpirationException,self).__init__(msg=msg,
                                                                    error_code=error_code)
        self.expired_value = value
class NotImplementedException(InstrumentException):
    """ A driver function is not implemented. """

class ReadOnlyException(InstrumentException):
    pass

class SampleException(InstrumentException):
    """ An expected sample could not be extracted. """

class RecoverableSampleException(InstrumentException):
    """ An expected sample could not be extracted, but this is recoverable. """

class SampleEncodingException(SampleException):
    """ An value could not be encoded as specified. """

class UnexpectedDataException(SampleException):
    """ Data was found that was not expected. """

class DatasetHarvesterException(InstrumentException):
    """ An dataset parser encountered trouble. """

class DatasetParserException(InstrumentException):
    """ An dataset parser encountered trouble. """

class SchedulerException(InstrumentException):
    """ An error occurred in the scheduler """

class ConfigurationException(InstrumentException):
    """ A driver configuration is missing parameters or has invalid values. """

class DataSourceLocationException(InstrumentException):
    """ A driver function is not implemented. """

class UnexpectedError(InstrumentException):
    """ wrapper to send non-MI exceptions over zmq """
    def __init__ (self, msg=None):
        super(UnexpectedError,self).__init__(msg=msg, error_code=ServerError)



class PortAgentLaunchException(InstrumentException):
    """Failed to launch the port agent"""
    pass


class DriverLaunchException(InstrumentException):
    """
    A driver process failed to launch
    """
    pass


class SampleException(InstrumentException):
    """
    An expected sample could not be extracted.
    """
    pass

class PacketFactoryException(InstrumentException):
    """
    Packet factory creation failed.
    """
    pass

class PortAgentException(Exception):
    """Base class for an exception related to the port agent
    """
    def __init__ (self, msg):
        super(PortAgentException,self).__init__(msg)


class PortAgentMissingConfig(PortAgentException):
    """
    A port agnet process failed to launch
    """
    pass

class PortAgentTimeout(PortAgentException):
    """
    A port agnet process failed to launch
    """
    pass
