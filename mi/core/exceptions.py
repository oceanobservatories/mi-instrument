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

from mi.core.common import BaseEnum
from ooi.exception import ApplicationException

BadRequest = 400
Timeout = 408
Conflict = 409
ResourceError = 700
ServerError = 500

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

class BadRequest(IonException):
    '''
    Incorrectly formatted client request
    '''
    status_code = 400


class Unauthorized(IonException):
    '''
    Client failed policy enforcement
    '''
    status_code = 401


class NotFound(IonException):
    ''''
    Requested resource not found
    '''
    status_code = 404


class Timeout(IonException):
    '''
    Client request timed out
    '''
    status_code = 408


class Conflict(IonException):
    '''
    Client request failed due to conflict with the current state of the resource
    '''
    status_code = 409


class Inconsistent(IonException):
    '''
    Client request failed due to internal error of the datastore
    '''
    status_code = 410

class FilesystemError(StreamException):
    """
    """
    status_code = 411

class StreamingError(StreamException):
    """
    """
    status_code = 412

class CorruptionError(StreamException):
    """
    """
    status_code = 413

class ServerError(IonException):
    '''
    For reporting generic service failure
    '''
    status_code = 500


class ServiceUnavailable(IonException):
    '''
    Requested service not started or otherwise unavailable
    '''
    status_code = 503


class ConfigNotFound(IonException):
    '''
    '''
    status_code = 540


class ContainerError(IonException):
    '''
    '''
    status_code = 550


class ContainerConfigError(ContainerError):
    '''
    '''
    status_code = 551


class ContainerStartupError(ContainerError):
    '''
    '''
    status_code = 553


class ContainerAppError(ContainerError):
    '''
    '''
    status_code = 554


class IonInstrumentError(IonException):
    """
    """
    status_code = 600


class InstConnectionError(IonInstrumentError):
    """
    """
    status_code = 610


class InstNotImplementedError(IonInstrumentError):
    """
    """
    status_code = 620


class InstParameterError(IonInstrumentError):
    """
    """
    status_code = 630


class InstProtocolError(IonInstrumentError):
    """
    """
    status_code = 640


class InstSampleError(IonInstrumentError):
    """
    """
    status_code = 650


class InstStateError(IonInstrumentError):
    """
    """
    status_code = 660


class InstUnknownCommandError(IonInstrumentError):
    """
    """
    status_code = 670


class InstDriverError(IonInstrumentError):
    """
    """
    status_code = 680


class InstTimeoutError(IonInstrumentError):
    """
    """
    status_code = 690


class InstDriverClientTimeoutError(IonInstrumentError):
    """
    A special kind of timeout that only applies at the driver client level (not an instrument timeout).
    """
    status_code = 691

class ResourceError(IonException):
    """
    A taskable resource error occurred.
    """
    status_code = 700


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
        super(InstrumentException,self).__init__()
        self.args = (error_code, msg)
        self.error_code = error_code
        self.msg = msg

    def get_triple(self):
        """ get exception info without depending on MI exception classes """
        return ( self.error_code.status_code, "%s: %s" % (self.__class__.__name__, self.msg), self._stacks )
    
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
