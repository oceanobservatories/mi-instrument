#!/usr/bin/env python
# coding=utf-8

"""
@package ion.services.mi.common Common classes for MI work
@file ion/services/mi/common.py
@author Steve Foley
@author Edward Hunter
@brief Common enumerations, constants, utilities used in the MI work
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

import yaml
import collections

"""Default timeout value in seconds"""
DEFAULT_TIMEOUT = 10

class Singleton(object):
    """
    Singleton interface:
    http://www.python.org/download/releases/2.2.3/descrintro/#__new__
    """
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass

class Config(object):
    """
    Read yaml config files.
    """
    config = None

    def __init__(self, files = None, content = None):
        result = None

        if files:
            content = []
            for file in files:
                infile = open(file, 'r')
                c = infile.read()
                infile.close()
                content.append(c)

        self._store_content(content)

    def _store_content(self, content_list):
        result = []
        for content in content_list:
            if content:
                input_config = yaml.load(content)
                if result:
                    self._merge(result, input_config, True)
                else:
                    result = input_config;

        self.config = result

    def _quacks_like_dict(self, object):
        """Check if object is dict-like"""
        return isinstance(object, collections.Mapping)

    def _merge(self, base, upd, inplace=False):
        """Merge two deep dicts non-destructively.
        Uses a stack to avoid maximum recursion depth exceptions.
        @param base the dict to merge into
        @param upd the content to merge
        @param inplace change base if True
        @retval the merged dict (base of inplace else a merged copy)
        """
        assert self._quacks_like_dict(base), self._quacks_like_dict(upd)
        dst = base if inplace else base.copy()

        stack = [(dst, upd)]
        while stack:
            current_dst, current_src = stack.pop()
            for key in current_src:
                if key not in current_dst:
                    current_dst[key] = current_src[key]
                else:
                    if self._quacks_like_dict(current_src[key]) and self._quacks_like_dict(current_dst[key]) :
                        stack.append((current_dst[key], current_src[key]))
                    else:
                        current_dst[key] = current_src[key]
        return dst

    def as_dict(self):
        return self.config
    
class BaseEnum(object):
    """Base class for enums.
    
    Used to code agent and instrument states, events, commands and errors.
    To use, derive a class from this subclass and set values equal to it
    such as:
    @code
    class FooEnum(BaseEnum):
       VALUE1 = "Value 1"
       VALUE2 = "Value 2"
    @endcode
    and address the values as FooEnum.VALUE1 after you import the
    class/package.
    
    Enumerations are part of the code in the MI modules since they are tightly
    coupled with what the drivers can do. By putting the values here, they
    are quicker to execute and more compartmentalized so that code can be
    re-used more easily outside of a capability container as needed.
    """
    
    @classmethod
    def list(cls):
        """List the values of this enum."""
        return [getattr(cls,attr) for attr in dir(cls) if\
                not callable(getattr(cls,attr)) and not attr.startswith('__')]

    @classmethod
    def dict(cls):
        """Return a dict representation of this enum."""
        result = {}
        for attr in dir(cls):
            if not callable(getattr(cls,attr)) and not attr.startswith('__'):
                result[attr] = getattr(cls,attr)
        return result

    @classmethod
    def has(cls, item):
        """Is the object defined in the class?
        
        Use this function to test
        a variable for enum membership. For example,
        @code
        if not FooEnum.has(possible_value)
        @endcode
        @param item The attribute value to test for.
        @retval True if one of the class attributes has value item, false
        otherwise.
        """
        return item in cls.list()

class EventKey(BaseEnum):
    """Keys to the event dictionary fields as used by the InstrumentProtocol
    and InstrumentDriver classes.
    """
    TYPE = 'type'
    ERROR_CODE = 'error_code'
    MESSAGE = 'message'


###############################################################################
# Common unit abbreviation strings. Taken from the IEEE Recommended Unit Symbols, SI Prefixes and Abbreviations.
##############################################################################
class Prefixes(BaseEnum):
    YOTTA = 'Y'   # 10^24
    ZETTA = 'Z'   # 10^21
    EXA =   'E'   # 10^18
    PETA =  'P'   # 10^15
    TERA =  'T'   # 10^12
    GIGA =  'G'   # 10^09
    MEGA =  'M'   # 10^06
    KILO =  'k'   # 10^03
    HECTO = 'h'   # 10^02
    DEKA =  'da'  # 10^01
    DECI =  'd'   # 10^-01
    CENTI = 'c'   # 10^-02
    MILLI = 'm'   # 10^-03
    MICRO = 'µ'   # 10^-06
    NANO =  'n'   # 10^-09
    PICO =  'p'   # 10^-12
    FEMTO = 'f'   # 10^-15
    ATTO =  'a'   # 10^-18
    ZEPTO = 'z'   # 10^-21
    YOCTO = 'y'   # 10^-24


class Units(BaseEnum):
    COUNTS = 'counts'
    ALTERNATING_CURRENT = 'ac'
    AMERICAN_WIRE_GAUGE = 'AWG'
    AMPERE = 'A'
    AMPERE_HOUR = 'Ah'
    AMPERE_TURN = 'A'
    AMPLITUDE_MODULATION = 'AM'
    ANTILOGARITHM = 'antilog'
    AUDIO_FREQUENCY = 'AF'
    AUTOMATIC_FREQUENCY_CONTROL = 'AFC'
    AUTOMATIC_GAIN_CONTROL = 'AGC'
    AUTOMATIC_VOLUME_CONTROL = 'AVC'
    AVERAGE = 'avg'
    BAUD = 'Bd'
    BEAT_FREQUENCY_OSCILLATOR = 'BFO'
    BINARY_CODED_DECIMAL = 'BCD'
    BIT = 'b'
    BYTE = 'B'
    BRITISH_THERMAL_UNIT = 'Btu'
    CALORIE = 'cal'
    CANDELA = 'cd'
    CANDELA_PER_SQUARE_FOOT = 'cd/ft²'
    CANDELA_PER_SQUARE_METER = 'cd/m²'
    CATHODE_RAY_OSCILLOSCOPE = 'CRO'
    CATHODE_RAY_TUBE = 'CRT'
    CENTIMETER = 'cm'
    CIRCULAR_MIL = 'cmil'
    CONTINUOUS_WAVE = 'CW'
    COULOMB = 'C'
    CUBIC_CENTIMETER = 'cm³'
    CUBIC_FOOT_PER_MINUTE = 'ft³/min'
    CUBIC_METER = 'm³'
    CUBIC_METER_PER_SECOND = 'm³/s'
    DAY = 'd'
    DECIBEL = 'dB'
    DEGREE_CELSIUS = '°C'
    DEGREE_FAHRENHEIT = '°F'
    DEGREE_PLANE_ANGLE = '...°'
    DEGREE_PLANE_ANGLE_PER_SECOND = '...°/s'
    DEGREE_RANKINE = '°R'
    DEGREE_TEMPERATURE_INTERVAL = 'deg'
    DIAMETER = 'diam'
    DIRECT_CURRENT = 'dc'
    ELECTROMAGNETIC_COMPATIBILITY = 'EMC'
    ELECTROMAGNETIC_UNIT = 'EMU'
    ELECTROMOTIVE_FORCE = 'EMF'
    ELECTRONIC_DATA_PROCESSING = 'EDP'
    ELECTRONVOLT = 'eV'
    ELECTROSTATIC_UNIT = 'ESU'
    EXTRA_HIGH_VOLTAGE = 'EHV'
    EXTREMELY_HIGH_FREQUENCY = 'EHF'
    EXTREMELY_LOW_FREQUENCY = 'ELF'
    FARAD = 'F'
    FIELD_EFFECT_TRANSISTOR = 'FET'
    FOOT = 'ft'
    FOOT_PER_MINUTE = 'ft/min'
    FOOT_PER_SECOND = 'ft/s'
    FOOT_POUND_FORCE = 'ft · lbf'
    FREQUENCY_MODULATION = 'FM'
    GALLON = 'gal'
    GALLON_PER_MINUTE = 'gal/min'
    GAUSS = 'G'
    GIGAELECTRONVOLT = 'GeV'
    GIGAHERTZ = 'GHz'
    GRAM = 'g'
    HENRY = 'H'
    HERTZ = 'Hz'
    HIGH_VOLTAGE = 'HV'
    HOUR = 'h'
    INCH = 'in'
    INCH_PER_SECOND = 'in/s'
    INDUCTANCE_CAPACITANCE = 'LC'
    INERTIA = 'kg · m2'
    INFRARED = 'IR'
    INSIDE_DIAMETER = 'ID'
    INTERMEDIATE_FREQUENCY = 'IF'
    JOULE = 'J'
    JOULE_PER_DEGREE = 'J/deg'
    KILOJOULE = 'kJ'
    KILOMETER = 'km'
    KILOMETER_PER_HOUR = 'km/h'
    KILOVAR = 'kvar'
    KILOVOLT = 'kV'
    KILOVOLTAMPERE = 'kVA'
    KILOWATT = 'kW'
    KILOWATTHOUR = 'kWh'
    KILOELECTRONVOLT = 'keV'
    KILOGRAM = 'kg'
    KILOHERTZ = 'kHz'
    KILOHM = 'k6'
    KELVIN = 'K'
    LAMBERT = 'L'
    LITER = 'L'
    LITER_PER_SECOND = 'L/s'
    LOGARITHM = 'log'
    LOGARITHM_NATURAL = 'ln'
    LOW_FREQUENCY = 'LF'
    LUMEN = 'lm'
    LUMEN_PER_SQUARE_FOOT = 'lm/ft²'
    LUMEN_PER_SQUARE_METER = 'lm/m²'
    LUMEN_PER_WATT = 'lm/W'
    LUMEN_SECOND = 'lm · s'
    LUX = 'lx'
    MAGNETOHYDRODYNAMICS = 'MHD'
    MAGNETOMOTIVE_FORCE = 'MMF'
    MEDIUM_FREQUENCY = 'MF'
    MEGAELECTRONVOLT = 'MeV'
    MEGAHERTZ = 'MHZ'
    MEGAVOLT = 'MV'
    MEGAWATT = 'MW'
    MEGOHM = 'MΩ'
    METAL_OXIDE_SEMICONDUCTOR = 'MOS'
    METER = 'm'
    METER_KILOGRAM_SECOND = 'MKS'
    MICROAMPERE = 'µA'
    MICROFARAD = 'µF'
    MICROGRAM = 'µg'
    MICROHENRY = 'µH'
    MICROMETER = 'µm'
    MICROOHM = 'µΩ'
    MICROSECOND = 'µs'
    MICROWATT = 'µW'
    MILE_PER_HOUR = 'mi/h'
    MILE = 'mi'
    MILLIAMPERE = 'mA'
    MILLIGRAM = 'mg'
    MILLIHENRY = 'mH'
    MILLILITER = 'ml'
    MILLIMETER = 'mm'
    MILLISECOND = 'ms'
    MILLIVOLT = 'mV'
    MILLIWATT = 'mW'
    MINUTE = 'min'
    MINUTE_PLANE_ANGLE = '...’'
    NANOFARAD = 'nF'
    NANOSECOND = 'ns'
    NANOMETER = 'nm'
    NANOWATT = 'nW'
    NEPER = 'Np'
    NEWTON = 'N'
    NEWTON_METER = 'N · m'
    NEWTON_PER_SQUARE_METER = 'N/m²'
    OHM = 'Ω'
    OUNCE_AVOIRDUPOIS = 'oz'
    PERCENT = '%'
    PER_UNIT = 'pu'
    PHASE_MODULATION = 'PM'
    PICOAMPERE = 'pA'
    PICOFARAD = 'pF'
    PICOSECOND = 'ps'
    PICOWATT = 'pW'
    POUND = 'lb'
    POUNDAL = 'pdl'
    POUND_FORCE = 'lbf'
    POUND_FORCE_FOOT = 'lbf · ft'
    POUND_FORCE_PER_SQUARE_INCH = 'lbf/in²'
    POUND_PER_SQUARE_INCH = 'lb/in²'  # preferred over psi
    POWER_FACTOR = 'PF'
    RADIAN = 'rad'
    RADIO_FREQUENCY = 'RF'
    RADIO_FREQUENCY_INTERFERENCE = 'RFI'
    RESISTANCE_CAPACITANCE = 'RC'
    RESISTANCE_INDUCTANCE_CAPACITANCE = 'RLC'
    REVOLUTION_PER_MINUTE = 'r/min'
    REVOLUTION_PER_SECOND = 'r/s'
    ROENTGEN = 'R'
    ROOT_MEAN_SQUARE = 'rms'
    SECOND_PLANE_ANGLE = '...”'
    SECOND = 's'
    SHORT_WAVE = 'SW'
    SIEMENS = 'S'
    SIGNAL_TO_NOISE_RATIO = 'SNR'
    SILICON_CONTROLLED_RECTIFIER = 'SCR'
    SQUARE_FOOT = 'ft²'
    SQUARE_INCH = 'in²'
    SQUARE_METER = 'm²'
    SQUARE_YARD = 'yd²'
    STANDING_WAVE_RATIO = 'SWR'
    TELEVISION_INTERFERENCE = 'TVI'
    TESLA = 'T'
    THOUSAND_CIRCULAR_MILS = 'kcmil'
    TRANSVERSE_ELECTRIC = 'TE'
    TRANSVERSE_ELECTROMAGNETIC = 'TEM'
    TRANSVERSE_MAGNETIC = 'TM'
    TRAVELING_WAVE_TUBE = 'TWT'
    VACUUM_TUBE_VOLTMETER = 'VTVM'
    VAR = 'var'
    VARIABLE_FREQUENCY_OSCILLATOR = 'VFO'
    VERY_HIGH_FREQUENCY = 'VHF'
    VOLT = 'V'
    VOLTAGE_CONTROLLED_OSCILLATOR = 'VCO'
    VOLTAGE_STANDING_WAVE_RATIO = 'VSWR'
    VOLTAMPERE = 'VA'
    WATT = 'W'
    WATTHOUR = 'Wh'
    WATT_PER_STERADIAN = 'W/sr'
    WATT_PER_STERADIAN_SQUARE_METER = 'W/(sr · m²)'
    WEBER = 'Wb'
    YARD = 'yd'


###############################################################################
# Error constants.
##############################################################################

class InstErrorCode(BaseEnum):
    """Error codes generated by instrument drivers and agents"""
    
    OK = ['OK']
    INVALID_DESTINATION = ['ERROR_INVALID_DESTINATION','Intended destination for a message or operation is not valid.']
    TIMEOUT = ['ERROR_TIMEOUT','The message or operation timed out.']
    NETWORK_FAILURE = ['ERROR_NETWORK_FAILURE','A network failure has been detected.']
    NETWORK_CORRUPTION = ['ERROR_NETWORK_CORRUPTION','A message passing through the network has been determined to be corrupt.']
    OUT_OF_MEMORY = ['ERROR_OUT_OF_MEMORY','There is no more free memory to complete the operation.']
    LOCKED_RESOURCE = ['ERROR_LOCKED_RESOURCE','The resource being accessed is in use by another exclusive operation.']
    RESOURCE_NOT_LOCKED = ['ERROR_RESOURCE_NOT_LOCKED','Attempted to unlock a free resource.']
    RESOURCE_UNAVAILABLE = ['ERROR_RESOURCE_UNAVAILABLE','The resource being accessed is unavailable.']
    TRANSACTION_REQUIRED = ['ERROR_TRANSACTION_REQUIRED','The operation requires a transaction with the agent.']
    UNKNOWN_ERROR = ['ERROR_UNKNOWN_ERROR','An unknown error has been encountered.']
    PERMISSION_ERROR = ['ERROR_PERMISSION_ERROR','The user does not have the correct permission to access the resource in the desired way.']
    INVALID_TRANSITION = ['ERROR_INVALID_TRANSITION','The transition being requested does not apply for the current state.']
    INCORRECT_STATE = ['ERROR_INCORRECT_STATE','The operation being requested does not apply to the current state.']
    UNKNOWN_EVENT = ['ERROR_UNKNOWN_EVENT','The event is not defined for this driver.']
    UNHANDLED_EVENT = ['ERROR_UNHANDLED_EVENT','The event was not handled by the state.']
    UNKNOWN_TRANSITION = ['ERROR_UNKNOWN_TRANSITION','The specified state transition does not exist.']
    CANNOT_PUBLISH = ['ERROR_CANNOT_PUBLISH','An attempt to publish has failed.']
    INSTRUMENT_UNREACHABLE = ['ERROR_INSTRUMENT_UNREACHABLE','The agent cannot communicate with the device.']
    MESSAGING_ERROR = ['ERROR_MESSAGING_ERROR','An error has been encountered during a messaging operation.']
    HARDWARE_ERROR = ['ERROR_HARDWARE_ERROR','An error has been encountered with a hardware element.']
    WRONG_TYPE = ['ERROR_WRONG_TYPE','The type of operation is not valid in the current state.']
    INVALID_COMMAND = ['ERROR_INVALID_COMMAND','The command is not valid in the given context.']
    UNKNOWN_COMMAND = ['ERROR_UNKNOWN_COMMAND','The command is not recognized.']
    UNKNOWN_CHANNEL = ['ERROR_UNKNOWN_CHANNEL','The channel is not recognized.']
    INVALID_CHANNEL = ['ERROR_INVALID_CHANNEL','The channel is not valid for the requested command.']
    NOT_IMPLEMENTED = ['ERROR_NOT_IMPLEMENTED','The command is not implemented.']
    INVALID_TRANSACTION_ID = ['ERROR_INVALID_TRANSACTION_ID','The transaction ID is not a valid value.']
    INVALID_DRIVER = ['ERROR_INVALID_DRIVER','Driver or driver client invalid.']
    GET_OBSERVATORY_ERR = ['ERROR_GET_OBSERVATORY','Could not retrieve all parameters.']
    EXE_OBSERVATORY_ERR = ['ERROR_EXE_OBSERVATORY','Could not execute observatory command.']
    SET_OBSERVATORY_ERR = ['ERROR_SET_OBSERVATORY','Could not set all parameters.']
    PARAMETER_READ_ONLY = ['ERROR_PARAMETER_READ_ONLY','Parameter is read only.']
    INVALID_PARAMETER = ['ERROR_INVALID_PARAMETER','The parameter is not available.']
    DUPLICATE_PARAMETER = ['ERROR_DUPLICATE_PARAMETER', 'Duplicate parameter.']
    REQUIRED_PARAMETER = ['ERROR_REQUIRED_PARAMETER','A required parameter was not specified.']
    INVALID_PARAM_VALUE = ['ERROR_INVALID_PARAM_VALUE','The parameter value is out of range.']
    INVALID_METADATA = ['ERROR_INVALID_METADATA','The metadata parameter is not available.']
    NO_PARAM_METADATA = ['ERROR_NO_PARAM_METADATA','The parameter has no associated metadata.']
    INVALID_STATUS = ['ERROR_INVALID_STATUS','The status parameter is not available.']
    INVALID_CAPABILITY = ['ERROR_INVALID_CAPABILITY','The capability parameter is not available.']
    BAD_DRIVER_COMMAND = ['ERROR_BAD_DRIVER_COMMAND','The driver did not recognize the command.']
    EVENT_NOT_HANDLED = ['ERROR_EVENT_NOT_HANDLED','The current state did not handle a received event.']
    GET_DEVICE_ERR = ['ERROR_GET_DEVICE','Could not retrieve all parameters from the device.']
    EXE_DEVICE_ERR = ['ERROR_EXE_DEVICE','Could not execute device command.']
    SET_DEVICE_ERR = ['ERROR_SET_DEVICE','Could not set all device parameters.']
    ACQUIRE_SAMPLE_ERR = ['ERROR_ACQUIRE_SAMPLE','Could not acquire a data sample.']
    DRIVER_NOT_CONFIGURED = ['ERROR_DRIVER_NOT_CONFIGURED','The driver could not be configured.']
    DISCONNECT_FAILED = ['ERROR_DISCONNECT_FAILED','The driver could not be properly disconnected.']    
    AGENT_INIT_FAILED = ['ERROR_AGENT_INIT_FAILED','The agent could not be initialized.']    
    AGENT_DEINIT_FAILED = ['ERROR_AGENT_DEINIT_FAILED','The agent could not be deinitialized.']    
    DRIVER_CONNECT_FAILED = ['ERROR_DRIVER_CONNECT_FAILED','The agent could not connect to the driver.']    
    DRIVER_DISCONNECT_FAILED = ['ERROR_DRIVER_DISCONNECT_FAILED_FAILED','The agent could not disconnect to the driver.']    
    INVALID_STATUS = ['ERROR_INVALID_STATUS','The given argument is not a valid status key.']    
    
    @classmethod
    def is_ok(cls,x):
        """Success test functional synonym. Will need iterable type checking
        if success codes get additional info in the future.

        @param x a str, tuple or list to match to an error code success value.
        @retval True if x is a success value, False otherwise.
        """
        
        try:
            x = cls.get_list_val(x)
            
        except AssertionError:
            return False
        
        return x == cls.OK
    
    @classmethod
    def is_error(cls,x):
        """Generic error test.
        
        @param x a str, tuple or list to match to an error code error value.
        @retval True if x is an error value, False otherwise.
        """
        
        try:
            x = cls.get_list_val(x)
            
        except AssertionError:
            return False
        
        return (cls.has(x) and x != cls.OK)
        
    @classmethod
    def is_equal(cls,val1,val2):
        """Compare error codes.
        
        Used so we are insulated against the framework
        converting error codes to tuples or other iterables.
        
        @param val1 str, tuple or list matching an error code value.
        @param val2 str, tuple or list matching an error code value.
        @retval True if val1 and val2 are equal and defined, False otherwise.
        """

        val1 = cls.get_list_val(val1)
        val2 = cls.get_list_val(val2)
        
        return cls.has(val1) and cls.has(val2) and (val1 == val2)

    @classmethod
    def get_list_val(cls,x):
        """Convert error code values to lists.
        
        The messaging framework can convert lists to tuples. Allow for simple
        strings to be compared also.
        """
        
        assert(isinstance(x,(str,tuple,list))), 'Expected a str, tuple or list \
        error code value.'
        
        # Object is a list, return unmodified.
        if isinstance(x,list):
            return x
        
        # Object is a string, return length 1 list with string as the value.
        elif isinstance(x,str):
            return list((x,))
        
        # Object is a tuple, return a tuple with same elements.
        else:
            return list(x)            

    @classmethod
    def get_string(cls,x):
        """Convert an error code to a printable string"""
        
        x = cls.get_list_val(x)
        if cls.has(x):
            strval = ''
            for item in x:
                strval += str(item) + ', '
            strval = strval[:-2]
            return strval

