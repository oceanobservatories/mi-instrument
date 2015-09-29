"""
@package mi.instrument.wetlabs.ac_s.ooicore.driver
@file marine-integrations/mi/instrument/wetlabs/ac_s/ooicore/driver.py
@author Rachel Manoni
@brief Driver for the ooicore
Release notes:

initial version
"""
__author__ = 'Rachel Manoni'
__license__ = 'Apache 2.0'

from _ctypes import sizeof
from collections import OrderedDict
from ctypes import BigEndianStructure, c_ushort, c_uint, c_ubyte
from io import BytesIO
import struct
from datetime import datetime
import os
import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.driver_dict import DriverDictKey

NEWLINE = '\n'

INDEX_OF_PACKET_RECORD_LENGTH = 4
INDEX_OF_START_OF_SCAN_DATA = 32
SIZE_OF_PACKET_RECORD_LENGTH = 2
SIZE_OF_SCAN_DATA_SIGNAL_COUNTS = 2
SIZE_OF_CHECKSUM_PLUS_PAD = 3   # three bytes for 2 byte checksum and 1 byte pad

PACKET_REGISTRATION_PATTERN = '\xff\x00\xff\x00'
PACKET_REGISTRATION_REGEX = re.compile(PACKET_REGISTRATION_PATTERN)

STATUS_PATTERN = r'AC-Spectra .+? quit\.'
STATUS_REGEX = re.compile(STATUS_PATTERN, re.DOTALL)

# Regexes for status particles
FLOAT_PATTERN = r'\d+\.\d+'
FLOAT_REGEX = re.compile(FLOAT_PATTERN)

DATE_PATTERN = r'\([A-Za-z]+\s+\d+\s+\d{4}\s+\d+:\d+:\d+\)'
DATE_REGEX = re.compile(DATE_PATTERN)

PERSISTOR_PATTERN = r'Persistor CF2 SN:\d+'
PERSISTOR_REGEX = re.compile(PERSISTOR_PATTERN)


####
#    Driver Constant Definitions
####
class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    OPTAA_SAMPLE = 'optaa_sample'
    OPTAA_STATUS = 'optaa_status'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    DISCOVER = DriverEvent.DISCOVER


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    DISCOVER = DriverEvent.DISCOVER


class Prompt(BaseEnum):
    """
    Device i/o prompts.
    """


###############################################################################
# Data Particles
###############################################################################
class OptaaSampleDataParticleKey(BaseEnum):
    RECORD_LENGTH = 'record_length'
    PACKET_TYPE = 'packet_type'
    METER_TYPE = 'meter_type'
    SERIAL_NUMBER = 'serial_number'
    A_REFERENCE_DARK_COUNTS = 'a_reference_dark_counts'
    PRESSURE_COUNTS = 'pressure_counts'
    A_SIGNAL_DARK_COUNTS = 'a_signal_dark_counts'
    EXTERNAL_TEMP_RAW = 'external_temp_raw'
    INTERNAL_TEMP_RAW = 'internal_temp_raw'
    C_REFERENCE_DARK_COUNTS = 'c_reference_dark_counts'
    C_SIGNAL_DARK_COUNTS = 'c_signal_dark_counts'
    ELAPSED_RUN_TIME = 'elapsed_run_time'
    NUM_WAVELENGTHS = 'num_wavelengths'
    C_REFERENCE_COUNTS = 'c_reference_counts'
    A_REFERENCE_COUNTS = 'a_reference_counts'
    C_SIGNAL_COUNTS = 'c_signal_counts'
    A_SIGNAL_COUNTS = 'a_signal_counts'


class OptaaSampleHeader(BigEndianStructure):
    _fields_ = [
        ('packet_registration', c_uint),
        ('record_length', c_ushort),
        ('packet_type', c_ubyte),
        ('reserved', c_ubyte),
        ('meter_type', c_ubyte),
        ('serial_number_high', c_ubyte),
        ('serial_number_low', c_ushort),
        ('a_ref_dark_counts', c_ushort),
        ('pressure_counts', c_ushort),
        ('a_signal_dark_counts', c_ushort),
        ('raw_external_temp_counts', c_ushort),
        ('raw_internal_temp_counts', c_ushort),
        ('c_ref_dark_counts', c_ushort),
        ('c_signal_dark_counts', c_ushort),
        ('time_high', c_ushort),
        ('time_low', c_ushort),
        ('reserved2', c_ubyte),
        ('num_wavelengths', c_ubyte),
    ]

    @staticmethod
    def from_string(input_str):
        header = OptaaSampleHeader()
        BytesIO(input_str).readinto(header)
        return header

    def __str__(self):
        d = OrderedDict()
        for name, _ in self._fields_:
            d[name] = getattr(self, name)
        return str(d)


class OptaaSampleDataParticle(DataParticle):
    _data_particle_type = DataParticleType.OPTAA_SAMPLE

    def __init__(self, *args, **kwargs):
        super(OptaaSampleDataParticle, self).__init__(*args, **kwargs)
        # for playback, we want to obtain the elapsed run time prior to generating
        # the particle, so we'll go ahead and parse the header on object creation
        self.header = OptaaSampleHeader.from_string(self.raw_data)
        self.data = struct.unpack_from('>%dH' % (self.header.num_wavelengths*4), self.raw_data, sizeof(self.header))
        self.elapsed = (self.header.time_high << 16) + self.header.time_low

    def _build_parsed_values(self):

        cref = self.data[::4]
        aref = self.data[1::4]
        csig = self.data[2::4]
        asig = self.data[3::4]

        key = OptaaSampleDataParticleKey
        header = self.header
        identity = lambda x: x

        serial_number = (header.serial_number_high << 16) + header.serial_number_low

        result = [
            self._encode_value(key.RECORD_LENGTH, header.record_length, identity),
            self._encode_value(key.PACKET_TYPE, header.packet_type, identity),
            self._encode_value(key.METER_TYPE, header.meter_type, identity),
            self._encode_value(key.SERIAL_NUMBER, serial_number, str),
            self._encode_value(key.A_REFERENCE_DARK_COUNTS, header.a_ref_dark_counts, identity),
            self._encode_value(key.PRESSURE_COUNTS, header.pressure_counts, identity),
            self._encode_value(key.A_SIGNAL_DARK_COUNTS, header.a_signal_dark_counts, identity),
            self._encode_value(key.EXTERNAL_TEMP_RAW, header.raw_external_temp_counts, identity),
            self._encode_value(key.INTERNAL_TEMP_RAW, header.raw_internal_temp_counts, identity),
            self._encode_value(key.C_REFERENCE_DARK_COUNTS, header.c_ref_dark_counts, identity),
            self._encode_value(key.C_SIGNAL_DARK_COUNTS, header.c_signal_dark_counts, identity),
            self._encode_value(key.ELAPSED_RUN_TIME, self.elapsed, identity),
            self._encode_value(key.NUM_WAVELENGTHS, header.num_wavelengths, identity),
            self._encode_value(key.C_REFERENCE_COUNTS, cref, list),
            self._encode_value(key.A_REFERENCE_COUNTS, aref, list),
            self._encode_value(key.C_SIGNAL_COUNTS, csig, list),
            self._encode_value(key.A_SIGNAL_COUNTS, asig, list),
            ]

        log.debug("raw data = %r", self.raw_data)
        log.debug('parsed particle = %r', result)

        return result


class OptaaStatusDataParticleKey(BaseEnum):
    FIRMWARE_VERSION = 'firmware_version'
    FIRMWARE_DATE = 'firmware_date'
    PERSISTOR_CF_SERIAL_NUMBER = 'persistor_cf_serial_number'
    PERSISTOR_CF_BIOS_VERSION = 'persistor_cf_bios_version'
    PERSISTOR_CF_PICODOS_VERSION = 'persistor_cf_picodos_version'


class OptaaStatusDataParticle(DataParticle):
    _data_particle_type = DataParticleType.OPTAA_STATUS

    def _build_parsed_values(self):

        data_stream = self.raw_data

        # This regex searching can be made a lot more specific, but at the expense of
        # more code. For now, grabbing all three floating point numbers in one sweep is
        # pretty efficient. Note, however, that if the manufacturer ever changes the
        # format of the status display, this code may have to be re-written.
        fp_results = re.findall(FLOAT_REGEX, data_stream)
        if len(fp_results) == 3:
            version = fp_results[0]
            bios = fp_results[1]
            picodos = fp_results[2]
        else:
            raise SampleException('Unable to find exactly three floating-point numbers in status message.')

        # find the date/time string and remove enclosing parens
        m = re.search(DATE_REGEX, data_stream)
        if m is not None:
                p = m.group()
                date_of_version = p[1:-1]
        else:
                date_of_version = 'None found'

        persistor = re.search(PERSISTOR_REGEX, data_stream)
        if persistor is not None:
            temp = persistor.group()
            temp1 = re.search(r'\d{2,10}', temp)
            if temp1 is not None:
                persistor_sn = temp1.group()
            else:
                persistor_sn = 'None found'
        else:
            persistor_sn = 'None found'

        result = [{DataParticleKey.VALUE_ID: OptaaStatusDataParticleKey.FIRMWARE_VERSION,
                   DataParticleKey.VALUE: str(version)},
                  {DataParticleKey.VALUE_ID: OptaaStatusDataParticleKey.FIRMWARE_DATE,
                  DataParticleKey.VALUE: date_of_version},
                  {DataParticleKey.VALUE_ID: OptaaStatusDataParticleKey.PERSISTOR_CF_SERIAL_NUMBER,
                   DataParticleKey.VALUE: str(persistor_sn)},
                  {DataParticleKey.VALUE_ID: OptaaStatusDataParticleKey.PERSISTOR_CF_BIOS_VERSION,
                   DataParticleKey.VALUE: str(bios)},
                  {DataParticleKey.VALUE_ID: OptaaStatusDataParticleKey.PERSISTOR_CF_PICODOS_VERSION,
                   DataParticleKey.VALUE: str(picodos)}]

        log.debug("raw data = %r", self.raw_data)
        log.debug('parsed particle = %r', result)

        return result


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return DriverParameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent, ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)

        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT, self._handler_autosample_exit)

        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._chunker = StringChunker(Protocol.sieve_function)

        self._build_driver_dict()
        self._cmd_dict.add(Capability.DISCOVER, display_name='Discover')

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples and status
        """
        raw_data_len = len(raw_data)
        return_list = []

        # look for samples
        # OPTAA record looks like this:
        # ff00ff00  <- packet registration
        # 02d0      <- record length minus checksum
        # ...       <- data
        # 2244      <- checksum
        # 00        <- pad
        for match in PACKET_REGISTRATION_REGEX.finditer(raw_data):
            # make sure I have at least 6 bytes (packet registration plus 2 bytes for record length)
            start = match.start()
            if (start+6) <= raw_data_len:
                packet_length = struct.unpack_from('>H', raw_data, start+4)[0]
                # make sure we have enough data to construct a whole packet
                if (start+packet_length+SIZE_OF_CHECKSUM_PLUS_PAD) <= raw_data_len:
                    # validate the checksum, if valid add to the return list
                    checksum = struct.unpack_from('>H', raw_data, start+packet_length)[0]
                    calulated_checksum = sum(bytearray(raw_data[start:start+packet_length])) & 0xffff
                    if checksum == calulated_checksum:
                        return_list.append((match.start(), match.start() + packet_length + SIZE_OF_CHECKSUM_PLUS_PAD))

        # look for status
        for match in STATUS_REGEX.finditer(raw_data):
            return_list.append((match.start(), match.end()))

        return return_list

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        self._extract_sample(OptaaSampleDataParticle, PACKET_REGISTRATION_REGEX, chunk, timestamp)
        self._extract_sample(OptaaStatusDataParticle, STATUS_REGEX, chunk, timestamp)

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _build_driver_dict(self):
        """
        Populate the driver dictionary with options
        """
        self._driver_dict.add(DriverDictKey.VENDOR_SW_COMPATIBLE, True)

    ########################################################################
    # Unknown handlers.
    ########################################################################
    def _handler_unknown_enter(self, *args, **kwargs):
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_unknown_exit(self, *args, **kwargs):
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Discover current state; can only be AUTOSAMPLE (instrument has no actual command mode).
        """
        return ProtocolState.AUTOSAMPLE, ResourceAgentState.STREAMING

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_enter(self, *args, **kwargs):
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_autosample_exit(self, *args, **kwargs):
        pass


class PlaybackProtocol(Protocol):
    def __init__(self, driver_event):
        super(PlaybackProtocol, self).__init__(None, None, driver_event)
        self.offset_timestamp = None
        self.offset = 0

    def got_filename(self, filename):
        filename = os.path.basename(filename)
        date_time_regex = re.compile(r'(\d{8}T\d{4}_UTC)')
        date_format = '%Y%m%dT%H%M_%Z'
        dt = datetime.strptime(date_time_regex.search(filename).group(1), date_format)
        self.offset_timestamp = (dt - datetime(1900, 1, 1)).total_seconds()

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        self._extract_sample(OptaaSampleDataParticle, PACKET_REGISTRATION_REGEX, chunk, timestamp)
        self._extract_sample(OptaaStatusDataParticle, STATUS_REGEX, chunk, timestamp)

    def _extract_sample(self, particle_class, regex, line, timestamp, publish=True):
        if regex.match(line):
            particle = particle_class(line, port_timestamp=timestamp)

            if hasattr(particle, 'elapsed'):
                if self.offset_timestamp is not None:
                    self.offset = self.offset_timestamp - particle.elapsed
                    self.offset_timestamp = None

                new_timestamp = particle.elapsed + self.offset
                particle.set_internal_timestamp(new_timestamp)

            parsed_sample = particle.generate()

            if publish and self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, parsed_sample)

            return parsed_sample


def create_playback_protocol(callback):
    return PlaybackProtocol(callback)