#!/usr/bin/env python

"""
@package mi.dataset.parser.adcps_jln_stc
@file marine-integrations/mi/dataset/parser/adcps_jln_stc.py
@author Maria Lutz
@brief Parser for the adcps_jln_stc dataset driver
Release notes:

Initial Release
"""

import re
import ntplib
import struct
import time
import calendar
import datetime as dt

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import SampleException, \
    RecoverableSampleException, \
    UnexpectedDataException, \
    ConfigurationException

from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX

log = get_logger()

__author__ = 'Maria Lutz'
__license__ = 'Apache 2.0'


# *** Defining regexes for this parser ***
HEADER_REGEX = r'#UIMM Status.+DateTime: (\d{8}\s\d{6}).+#ID=(\d+).+#SN=(\d+).+#Volts=(\d+\.\d{2}).+' \
               '#Records=(\d+).+#Length=(\d+).+#Events=(\d+).+#Begin UIMM Data' + END_OF_LINE_REGEX

HEADER_MATCHER = re.compile(HEADER_REGEX, re.DOTALL)

FOOTER_REGEX = r'#End UIMM Data, (\d+) samples written.+'
FOOTER_MATCHER = re.compile(FOOTER_REGEX, re.DOTALL)

HEADER_FOOTER_REGEX = r'#UIMM Status.+DateTime: (\d{8}\s\d{6}).+#ID=(\d+).+#SN=(\d+).+#Volts=(\d+\.\d{2})' \
    '.+#Records=(\d+).+#Length=(\d+).+#Events=(\d+).+'  \
                      '#End UIMM Data, (\d+) samples written.+'
HEADER_FOOTER_MATCHER = re.compile(HEADER_FOOTER_REGEX, re.DOTALL)

DATA_REGEX = r'(Record\[\d+\]:).*?(\x6e\x7f.+?)' + END_OF_LINE_REGEX + '(?=Record|#End U)'
DATA_MATCHER = re.compile(DATA_REGEX, re.DOTALL)

RX_FAILURE_REGEX = r'Record\[\d+\]:ReceiveFailure' + END_OF_LINE_REGEX
RX_FAILURE_MATCHER = re.compile(RX_FAILURE_REGEX)

HEADER_BYTES = 200  # nominal number of bytes at beginning of file to look for Header
FOOTER_BYTES = 43   # Nominal number of bytes from end of file to look for Footer.


class AdcpsJlnStcParticleClassKey (BaseEnum):
    """
    An enum for the keys application to the adcps_jln_stc abc particle classes
    """
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'


class DataParticleType(BaseEnum):
    ADCPS_JLN_INS_TELEMETERED = 'adcps_jln_stc_instrument'
    ADCPS_JLN_META_TELEMETERED = 'adcps_jln_stc_metadata'
    ADCPS_JLN_INS_RECOVERED = 'adcps_jln_stc_instrument_recovered'
    ADCPS_JLN_META_RECOVERED = 'adcps_jln_stc_metadata_recovered'


class AdcpsJlnStcInstrumentParserDataParticleKey(BaseEnum):
    # params collected for adcps_jln_stc_instrument stream:
    ADCPS_JLN_RECORD = 'adcps_jln_record'
    ADCPS_JLN_NUMBER = 'adcps_jln_number'
    ADCPS_JLN_UNIT_ID = 'adcps_jln_unit_id'
    ADCPS_JLN_FW_VERS = 'adcps_jln_fw_vers'
    ADCPS_JLN_FW_REV = 'adcps_jln_fw_rev'
    ADCPS_JLN_YEAR = 'adcps_jln_year'
    ADCPS_JLN_MONTH = 'adcps_jln_month'
    ADCPS_JLN_DAY = 'adcps_jln_day'
    ADCPS_JLN_HOUR = 'adcps_jln_hour'
    ADCPS_JLN_MINUTE = 'adcps_jln_minute'
    ADCPS_JLN_SECOND = 'adcps_jln_second'
    ADCPS_JLN_HSEC = 'adcps_jln_hsec'
    ADCPS_JLN_HEADING = 'adcps_jln_heading'
    ADCPS_JLN_PITCH = 'adcps_jln_pitch'
    ADCPS_JLN_ROLL = 'adcps_jln_roll'
    ADCPS_JLN_TEMP = 'adcps_jln_temp'
    ADCPS_JLN_PRESSURE = 'adcps_jln_pressure'
    ADCPS_JLN_STARTBIN = 'adcps_jln_startbin'
    ADCPS_JLN_BINS = 'adcps_jln_bins'
    ADCPS_JLN_VEL_ERROR = 'error_velocity'
    ADCPS_JLN_VEL_UP = 'water_velocity_up'
    ADCPS_JLN_VEL_NORTH = 'water_velocity_north'
    ADCPS_JLN_VEL_EAST = 'water_velocity_east'


class AdcpsJlnStcInstrumentDataParticle(DataParticle):
    """
    Base class for parsing data from the adcps_jln_stc instrument data set
    """
    ntp_epoch = dt.datetime(1900, 1, 1)

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        try:
            record_str = self.raw_data.group(1).strip('Record\[').strip('\]:')

            fields = struct.unpack_from('<HHIBBBHBBBBBBHhhHIBBB', self.raw_data.group(2))

            # ID field should always be 7F6E
            if fields[0] != int('0x7F6E', 16):
                raise ValueError('ID field does not equal 7F6E.')

            num_bytes = fields[1]
            if len(self.raw_data.group(2)) - 2 != num_bytes:
                raise ValueError('num bytes %d does not match data length %d'
                                 % (num_bytes, len(self.raw_data.group(2)) - 2))

            nbins = fields[20]
            if len(self.raw_data.group(2)) < (36 + (nbins * 8)):
                raise ValueError('Number of bins %d does not fit in data length %d'
                                 % (nbins, len(self.raw_data.group(0))))

            dts = dt.datetime(fields[6],
                              fields[7],
                              fields[8],
                              fields[9],
                              fields[10],
                              fields[11])

            rtc_time = (dts - self.ntp_epoch).total_seconds() + fields[12] / 100.0
            self.set_internal_timestamp(rtc_time)

            velocity_data = struct.unpack_from('<%dh' % (nbins * 4),
                                               self.raw_data.group(2), 34)

            adcps_jln_vel_east = velocity_data[:nbins]
            adcps_jln_vel_north = velocity_data[nbins:nbins*2]
            adcps_jln_vel_up = velocity_data[nbins*2:nbins*3]
            adcps_jln_vel_error = velocity_data[nbins*3:]

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: %r"
                                  % (ex, (self.raw_data.group(0))))

        result = [self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_RECORD, record_str, int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_NUMBER, fields[2], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_UNIT_ID, fields[3], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_FW_VERS, fields[4], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_FW_REV, fields[5], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_YEAR, fields[6], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_MONTH, fields[7], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_DAY, fields[8], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_HOUR, fields[9], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_MINUTE, fields[10], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_SECOND, fields[11], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_HSEC, fields[12], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_HEADING, fields[13], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_PITCH, fields[14], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_ROLL, fields[15], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_TEMP, fields[16], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_PRESSURE, fields[17], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_STARTBIN, fields[19], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_BINS, fields[20], int),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_VEL_ERROR,
                                     adcps_jln_vel_error, list),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_VEL_UP,
                                     adcps_jln_vel_up, list),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_VEL_NORTH,
                                     adcps_jln_vel_north, list),
                  self._encode_value(AdcpsJlnStcInstrumentParserDataParticleKey.ADCPS_JLN_VEL_EAST,
                                     adcps_jln_vel_east, list)]

        return result


class AdcpsJlnStcInstrumentTelemeteredDataParticle(AdcpsJlnStcInstrumentDataParticle):
    """
    Class for parsing data from the adcps_jln_stc instrument telemetered data set
    """
    _data_particle_type = DataParticleType.ADCPS_JLN_INS_TELEMETERED


class AdcpsJlnStcInstrumentRecoveredDataParticle(AdcpsJlnStcInstrumentDataParticle):
    """
    Class for parsing data from the adcps_jln_stc instrument recovered data set
    """
    _data_particle_type = DataParticleType.ADCPS_JLN_INS_RECOVERED


class AdcpsJlnStcMetadataDataParticleKey(BaseEnum):
    # params collected for adcps_jln_stc_metatdata stream:
    ADCPS_JLN_TIMESTAMP = 'adcps_jln_timestamp'
    ADCPS_JLN_ID = 'adcps_jln_id'
    SERIAL_NUMBER = 'serial_number'
    ADCPS_JLN_VOLTS = 'adcps_jln_volts'
    ADCPS_JLN_RECORDS = 'adcps_jln_records'
    ADCPS_JLN_LENGTH = 'adcps_jln_length'
    ADCPS_JLN_EVENTS = 'adcps_jln_events'
    ADCPS_JLN_SAMPLES_WRITTEN = 'adcps_jln_samples_written'


class AdcpsJlnStcMetadataDataParticle(DataParticle):

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        match = HEADER_FOOTER_MATCHER.search(self.raw_data)
        if not match:
            raise RecoverableSampleException("AdcpsJlnStcMetadataParserDataParticle: No regex match of \
                                             parsed sample data [%r]", self.raw_data)

        result = [self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_TIMESTAMP, match.group(1), str),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_ID, match.group(2), int),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.SERIAL_NUMBER, match.group(3), str),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_VOLTS, match.group(4), float),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_RECORDS, match.group(5), int),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_LENGTH, match.group(6), int),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_EVENTS,  match.group(7), int),
                  self._encode_value(AdcpsJlnStcMetadataDataParticleKey.ADCPS_JLN_SAMPLES_WRITTEN, match.group(8), int),
                  ]
        return result


class AdcpsJlnStcMetadataTelemeteredDataParticle(AdcpsJlnStcMetadataDataParticle):

    _data_particle_type = DataParticleType.ADCPS_JLN_META_TELEMETERED


class AdcpsJlnStcMetadataRecoveredDataParticle(AdcpsJlnStcMetadataDataParticle):

    _data_particle_type = DataParticleType.ADCPS_JLN_META_RECOVERED


class AdcpsJlnStcParser(SimpleParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(AdcpsJlnStcParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback)

        try:
            self._metadata_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                AdcpsJlnStcParticleClassKey.METADATA_PARTICLE_CLASS]

            self._instrument_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                AdcpsJlnStcParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
        except KeyError:
            message = "Unable to access adcps jln stc data particle class types in config dictionary"
            log.warn(message)
            raise ConfigurationException(message)

    @staticmethod
    def compare_checksum(raw_bytes):

        rcv_checksum = struct.unpack('<H', raw_bytes[-2:])[0]

        calc_checksum = sum(bytearray(raw_bytes[:-2])) & 0xFFFF

        if rcv_checksum == calc_checksum:
            return True
        return False

    def _parse_header(self):
        """
        Parse required parameters from the header and the footer.
        """

        # read the first bytes from the file
        header = self._stream_handle.read(HEADER_BYTES)
        if len(header) < HEADER_BYTES:
            log.warn("File is not long enough to read header")
            return

        # read the last 43 bytes from the file
        self._stream_handle.seek(-FOOTER_BYTES, 2)
        footer = self._stream_handle.read()
        footer_match = FOOTER_MATCHER.search(footer)

        # parse the header to get the timestamp
        header_match = HEADER_MATCHER.search(header)

        if footer_match and header_match:
            self._stream_handle.seek(len(header_match.group(0)))
            timestamp_struct = time.strptime(header_match.group(1), "%Y%m%d %H%M%S")
            timestamp_s = calendar.timegm(timestamp_struct)
            self._timestamp = float(ntplib.system_to_ntp_time(timestamp_s))

            header_footer = header_match.group(0) + footer_match.group(0)

            particle = self._extract_sample(self._metadata_class, None,
                                            header_footer, self._timestamp)

            self._record_buffer.append(particle)

        else:
            log.warn("File header or footer does not match header regex")

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        self._parse_header()

        input_buffer = self._stream_handle.read()  # read the remainder of the file in a buffer
        position = 0  # initialize buffer index

        while position < len(input_buffer):

            # check if this is a data failure - ignore
            fail_match = RX_FAILURE_MATCHER.match(input_buffer[position:])
            if fail_match:
                position += fail_match.end(0)
                continue

            data_finder_match = DATA_MATCHER.search(input_buffer[position:])
            if data_finder_match is not None:

                if data_finder_match.start(0) != 0:  # must have been garbage between records
                    message = "Found un-expected non-data in byte %r after the header" % input_buffer[position:]
                    log.warn(message)
                    self._exception_callback(UnexpectedDataException(message))

                position += data_finder_match.end(0)
                data_packet = data_finder_match.group(2)  # grab the data portion to validate checksum

                if self.compare_checksum(data_packet):

                    # If this is a valid sensor data record,
                    # use the extracted fields to generate a particle.

                    particle = self._extract_sample(self._instrument_class,
                                                    None,
                                                    data_finder_match,
                                                    None)

                    self._record_buffer.append(particle)
                else:
                    log.warn("Found record whose checksum doesn't match 0x%r", data_finder_match.group(0))
                    self._exception_callback(SampleException("Found record whose checksum doesn't match %r"
                                                             % data_finder_match.group(0)))

            else:
                footer_match = FOOTER_MATCHER.match(input_buffer[position:])
                if footer_match:
                    position += footer_match.end(0)

                else:  # we found some garbage
                    message = "Found un-expected non-data in byte %r after the header" % input_buffer[position:]
                    log.warn(message)
                    self._exception_callback(UnexpectedDataException(message))
                    break  # give up

