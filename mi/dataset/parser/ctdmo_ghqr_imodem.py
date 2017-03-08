#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/ctdmo_ghqr_imodem_telemetered_driver.py
@author Maria Lutz, Mark Worden
@brief Parser for the ctdmo_ghqr_imodem dataset driver

Release notes:

Initial release
"""

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import array
import binascii
import struct
import ntplib
import re

from mi.core.common import BaseEnum
from mi.core.log import get_logger
log = get_logger()
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import ConfigurationException, UnexpectedDataException
from mi.dataset.parser.utilities import time_2000_to_ntp_time, \
    formatted_timestamp_utc_time
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, ASCII_HEX_CHAR_REGEX

from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys


class CtdmoGhqrImodemParticleClassKey(BaseEnum):
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'


class CtdmoGhqrImodemDataParticleKey(BaseEnum):
    # For metadata data particle
    DATE_TIME_STRING = 'date_time_string'
    SERIAL_NUMBER = 'serial_number'
    BATTERY_VOLTAGE_MAIN = 'battery_voltage_main'
    BATTERY_VOLTAGE_LITHIUM = 'battery_voltage_lithium'
    SAMPLE_NUMBER = 'sample_number'
    MEM_FREE = 'mem_free'
    SAMPLE_INTERVAL = 'sample_interval'
    PRESSURE_RANGE = 'pressure_range'
    NUM_SAMPLES = 'num_samples'

    # For instrument data particle
    TEMPERATURE = 'temperature'
    CONDUCTIVITY = 'conductivity'
    PRESSURE = 'pressure'
    CTD_TIME = 'ctd_time'

HEADER_BEGIN_REGEX = r'#MCAT Status' + END_OF_LINE_REGEX

FILE_DATETIME_REGEX = r'#7370_DateTime:\s+(?P<' + \
                      CtdmoGhqrImodemDataParticleKey.DATE_TIME_STRING + \
                      '>\d{8}\s+\d{6})' + END_OF_LINE_REGEX

INSTRUMENT_SERIAL_NUM_REGEX = \
    r'#SBE37-IM.*SERIAL NO.\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.SERIAL_NUMBER + \
    '>\d+).*' + END_OF_LINE_REGEX

BATTERY_VOLTAGE_REGEX = \
    r'#vMain\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_MAIN + \
    '>' + FLOAT_REGEX + '),\s+vLith\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_LITHIUM + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

SAMPLE_NUM_MEM_REGEX = \
    r'#samplenumber\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.SAMPLE_NUMBER + \
    '>\d+),\s+free\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.MEM_FREE + \
    '>\d+)' + END_OF_LINE_REGEX

SAMPLE_INTERVAL_REGEX = \
    r'#sample interval\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.SAMPLE_INTERVAL + \
    '>\d+) seconds' + END_OF_LINE_REGEX

PRESSURE_RANGE_REGEX = \
    r'#PressureRange\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.PRESSURE_RANGE + \
    '>\d+)' + END_OF_LINE_REGEX

SAMPLES_RECORDED_REGEX = \
    r'#SamplesRecorded\s+=\s+(?P<' + \
    CtdmoGhqrImodemDataParticleKey.NUM_SAMPLES + \
    '>\d+)' + END_OF_LINE_REGEX


class MetadataMatchKey(BaseEnum):
    FILE_DATETIME_MATCH = 'file_datetime_match'
    INSTRUMENT_SERIAL_NO_MATCH = 'instrument_serial_no_match'
    BATTERY_VOLTAGE_MATCH = 'battery_voltage_match'
    SAMPLE_NUM_MEM_MATCH = 'sample_num_mem_match'
    SAMPLE_INTERVAL_MATCH = 'sample_interval_match'
    PRESSURE_RANGE_MATCH = 'pressure_range_match'
    SAMPLES_RECORDED_MATCH = 'samples_recorded_match'


LOGGING_REGEX = r'#logging.*' + END_OF_LINE_REGEX

DATA_BEGIN_REGEX = r'#Begin Data' + END_OF_LINE_REGEX

DATA_FORMAT_REGEX = r'#Data Format:.+' + END_OF_LINE_REGEX

DATA_END_REGEX = r'#End Data.*'

# each line of format tttttcccccppppTTTTTTTT.
# ttttt: temp
# ccccc: conductivity
# pppp: pressure
# TTTTTTTT: time
INSTRUMENT_DATA_REGEX = \
    b'(?P<' + CtdmoGhqrImodemDataParticleKey.TEMPERATURE + \
    '>' + ASCII_HEX_CHAR_REGEX + '{5})(?P<' +  \
    CtdmoGhqrImodemDataParticleKey.CONDUCTIVITY + \
    '>' + ASCII_HEX_CHAR_REGEX + '{5})(?P<' + \
    CtdmoGhqrImodemDataParticleKey.PRESSURE + \
    '>' + ASCII_HEX_CHAR_REGEX + '{4})(?P<' + \
    CtdmoGhqrImodemDataParticleKey.CTD_TIME + \
    '>' + ASCII_HEX_CHAR_REGEX + '{8})' + END_OF_LINE_REGEX
INSTRUMENT_DATA_MATCHER = re.compile(INSTRUMENT_DATA_REGEX)

# This table is used in the generation of the metadata data particle.
METADATA_ENCODING_RULES = [
    (CtdmoGhqrImodemDataParticleKey.DATE_TIME_STRING, str),
    (CtdmoGhqrImodemDataParticleKey.SERIAL_NUMBER, str),
    (CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_MAIN, float),
    (CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_LITHIUM, float),
    (CtdmoGhqrImodemDataParticleKey.SAMPLE_NUMBER, int),
    (CtdmoGhqrImodemDataParticleKey.MEM_FREE, int),
    (CtdmoGhqrImodemDataParticleKey.SAMPLE_INTERVAL, int),
    (CtdmoGhqrImodemDataParticleKey.PRESSURE_RANGE, int),
    (CtdmoGhqrImodemDataParticleKey.NUM_SAMPLES, int)
]


class DataParticleType(BaseEnum):
    CTDMO_GHQR_IMODEM_INSTRUMENT_RECOVERED = \
        'ctdmo_ghqr_imodem_instrument_recovered'
    CTDMO_GHQR_IMODEM_METADATA_RECOVERED = \
        'ctdmo_ghqr_imodem_metadata_recovered'
    CTDMO_GHQR_IMODEM_INSTRUMENT = \
        'ctdmo_ghqr_imodem_instrument'
    CTDMO_GHQR_IMODEM_METADATA = \
        'ctdmo_ghqr_imodem_metadata'


class CtdmoGhqrImodemInstrumentDataParticle(DataParticle):
    """
    Class for generating the CTDMO instrument particle.
    """

    def _build_parsed_values(self):
        """
        Generate a particle by iterating through the raw_data dictionary
        items.  Convert the data in the manner necessary and return the
        encoded particles.
        """
        result = []

        for key in self.raw_data.keys():
            if key == CtdmoGhqrImodemDataParticleKey.CONDUCTIVITY or \
                    key == CtdmoGhqrImodemDataParticleKey.TEMPERATURE:
                encoded = self._encode_value(key, self.raw_data[key],
                                             lambda x: int('0x' + x, 16))
                result.append(encoded)
            elif key == CtdmoGhqrImodemDataParticleKey.PRESSURE or \
                    key == CtdmoGhqrImodemDataParticleKey.CTD_TIME:

                if key == CtdmoGhqrImodemDataParticleKey.PRESSURE:
                    type_code = 'H'
                else:
                    type_code = 'I'

                # Will first unhexlify into binary and put into an
                # array of longs
                byte_array = array.array(type_code, binascii.unhexlify(
                    self.raw_data[key]))
                # Then swap bytes to get the bytes in the right order.
                # This is called out as necessary in the IDD
                byte_array.byteswap()
                # Then unpack the binary data to get the final value
                (val,) = struct.unpack('>'+type_code, byte_array)
                # Now we will encode it and append it to the result to
                # return
                encoded = self._encode_value(key, val, int)
                result.append(encoded)

                if key == CtdmoGhqrImodemDataParticleKey.CTD_TIME:

                    # Need to use the CTD time for the internal timestamp
                    ctd_time = time_2000_to_ntp_time(val)
                    self.set_internal_timestamp(timestamp=ctd_time)

        return result


class CtdmoGhqrImodemInstrumentTelemeteredDataParticle(
    CtdmoGhqrImodemInstrumentDataParticle):
    _data_particle_type = \
        DataParticleType.CTDMO_GHQR_IMODEM_INSTRUMENT


class CtdmoGhqrImodemInstrumentRecoveredDataParticle(
    CtdmoGhqrImodemInstrumentDataParticle):
    _data_particle_type = \
        DataParticleType.CTDMO_GHQR_IMODEM_INSTRUMENT_RECOVERED


class CtdmoGhqrImodemMetadataDataParticle(DataParticle):
    """
    Class for generating the Metadata particle.
    """
    def _build_parsed_values(self):
        """
        Generate a particle by calling encode_value for each entry
        in the METADATA_ENCODING_RULES list, where each entry is a
        tuple containing the particle field name and a function to
        use for data conversion.
        """
        return [self._encode_value(name,
                                   self.raw_data[name],
                                   encoding_function)
                for name,
                    encoding_function in METADATA_ENCODING_RULES]


class CtdmoGhqrImodemMetadataTelemeteredDataParticle(
    CtdmoGhqrImodemMetadataDataParticle):

    _data_particle_type = \
        DataParticleType.CTDMO_GHQR_IMODEM_METADATA


class CtdmoGhqrImodemMetadataRecoveredDataParticle(
    CtdmoGhqrImodemMetadataDataParticle):

    _data_particle_type = \
        DataParticleType.CTDMO_GHQR_IMODEM_METADATA_RECOVERED


class CtdmoGhqrImodemParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        super(CtdmoGhqrImodemParser, self).__init__(config,
                                                    stream_handle,
                                                    exception_callback)

        try:
            self.instrument_particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    CtdmoGhqrImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
            self.metadata_particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    CtdmoGhqrImodemParticleClassKey.METADATA_PARTICLE_CLASS]
        except:
            raise ConfigurationException

        # Construct the dictionary to save off the metadata record matches
        self._metadata_matches_dict = {
            MetadataMatchKey.FILE_DATETIME_MATCH: None,
            MetadataMatchKey.INSTRUMENT_SERIAL_NO_MATCH: None,
            MetadataMatchKey.BATTERY_VOLTAGE_MATCH: None,
            MetadataMatchKey.SAMPLE_NUM_MEM_MATCH: None,
            MetadataMatchKey.SAMPLE_INTERVAL_MATCH: None,
            MetadataMatchKey.PRESSURE_RANGE_MATCH: None,
            MetadataMatchKey.SAMPLES_RECORDED_MATCH: None,
        }

        self._metadata_sample_generated = False

    def _process_metadata_match_dict(self, key, particle_data):

        group_dict = self._metadata_matches_dict[key].groupdict()

        if key == MetadataMatchKey.FILE_DATETIME_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.DATE_TIME_STRING] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.DATE_TIME_STRING]

        elif key == MetadataMatchKey.INSTRUMENT_SERIAL_NO_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.SERIAL_NUMBER] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.SERIAL_NUMBER]

        elif key == MetadataMatchKey.BATTERY_VOLTAGE_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_MAIN] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_MAIN]
            particle_data[CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_LITHIUM] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.BATTERY_VOLTAGE_LITHIUM]

        elif key == MetadataMatchKey.SAMPLE_NUM_MEM_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.SAMPLE_NUMBER] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.SAMPLE_NUMBER]
            particle_data[CtdmoGhqrImodemDataParticleKey.MEM_FREE] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.MEM_FREE]

        elif key == MetadataMatchKey.SAMPLE_INTERVAL_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.SAMPLE_INTERVAL] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.SAMPLE_INTERVAL]

        elif key == MetadataMatchKey.PRESSURE_RANGE_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.PRESSURE_RANGE] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.PRESSURE_RANGE]

        elif key == MetadataMatchKey.SAMPLES_RECORDED_MATCH:

            particle_data[CtdmoGhqrImodemDataParticleKey.NUM_SAMPLES] = \
                group_dict[CtdmoGhqrImodemDataParticleKey.NUM_SAMPLES]

    def _generate_metadata_particle(self):
        """
        This function generates a metadata particle.
        """

        particle_data = dict()

        for key in self._metadata_matches_dict.keys():

            self._process_metadata_match_dict(key, particle_data)

        utc_time = formatted_timestamp_utc_time(
            particle_data[CtdmoGhqrImodemDataParticleKey.DATE_TIME_STRING],
            "%Y%m%d %H%M%S")
        ntp_time = ntplib.system_to_ntp_time(utc_time)

        # Generate the metadata particle class and add the
        # result to the list of particles to be returned.
        particle = self._extract_sample(self.metadata_particle_class,
                                        None,
                                        particle_data,
                                        ntp_time)
        if particle is not None:
            log.debug("Appending metadata particle to record buffer")
            self._record_buffer.append(particle)

    def _generate_instrument_particle(self, inst_match):
        """
        This method will create an instrument particle given
        instrument match data found from parsing an input file.
        """

        # Extract the instrument particle sample providing the instrument data
        # tuple and ntp timestamp
        particle = self._extract_sample(self.instrument_particle_class,
                                        None,
                                        inst_match.groupdict(),
                                        None)
        if particle is not None:
            log.debug("Appending instrument particle to record buffer")
            self._record_buffer.append(particle)

    def _handle_non_match(self, line):

        # Check for other lines that can be ignored
        if (re.match(LOGGING_REGEX, line) or
                re.match(HEADER_BEGIN_REGEX, line) or
                re.match(DATA_FORMAT_REGEX, line) or
                re.match(DATA_BEGIN_REGEX, line) or
                re.match(DATA_END_REGEX, line)):
            log.debug("Ignoring line: %s", line)

        else:
            # Exception callback
            message = "Unexpected data found.  Line: " + line
            log.warn(message)
            self._exception_callback(UnexpectedDataException(message))

    def _process_line(self, line):

        file_datetime_match = re.match(FILE_DATETIME_REGEX, line)
        instrument_serial_num_match = \
            re.match(INSTRUMENT_SERIAL_NUM_REGEX, line)
        battery_voltage_match = re.match(BATTERY_VOLTAGE_REGEX, line)
        sample_num_mem_match = re.match(SAMPLE_NUM_MEM_REGEX, line)
        sample_interval_match = re.match(SAMPLE_INTERVAL_REGEX, line)
        pressure_range_match = re.match(PRESSURE_RANGE_REGEX, line)
        samples_recorded_match = re.match(SAMPLES_RECORDED_REGEX, line)
        instrument_data_match = re.match(INSTRUMENT_DATA_REGEX, line)

        # Does the line contain data needed for the metadata particle?
        if file_datetime_match:

            self._metadata_matches_dict[MetadataMatchKey.FILE_DATETIME_MATCH] = \
                file_datetime_match

        elif instrument_serial_num_match:
            self._metadata_matches_dict[MetadataMatchKey.INSTRUMENT_SERIAL_NO_MATCH] = \
                instrument_serial_num_match

        elif battery_voltage_match:
            self._metadata_matches_dict[MetadataMatchKey.BATTERY_VOLTAGE_MATCH] = \
                battery_voltage_match

        elif sample_num_mem_match:
            self._metadata_matches_dict[MetadataMatchKey.SAMPLE_NUM_MEM_MATCH] = \
                sample_num_mem_match

        elif sample_interval_match:
            self._metadata_matches_dict[MetadataMatchKey.SAMPLE_INTERVAL_MATCH] = \
                sample_interval_match

        elif pressure_range_match:
            self._metadata_matches_dict[MetadataMatchKey.PRESSURE_RANGE_MATCH] = \
                pressure_range_match

        elif samples_recorded_match:
            self._metadata_matches_dict[MetadataMatchKey.SAMPLES_RECORDED_MATCH] = \
                samples_recorded_match

        # Does the line contain instrument data?
        elif instrument_data_match:

            # create instrument particle
            self._generate_instrument_particle(instrument_data_match)

        else:
            self._handle_non_match(line)

    def parse_file(self):

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:

            self._process_line(line)

            if (None not in self._metadata_matches_dict.values() and
                    not self._metadata_sample_generated):
                # Attempt to generate metadata particles
                self._generate_metadata_particle()
                self._metadata_sample_generated = True

            # read the next line in the file
            line = self._stream_handle.readline()
