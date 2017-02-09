#!/usr/bin/env python

"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/wfp_eng_wfp_sio.py
@author Mark Worden
@brief Parser for the wfp_eng_wfp_sio dataset driver
Release notes:

initial release
"""

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import re
import ntplib
import struct
import binascii

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, UnexpectedDataException
from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER, \
    SIO_TIMESTAMP_NUM_BYTES
from mi.dataset.parser.WFP_E_file_common import WFP_E_GLOBAL_FLAGS_HEADER_REGEX, \
    WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES
from mi.dataset.parser.WFP_E_file_common import STATUS_BYTES_AUGMENTED, \
    STATUS_BYTES, STATUS_START_MATCHER, WFP_TIMESTAMP_BYTES

# The regex for a WFP E data block.
DATA_REGEX = WFP_E_GLOBAL_FLAGS_HEADER_REGEX + '(.*)\x03'
DATA_MATCHER = re.compile(DATA_REGEX, flags=re.DOTALL)


class DataParticleType(BaseEnum):
    START_TIME = 'wfp_eng_wfp_sio_mule_start_time'
    STATUS = 'wfp_eng_wfp_sio_mule_status'
    ENGINEERING = 'wfp_eng_wfp_sio_mule_engineering'


class WfpEngWfpSioParserDataStartTimeParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = 'sio_controller_timestamp'
    WFP_SENSOR_START = 'wfp_sensor_start'
    WFP_PROFILE_START = 'wfp_profile_start'


class WfpEngWfpSioParserDataStatusParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = 'sio_controller_timestamp'
    WFP_INDICATOR = 'wfp_indicator'
    WFP_RAMP_STATUS = 'wfp_ramp_status'
    WFP_PROFILE_STATUS = 'wfp_profile_status'
    WFP_SENSOR_STOP = 'wfp_sensor_stop'
    WFP_PROFILE_STOP = 'wfp_profile_stop'
    WFP_DECIMATION_FACTOR = 'wfp_decimation_factor'


class WfpEngWfpSioParserDataEngineeringParticleKey(BaseEnum):
    WFP_TIMESTAMP = 'wfp_timestamp'
    WFP_PROF_CURRENT = 'wfp_prof_current'
    WFP_PROF_VOLTAGE = 'wfp_prof_voltage'
    WFP_PROF_PRESSURE = 'wfp_prof_pressure'


class WfpEngWfpSioDataParticle(DataParticle):
    """
    A base class for wfp_eng_wfp_sio data particles.
    """

    def _build_result(self, encoding_rules):
        """
        Uses a supplied set of encoding rules to generate a list of
        encoded parameter values.
        """
        result = []

        for particle_key, value, encoding_func in encoding_rules:
            result.append(self._encode_value(particle_key, value, encoding_func))

        return result


class WfpEngWfpSioParserDataStartTimeParticle(WfpEngWfpSioDataParticle):
    """
    Class for building the WfpEngWfpSioParserDataStartTimeParticle
    """

    _data_particle_type = DataParticleType.START_TIME

    def _build_parsed_values(self):
        """
        Takes the raw_data provided on construction and extracts particle data from
        it to create a list of encoded parameter values.
        """

        # Unpack the binary data content that includes:
        # - SIO controller timestamp 8 ASCII char string)
        # - sensor start time unsigned int
        # - profile start time unsigned int
        fields = struct.unpack_from('>8sII', self.raw_data)

        # Unpack the SIO controller timestamp in ASCII to unsigned int
        controller_timestamp = struct.unpack_from('>I', binascii.a2b_hex(fields[0]))[0]

        sensor_start_time = fields[1]
        profile_start_time = fields[2]

        return self._build_result([
            (WfpEngWfpSioParserDataStartTimeParticleKey.CONTROLLER_TIMESTAMP, controller_timestamp, int),
            (WfpEngWfpSioParserDataStartTimeParticleKey.WFP_SENSOR_START, sensor_start_time, int),
            (WfpEngWfpSioParserDataStartTimeParticleKey.WFP_PROFILE_START, profile_start_time, int)])


class WfpEngWfpSioParserDataStatusParticle(WfpEngWfpSioDataParticle):
    """
    Class for building the WfpEngWfpSioParserDataStatusParticle
    """

    _data_particle_type = DataParticleType.STATUS

    def _build_parsed_values(self):
        """
        Takes the raw_data provided on construction and extracts particle data from
        it to create a list of encoded parameter values.
        """

        # Default the optional decimation factor to None
        decimation_factor = None

        # Unpack the binary data content that includes:
        # - SIO controller timestamp 8 ASCII char string)
        # - indicator int
        # - ramp status short
        # - profile status short
        # - profile stop time unsigned int
        # - sensor stop time unsigned int
        # - (optional) decimation factor
        if len(self.raw_data) == SIO_TIMESTAMP_NUM_BYTES+STATUS_BYTES_AUGMENTED:
            # Need to deal with the extra decimation factor
            fields = struct.unpack_from('>8sihhIIH', self.raw_data)

            # The last field is the unsigned int 16 decimation factor
            decimation_factor = fields[6]
        else:
            fields = struct.unpack_from('>8sihhII', self.raw_data)

        # Unpack the SIO controller timestamp in ASCII to unsigned int
        controller_timestamp = struct.unpack_from('>I', binascii.a2b_hex(fields[0]))[0]
        indicator = fields[1]
        ramp_status = fields[2]
        profile_status = fields[3]
        profile_stop_time = fields[4]
        sensor_stop_time = fields[5]

        result = self._build_result([
            (WfpEngWfpSioParserDataStatusParticleKey.CONTROLLER_TIMESTAMP, controller_timestamp, int),
            (WfpEngWfpSioParserDataStatusParticleKey.WFP_INDICATOR, indicator, int),
            (WfpEngWfpSioParserDataStatusParticleKey.WFP_RAMP_STATUS, ramp_status, int),
            (WfpEngWfpSioParserDataStatusParticleKey.WFP_PROFILE_STATUS, profile_status, int),
            (WfpEngWfpSioParserDataStatusParticleKey.WFP_PROFILE_STOP, profile_stop_time, int),
            (WfpEngWfpSioParserDataStatusParticleKey.WFP_SENSOR_STOP, sensor_stop_time, int)])

        result.append({DataParticleKey.VALUE_ID: WfpEngWfpSioParserDataStatusParticleKey.WFP_DECIMATION_FACTOR,
                       DataParticleKey.VALUE: decimation_factor})

        return result


class WfpEngWfpSioParserDataEngineeringParticle(WfpEngWfpSioDataParticle):
    """
    Class for building the WfpEngWfpSioParserDataEngineeringParticle
    """

    _data_particle_type = DataParticleType.ENGINEERING

    def _build_parsed_values(self):
        """
        Takes the raw_data provided on construction and extracts particle data from
        it to create a list of encoded parameter values.
        """

        # Let's first get the 32-bit unsigned int timestamp which should be in the first match group
        fields = struct.unpack_from('>I', self.raw_data)
        wfp_timestamp = fields[0]

        # Now let's grab the global engineering data record match group
        # Should be 3 float 32-bit values
        fields = struct.unpack_from('>fff', self.raw_data[WFP_TIMESTAMP_BYTES:])

        current = fields[0]
        voltage = fields[1]
        pressure = fields[2]

        return self._build_result([
            (WfpEngWfpSioParserDataEngineeringParticleKey.WFP_TIMESTAMP, wfp_timestamp, int),
            (WfpEngWfpSioParserDataEngineeringParticleKey.WFP_PROF_CURRENT, current, float),
            (WfpEngWfpSioParserDataEngineeringParticleKey.WFP_PROF_VOLTAGE, voltage, float),
            (WfpEngWfpSioParserDataEngineeringParticleKey.WFP_PROF_PRESSURE, pressure, float)])


class WfpEngWfpSioParser(SioParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):
        super(WfpEngWfpSioParser, self).__init__(config,
                                                 stream_handle,
                                                 exception_callback)

        self._result_particles = []
        self._current_controller_timestamp = None

    def _process_sensor_profile_start_time_data(self,
                                                sensor_profile_start_time_data):
        """
        This method processes sensor profile start time data attempting to create
         a WfpEngWfpSioParserDataStartTimeParticle from the data.
        """

        # Need to unpack the sensor profile start time timestamp
        fields_prof = struct.unpack_from('>I', sensor_profile_start_time_data[4:])
        timestamp = fields_prof[0]

        sample = self._extract_sample(WfpEngWfpSioParserDataStartTimeParticle, None,
                                      self._current_controller_timestamp +
                                      sensor_profile_start_time_data,
                                      float(ntplib.system_to_ntp_time(timestamp)))

        if sample:
            log.trace("Sample found: %s", sample.generate())
            self._result_particles.append(sample)

    def _process_engineering_data(self, profile_eng_data):
        """
        This method processes profile engineering data attempting to create
         the following particle types along the way:
            WfpEngWfpSioParserDataStatusParticle
            WfpEngWfpSioParserDataEngineeringParticle
        """

        # Start from the end of the chunk and working backwards
        parse_end_point = len(profile_eng_data)

        # We are going to go through the file data in reverse order since we have a
        # variable length sample record that could have a decimation factor.
        # While we do not hit the beginning of the file contents, continue
        while parse_end_point > 0:

            # Create the different start indices for the three different scenarios
            start_index_augmented = parse_end_point-STATUS_BYTES_AUGMENTED
            start_index_normal = parse_end_point-STATUS_BYTES
            global_recovered_eng_rec_index = parse_end_point-WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES

            # Check for an an augmented status first
            if start_index_augmented >= 0 and \
                    STATUS_START_MATCHER.match(profile_eng_data[start_index_augmented:parse_end_point]):
                log.trace("Found OffloadProfileData with decimation factor")

                fields_prof = struct.unpack_from('>I', profile_eng_data[start_index_augmented+8:])
                timestamp = fields_prof[0]

                sample = self._extract_sample(WfpEngWfpSioParserDataStatusParticle, None,
                                              self._current_controller_timestamp +
                                              profile_eng_data[start_index_augmented:parse_end_point],
                                              float(ntplib.system_to_ntp_time(timestamp)))

                # Set the new end point
                parse_end_point = start_index_augmented

            # Check for a normal status
            elif start_index_normal >= 0 and \
                    STATUS_START_MATCHER.match(profile_eng_data[start_index_normal:parse_end_point]):
                log.trace("Found OffloadProfileData without decimation factor")

                fields_prof = struct.unpack_from('>I', profile_eng_data[start_index_normal+8:])
                timestamp = fields_prof[0]

                sample = self._extract_sample(WfpEngWfpSioParserDataStatusParticle, None,
                                              self._current_controller_timestamp +
                                              profile_eng_data[start_index_normal:parse_end_point],
                                              float(ntplib.system_to_ntp_time(timestamp)))

                parse_end_point = start_index_normal

            # If neither, we are dealing with a global wfp_sio e recovered engineering data record,
            # so we will save the start and end points
            elif global_recovered_eng_rec_index >= 0:
                log.trace("Found OffloadEngineeringData")

                fields_prof = struct.unpack_from('>I', profile_eng_data[global_recovered_eng_rec_index:])
                timestamp = fields_prof[0]

                sample = self._extract_sample(WfpEngWfpSioParserDataEngineeringParticle, None,
                                              profile_eng_data[
                                              global_recovered_eng_rec_index:parse_end_point],
                                              float(ntplib.system_to_ntp_time(timestamp)))

                # Set the new end point
                parse_end_point = global_recovered_eng_rec_index

            # We must not have a good file, log some debug info for now
            else:
                log.debug("start_index_augmented %d", start_index_augmented)
                log.debug("start_index_normal %d", start_index_normal)
                log.debug("global_recovered_eng_rec_index %d", global_recovered_eng_rec_index)
                self._exception_callback(SampleException("Data invalid"))

            if sample:
                log.trace("Sample found: %s", sample.generate())
                # create particle
                self._result_particles.append(sample)

    def _process_wfp_eng_chunk(self, chunk):
        """
        This method processes a chunk of data that is expected to match a WFP
        engineering data chunk.
        """

        data_match = DATA_MATCHER.match(chunk)

        if data_match:

            sensor_profile_start_time_data = data_match.group(2)

            self._process_sensor_profile_start_time_data(
                sensor_profile_start_time_data)

            profile_eng_data = data_match.group(3)

            self._process_engineering_data(profile_eng_data)

        else:
            message = "Invalid data found while processing wfp_eng_wfp_sio data file"
            log.warn(message)
            self._exception_callback(UnexpectedDataException(message))

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

        while chunk is not None:

            header_match = SIO_HEADER_MATCHER.match(chunk)

            # Check to see if we are dealing with a wfp_eng SIO chunk
            if header_match.group(1) == 'WE':

                self._current_controller_timestamp = header_match.group(3)

                self._process_wfp_eng_chunk(chunk[len(header_match.group(0)):])

            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        return self._result_particles
