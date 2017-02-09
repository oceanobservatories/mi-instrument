#!/usr/bin/env python

"""
@file coi-services/mi/idk/result_set.py
@author Bill French
@brief Read a result set file and use the data to verify
data particles.

Usage:

from mi.core.log import log

rs = ResultSet(result_set_file_path)
if not rs.verify(particles):
    log.info("Particle verified")
else:
    log.error("Particle validate failed")
    log.error(rs.report())

Result Set File Format:
  result files are yml formatted files with a header and data section.
  the data is stored in record elements with the key being the parameter name.
     - two special fields are internal_timestamp and _index.
     - internal timestamp can be input in text string or ntp float format

eg.

# Result data for verifying particles. Comments are ignored.

header:
  particle_object: CtdpfParserDataParticleKey
  particle_type: ctdpf_parsed

data:
  -  _index: 1
     internal_timestamp: 07/26/2013 21:01:03
     temperature: 4.1870
     conductivity: 10.5914
     pressure: 161.06
     oxygen: 2693.0
  -  _index: 2
     internal_timestamp: 07/26/2013 21:01:04
     temperature: 4.1872
     conductivity: 10.5414
     pressure: 161.16
     oxygen: 2693.1

If a driver returns multiple particle types, the particle type must be specified in each particle

header:
  particle_object: 'MULTIPLE'
  particle_type: 'MULTIPLE'

data:
  -  _index: 1
     particle_object: CtdpfParser1DataParticleKey
     particle_type: ctdpf_parsed_1
     internal_timestamp: 07/26/2013 21:01:03
     temperature: 4.1870
     conductivity: 10.5914
     pressure: 161.06
     oxygen: 2693.0
  -  _index: 2
     particle_object: CtdpfParser2DataParticleKey
     particle_type: ctdpf_parsed_2
     internal_timestamp: 07/26/2013 21:01:04
     temperature: 4.1872
     conductivity: 10.5414
     pressure: 161.16
     oxygen: 2693.1

"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import json
import yaml
import numpy

from mi.core.time import string_to_ntp_date_time
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey

from mi.core.log import get_logger
log = get_logger()

FLOAT_ALLOWED_DIFF = .00001

OBJECT_KEY = 'particle_object'
TYPE_KEY = 'particle_type'
MULTIPLE = 'MULTIPLE'
INDEX = '_index'


class ResultSet(object):
    """
    Result Set object
    Read result set files and compare to parsed particles.
    """
    def __init__(self, result_file_path):
        """
        Read in the yml file and error check to confirm it is formatted as expected, storing file header and data
        :param result_file_path: The file path of the .yml file
        """
        log.debug("read result file: %s" % result_file_path)
        stream = file(result_file_path, 'r')

        if result_file_path.endswith('.yml') or result_file_path.endswith('.yaml'):
            result_set = yaml.load(stream)
        elif result_file_path.endswith('json'):
            result_set = json.load(stream)
        else:
            result_set = {}

        # confirm the yml has a 'header' section
        self._result_set_header = result_set.get("header")
        if not self._result_set_header:
            ResultSet.log_and_raise_ioerror("Missing result set header")

        # check for required particle object and particle type fields, if not present raise error
        if self._result_set_header.get(OBJECT_KEY) is None:
            ResultSet.log_and_raise_ioerror("header particle_object not defined")

        if self._result_set_header.get(TYPE_KEY) is None:
            ResultSet.log_and_raise_ioerror("header particle_type not defined")

        # confirm the yml has a 'data' section
        self._result_set_data = {}
        data = result_set.get("data")
        if not data:
            ResultSet.log_and_raise_ioerror("Missing 'data' section from yml file")

        # confirm the particles have been formed correctly within data, with a unique _index
        for particle in data:
            if isinstance(particle, dict):
                index = particle.get(INDEX)
                if index is None:
                    ResultSet.log_and_raise_ioerror("Particle definition missing _index: %s" % particle)

                if self._result_set_data.get(index) is not None:
                    ResultSet.log_and_raise_ioerror("Duplicate particle definition for _index %s: %s" %
                                                    (index, particle))

            else:
                ResultSet.log_and_raise_ioerror("Yml not formatted properly to make a particle dictionary")

            # store the particle by its index for comparison
            self._result_set_data[index] = particle

    @staticmethod
    def log_and_raise_ioerror(message):
        """
        Log an error message and raise an IOError with the same message
        :param message: The message to log and raise the error with
        """
        log.error(message)
        raise IOError(message)

    def verify(self, particles):
        """
        Verify particles passed in against result set read in the constructor.
        Verify particles as a set and individual particle data

        :param particles: list of particles to verify.
        :return True if verification successful, False if unsuccessful
        """

        # verify the header, then if that is okay verify each data particle
        result = self._verify_header(particles)
        if result:
            result = self._verify_particles(particles)

        return result

    def _verify_header(self, particles):
        """
        Verify the particles as a set match what we expect.
        - All particles are of the expected type
        - Check particle count
        :param particles: All particles
        """

        if len(self._result_set_data) != len(particles):
            log.debug("result set records != particles to verify (%d != %d)" %
                      (len(self._result_set_data), len(particles)))
            return False

        # if this driver returns multiple particle classes, type checking happens
        # for each particle in _get_particle_data_errors
        if self._result_set_header.get(OBJECT_KEY) != MULTIPLE and self._result_set_header.get(TYPE_KEY) != MULTIPLE:

            for particle in particles:

                # compare the particle class
                if not ResultSet._are_classes_equal(particle, self._result_set_header.get(OBJECT_KEY)):
                    return False

                # compare the particle stream
                if not ResultSet._are_streams_equal(particle, self._result_set_header.get(TYPE_KEY)):
                    return False

        return True

    def _verify_particles(self, particles):
        """
        Verify data in the particles individually.
        - Verify order based on _index
        - Verify parameter data values
        - Verify there are extra or missing parameters
        :param particles: All particles to compare
        """
        result = True
        index = 1
        for particle in particles:
            # find any errors in comparing this particle to the yml at this index
            if not self._get_particle_data_errors(particle, self._result_set_data.get(index)):
                result = False

            index += 1

        return result

    @staticmethod
    def _are_classes_equal(particle, expected_object):
        """
        Verify that the object is a DataParticle and is the correct class.
        :param particle: The received particle
        :param expected_object: The expected object class
        :returns: Error message indicating class error if there is one, None if there are no errors
        """
        # if particle is in dictionary form, can't get its class
        if not isinstance(particle, dict):
            # check that the class is a DataParticle subclass
            if not issubclass(particle.__class__, DataParticle):
                log.error("particle class is not a subclass of DataParticle")
                return False

            if expected_object != particle.__class__.__name__:
                log.error("class mismatch: %s != %s" % (expected_object, particle.__class__.__name__))
                return False

        return True

    @staticmethod
    def _are_streams_equal(particle, expected_stream):
        """
        Verify the particle stream type
        :param particle: Received particle
        :param expected_stream: Expected stream type from yml
        :return: Error message indicating stream mismatch if streams don't match, None if they match
        """
        particle_dict = ResultSet._particle_as_dict(particle)
        received_stream = particle_dict[DataParticleKey.STREAM_NAME]
        if received_stream != expected_stream:
            log.error("Stream type mismatch: %s != %s" % (received_stream, expected_stream))
            return False

        return True

    @staticmethod
    def _are_timestamps_equal(received_ts, expected_ts):
        """
        Compare the timestamps
        :param received_ts: received timestamp
        :param expected_ts: expected timestamp from yml (can be string or ntp float)
        :return: If the timestamps are the same, None is returned, otherwise an error message string is
        """
        # Verify the timestamp, required to be in the particle
        if received_ts and expected_ts:
            # got timestamp in yml and received particle
            if isinstance(expected_ts, str):
                expected = string_to_ntp_date_time(expected_ts)
            else:
                # if not a string, timestamp should already be in ntp
                expected = expected_ts

            if abs(received_ts - expected) > FLOAT_ALLOWED_DIFF:
                log.error("expected internal_timestamp mismatch, %.9f != %.9f" % (expected, received_ts))
                return False

        elif expected_ts and not received_ts:
            log.error("expected internal_timestamp, missing from received particle")
            return False

        elif received_ts and not expected_ts:
            log.error("internal_timestamp was received but is missing from .yml")
            return False

        return True

    def _get_particle_data_errors(self, particle_received, particle_expected):
        """
        Verify that all data parameters are present and have the
        expected value
        :param: particle_received
        :param: particle_expected
        :returns: List of error strings
        """
        received_dict = ResultSet._particle_as_dict(particle_received)
        log.debug("Particle to test: %s", received_dict)
        log.debug("Particle definition: %s", particle_expected)

        # compare internal timestamps
        if not ResultSet._are_timestamps_equal(received_dict.get(DataParticleKey.INTERNAL_TIMESTAMP),
                                               particle_expected.get(DataParticleKey.INTERNAL_TIMESTAMP)):
            return False

        # particle object (class) and particle type (stream) keys will only be present for drivers
        # returning multiple particle types
        if self._result_set_header.get(OBJECT_KEY) == MULTIPLE:
            # confirm that the class and stream are defined within this particle
            if OBJECT_KEY not in particle_expected or TYPE_KEY not in particle_expected:
                log.error('Particle object and type not present in each particle although MULTIPLE in header')
                return False

            # check if the stream and class match for this particle
            if not ResultSet._are_classes_equal(particle_received, particle_expected.get(OBJECT_KEY)):
                return False

            if not ResultSet._are_streams_equal(particle_received, particle_expected.get(TYPE_KEY, None)):
                return False

        expected_keys = particle_expected.keys()
        ignore_list = [INDEX, DataParticleKey.NEW_SEQUENCE,
                       DataParticleKey.INTERNAL_TIMESTAMP,
                       DataParticleKey.PORT_TIMESTAMP,
                       OBJECT_KEY, TYPE_KEY]
        # remove keys in ignore list from expected_keys, these are specifically ignored or were already handled
        expected_keys = filter(lambda x: x not in ignore_list, expected_keys)

        # the received dictionary contains an array of dictionaries {value_id: KEY, value: VALUE}
        # reformat into a list of value keys
        received_keys = map(lambda x: x[DataParticleKey.VALUE_ID], received_dict[DataParticleKey.VALUES])
        # transform dictionary into a list of tuples, with each tuple containing the key and value pair,
        # i.e. [(key, value), (key, value)]
        received_value_map = map(lambda x: (x[DataParticleKey.VALUE_ID], x[DataParticleKey.VALUE]),
                                 received_dict[DataParticleKey.VALUES])

        # confirm the two particles have the same set of value keys
        if sorted(expected_keys) != sorted(received_keys):
            log.error("expected / particle keys mismatch: %s != %s" %
                          (sorted(expected_keys), sorted(received_keys)))
            return False

        # loop over each key and compare values
        all_match = True
        for item in received_value_map:
            key = item[0]
            log.debug("Verify value for '%s'", key)
            if not ResultSet._are_values_equal(key, particle_expected[key], item[1]):
                all_match = False

        return all_match

    @staticmethod
    def _are_values_equal(key, expected_value, particle_value):
        """
        Verify a value matches what we expect.  If the expected value (from the yaml)
        is a dict then we expect the value to be in a 'value' field.  Otherwise just
        use the parameter as a raw value.

        when passing a dict you can specify a 'round' factor.
        :param expected_value - the expected particle value
        :param particle_value - the received particle value
        :returns: None if the values match, an error message if they don't match
        """

        local_expected_value = expected_value

        # Let's first check to see if we have a None for an expected value
        if local_expected_value is None:
            log.debug("%s no value to compare, ignoring", key)
            return True

        if isinstance(expected_value, dict):
            # if the yml dictionary specifically requests rounding, round the value
            local_expected_value = ResultSet._perform_round(expected_value[DataParticleKey.VALUE],
                                                            expected_value.get('round'))

        # turn values into a numpy array, allows for easier comparison of single, lists, and nested lists of floats
        expected_array = numpy.array(local_expected_value)
        received_array = numpy.array(particle_value)

        # check for floats
        if expected_array.dtype.name.find('float') != -1 and received_array.dtype.name.find('float') != -1:
            # both are float type, this will catch single floats, lists of floats, and nested lists of floats

            # find where there are nans (returns array of True / False if it is a nan or not)
            isnan_expected = numpy.isnan(expected_array)
            isnan_received = numpy.isnan(received_array)

            # check that nans occur in the same locations
            if numpy.any(isnan_expected != isnan_received):
                log.error("%s value mismatch, %s != %s", key, local_expected_value, particle_value)
                return False

            # check if any nans are present in either
            if numpy.any(isnan_expected) or numpy.any(isnan_received):
                # remove nans, only compare non-nan values
                expected_array = expected_array[numpy.logical_not(isnan_expected)]
                received_array = received_array[numpy.logical_not(isnan_received)]

            # compare floats with a tolerance
            if numpy.any(numpy.abs(expected_array - received_array) > FLOAT_ALLOWED_DIFF):
                log.error("%s value mismatch, %s != %s", key, local_expected_value, particle_value)
                return False

        # not a float, do simple compare
        elif local_expected_value != particle_value:
            log.error("%s value mismatch, %s != %s", key, local_expected_value, particle_value)
            return False

        return True

    @staticmethod
    def _particle_as_dict(particle):
        """
        If the particle is an object, generate the particle dictionary and return it
        :returns: The particle as a dictionary
        """
        if isinstance(particle, dict):
            return particle

        return json.loads(particle.generate())

    @staticmethod
    def _perform_round(value, round_factor):
        """
        Round a value or list of values
        :param value:
        :param round_factor:
        :return:
        """

        if type(value) is float:
            return round(value, round_factor)
        elif type(value) is list:
            return [ResultSet._perform_round(x, round_factor) for x in value]
        else:
            return value
