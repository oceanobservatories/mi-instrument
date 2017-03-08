#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_ckl_mmp_cds
@file marine-integrations/mi/dataset/parser/mmp_cds_base.py
@author Mark Worden
@brief Base Parser for the MmpCds dataset drivers
Release notes:

initial release
"""

import msgpack
import ntplib

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import SampleException, NotImplementedException
from mi.dataset.dataset_parser import SimpleParser

log = get_logger()

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

# The number of items in a list associated unpacked data within a McLane Moored Profiler cabled docking station
# data chunk
NUM_MMP_CDS_UNPACKED_ITEMS = 3

# A message to be reported when the state provided to the parser is missing PARTICLES_RETURNED
PARTICLES_RETURNED_MISSING_ERROR_MSG = "PARTICLES_RETURNED missing from state"

# A message to be reported when the mmp cds msgpack data cannot be parsed correctly
UNABLE_TO_PARSE_MSGPACK_DATA_MSG = "Unable to parse msgpack data into expected parameters"

# A message to be reported when unable to iterate through unpacked msgpack data
UNABLE_TO_ITERATE_THROUGH_UNPACKED_MSGPACK_MSG = "Unable to iterate through unpacked msgpack data"

# A message to be reported when the format of the unpacked msgpack data does nto match expected
UNEXPECTED_UNPACKED_MSGPACK_FORMAT_MSG = "Unexpected unpacked msgpack format"


class StateKey(BaseEnum):
    PARTICLES_RETURNED = 'particles_returned'  # holds the number of particles returned


class MmpCdsParserDataParticleKey(BaseEnum):
    RAW_TIME_SECONDS = 'raw_time_seconds'
    RAW_TIME_MICROSECONDS = 'raw_time_microseconds'


class MmpCdsParserDataParticle(DataParticle):
    """
    Class for building a data particle given parsed data as received from a McLane Moored Profiler connected to
    a cabled docking station.
    """

    def _get_mmp_cds_subclass_particle_params(self, subclass_specific_msgpack_unpacked_data):
        """
        This method is expected to be implemented by subclasses.  It is okay to let the implemented method to
        allow the following exceptions to propagate: ValueError, TypeError, IndexError, KeyError
        @param dict_data the dictionary data containing the specific particle parameter name value pairs
        @return a list of particle params specific to the subclass
        """

        # This implementation raises a NotImplementedException to enforce derived classes to implement
        # this method.
        raise NotImplementedException

    def _build_parsed_values(self):
        """
        This method generates a list of particle parameters using the self.raw_data which is expected to be
        a list of three items.  The first item is expected to be the "raw_time_seconds".  The second item
        is expected to be the "raw_time_microseconds".  The third item is an element type specific to the subclass.
        This method depends on an abstract method (_get_mmp_cds_subclass_particle_params) to generate the specific
        particle parameters from the third item element.
        @throws SampleException If there is a problem with sample creation
        """
        try:

            raw_time_seconds = self.raw_data[0]
            raw_time_microseconds = self.raw_data[1]
            raw_time_seconds_encoded = self._encode_value(MmpCdsParserDataParticleKey.RAW_TIME_SECONDS,
                                                          raw_time_seconds, int)
            raw_time_microseconds_encoded = self._encode_value(MmpCdsParserDataParticleKey.RAW_TIME_MICROSECONDS,
                                                               raw_time_microseconds, int)

            ntp_timestamp = ntplib.system_to_ntp_time(raw_time_seconds + raw_time_microseconds/1000000.0)

            log.debug("Calculated timestamp from raw %.10f", ntp_timestamp)

            self.set_internal_timestamp(ntp_timestamp)

            subclass_particle_params = self._get_mmp_cds_subclass_particle_params(self.raw_data[2])

        except (ValueError, TypeError, IndexError, KeyError) as ex:
            log.warn(UNABLE_TO_PARSE_MSGPACK_DATA_MSG)
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        result = [raw_time_seconds_encoded,
                  raw_time_microseconds_encoded] + subclass_particle_params

        log.debug('MmpCdsParserDataParticle: particle=%s', result)
        return result


class MmpCdsParser(SimpleParser):
    """
    Class for parsing data as received from a McLane Moored Profiler connected to a cabled docking station.
    """

    def parse_file(self):
        """
        This method parses each chunk and attempts to extract samples to return.
        @return for each discovered sample, a list of tuples containing each particle and associated state position
        # information
        """

        # We need to put the following in a try block just in case the data provided is malformed
        try:
            # Let's iterate through each unpacked list item
            for unpacked_data in msgpack.Unpacker(self._stream_handle):

                # The expectation is that an unpacked list item associated with a McLane Moored Profiler cabled
                # docking station data chunk consists of a list of three items
                if isinstance(unpacked_data, tuple) or isinstance(unpacked_data, list) and \
                        len(unpacked_data) == NUM_MMP_CDS_UNPACKED_ITEMS:

                    # Extract the sample an provide the particle class which could be different for each
                    # derived MmpCdsParser

                    try:
                        data_particle = self._extract_sample(self._particle_class, None, unpacked_data, None)
                        self._record_buffer.append(data_particle)
                    except SampleException:
                        log.debug(UNEXPECTED_UNPACKED_MSGPACK_FORMAT_MSG)
                        self._exception_callback(SampleException(UNEXPECTED_UNPACKED_MSGPACK_FORMAT_MSG))

                else:
                    log.debug(UNEXPECTED_UNPACKED_MSGPACK_FORMAT_MSG)
                    self._exception_callback(SampleException(UNEXPECTED_UNPACKED_MSGPACK_FORMAT_MSG))

        except TypeError:
            log.warn(UNABLE_TO_ITERATE_THROUGH_UNPACKED_MSGPACK_MSG)
            self._exception_callback( SampleException(UNABLE_TO_ITERATE_THROUGH_UNPACKED_MSGPACK_MSG))
