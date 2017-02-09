"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/auv_common.py
@author Jeff Roy
@brief Common Parser and Particle Classes and tools for the file auv type
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import ntplib

from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.instrument.dataset_data_particle import DataParticleKey, DataParticleValue

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException, \
    NotImplementedException

EARLIEST_TIMESTAMP = 3471292800.0  # Jan 1, 2010 in NTP float format


def compute_timestamp(parts):
    """
    This method is required as part of the auv_message_map passed to the
    AuvCommonParser constructor
    This version uses mission_epoch and mission time from parts items 1 and 4
    Other instruments may need a different mehtod.

    :param parts: a list of the individual parts of an input record
    :return: a timestamp in the float64 NTP format.
    """
    mission_epoch = parts[1]
    mission_time = parts[4]

    milliseconds = int(mission_time[-3:])/1000.0

    unix_time = float(mission_epoch) + milliseconds
    timestamp = ntplib.system_to_ntp_time(unix_time)

    return timestamp


class AuvCommonParticle(DataParticle):
    """
    Abstract AUV Common Particle Class
    derived classes must provide a parameter map in class variable
    self._auv_param_map.
    This map shall be constructed as a list of tuples.
    Each tuple shall contain 3 values
    (parameter_name, index into raw_data, encoding function)
    """

    _auv_param_map = None  # must be set in derived class constructor

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        if self._auv_param_map is None:
            raise NotImplementedException("_auv_param_map not provided")

        super(AuvCommonParticle, self).__init__(raw_data,
                                                port_timestamp,
                                                internal_timestamp,
                                                preferred_timestamp,
                                                quality_flag,
                                                new_sequence)

    def _build_parsed_values(self):

        parameters = []  # empty list to start

        # loop through the map and append the named parameters
        for name, index, function in self._auv_param_map:
            parameters.append(self._encode_value(name, self.raw_data[index], function))
            # save the epoch and time for the timestamp

        return parameters


class AuvCommonParser(SimpleParser):
    """
    Abstract AUV Common Parser class
    Derived classes must override the _compute_timestamp method
    and provide the message ID and field count to the constructor.
    """

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 auv_message_map):
        """
        @param stream_handle: The stream handle of the file to parse
        @param exception_callback: The callback to use when an exception occurs
        @param auv_message_map is a list of tuples described below.
        Each tuple in the list corresponds with the details of a specific message type to parse
        Each tuple shall contain the following items in this specific order
        message_id: the id found as the first item in a data record
        field_count: the number of data items expected
        compute_timestamp: a method to compute the timestamp
                           this method shall accept a list containing the individual record parts
        particle_class: the class to be created in the call to extract_sample
        """

        self._auv_message_map = auv_message_map
        super(AuvCommonParser, self).__init__({},
                                              stream_handle,
                                              exception_callback)

    def parse_file(self):
        """
        Entry point into parsing the file, loop over each line and interpret it until the entire file is parsed
        """

        for line in self._stream_handle:

            line = line.strip()  # remove the line terminator
            line = line.replace('"', '')  # remove the quote characters from string fields

            for message_id, field_count, compute_timestamp, particle_class in self._auv_message_map:
                # Process records of interest according to map values

                # split it up into parts, limit number of splits because fault messages
                # may contain commas in the last field.
                parts = line.split(',', field_count - 1)

                if parts[0] == message_id:
                    if len(parts) != field_count:
                        msg = 'Expected %d fields but received %d for message id %s' \
                              % (field_count, len(parts), message_id)
                        log.warn(msg)
                        self._exception_callback(RecoverableSampleException(msg))
                    else:
                        try:
                            timestamp = compute_timestamp(parts)
                            if timestamp > EARLIEST_TIMESTAMP:  # Check to make sure the timestamp is OK

                                particle = self._extract_sample(particle_class, None, parts, timestamp)
                                self._record_buffer.append(particle)
                        except Exception:
                            msg = 'Could not compute timestamp'
                            log.warn(msg)
                            self._exception_callback(RecoverableSampleException(msg))

