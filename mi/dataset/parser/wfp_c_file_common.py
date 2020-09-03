#!/usr/bin/env python

"""
@package mi.dataset.parser.wfp_c_file_common
@file marine-integrations/mi/dataset/parser/wfp_c_file_common.py
@author Emily Hahn
@brief Parser for the c file type for the wfp
Release notes:

initial release
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import copy
import re
import time
import ntplib
import struct
import binascii

from mi.core.log import get_logger ; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, DatasetParserException
from mi.core.log import get_logger

from mi.dataset.dataset_parser import BufferLoadingParser, DataSetDriverConfigKeys

from ion_functions.data.ctd_functions import ctd_sbe52mp_preswat

log = get_logger()

EOP_ONLY_MATCHER = re.compile(r'\xFF{11}')
EOP_REGEX = r'.*(\xFF{11})(.{8})'
EOP_MATCHER = re.compile(EOP_REGEX, re.DOTALL)

DATA_RECORD_BYTES = 11
TIME_RECORD_BYTES = 8
FOOTER_BYTES = DATA_RECORD_BYTES + TIME_RECORD_BYTES

DATA_PARTICLE_CLASS_KEY = 'instrument_data_particle_class'

class WfpCFileCommonConfigKeys(BaseEnum):
    PRESSURE_FIELD_C_FILE = "pressure_field_c_file"
    PRESSURE_FIELD_E_FILE = "pressure_field_e_file"

class StateKey(BaseEnum):
    POSITION = 'position' # holds the file position
    RECORDS_READ = 'records_read' # holds the number of records read so far
    METADATA_SENT = 'metadata_sent' # holds a flag indicating if the footer has been sent

class WfpMetadataParserDataParticleKey(BaseEnum):
    WFP_TIME_ON = 'wfp_time_on'
    WFP_TIME_OFF = 'wfp_time_off'
    WFP_NUMBER_SAMPLES = 'wfp_number_samples'

class WfpCFileCommonParser(BufferLoadingParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 filesize,
                 e_file_time_pressure_tuples=None,
                 *args, **kwargs):
        self._start_time = 0.0
        self._time_increment = 0.0
        self._filesize = filesize
        if filesize < FOOTER_BYTES:
            raise SampleException('File must be at least %d bytes to read the timestamp' % FOOTER_BYTES)
        self._read_state = {StateKey.POSITION: 0,
                            StateKey.RECORDS_READ: 0,
                            StateKey.METADATA_SENT: False}
        self._e_file_time_pressure_tuples = e_file_time_pressure_tuples
        super(WfpCFileCommonParser, self).__init__(config,
                                                   stream_handle,
                                                   state,
                                                   self.sieve_function,
                                                   state_callback,
                                                   publish_callback,
                                                   exception_callback,
                                                   *args, **kwargs)

        # need to read the footer every time to calculate start time and time increment
        self.read_footer()
        if state:
            self.set_state(state)

    def sieve_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.
        This is needed instead of a regex because blocks are identified by position
        in this binary file.
        @param raw_data The raw data read from the file
        """
        data_index = 0
        return_list = []
        raw_data_len = len(raw_data)

        while data_index < raw_data_len:
            # check if this is a status or data sample message
            if EOP_ONLY_MATCHER.match(raw_data[data_index:data_index + DATA_RECORD_BYTES]):
                if (raw_data_len - (data_index + DATA_RECORD_BYTES)) >= TIME_RECORD_BYTES:
                    return_list.append((data_index, data_index + DATA_RECORD_BYTES + TIME_RECORD_BYTES))
                    break;
                else:
                    # not enough bytes have been read to get both the end of profile and timestamps, need to wait for more
                    break
            else:
                return_list.append((data_index, data_index + DATA_RECORD_BYTES))
                data_index += DATA_RECORD_BYTES

            # if the remaining bytes are less than the data sample bytes, we will just have the
            # timestamp which is parsed in __init__
            if (raw_data_len - data_index) < DATA_RECORD_BYTES:
                break
        return return_list

    def extract_metadata_particle(self, raw_data, timestamp):
        """
        Class for extracting metadata for a particular data particle, need to override this 
        """
        ##something like this:
        #sample = self._extract_sample(WfpMetadataParserDataParticle, None, raw_data, timestamp)
        #return sample
        raise NotImplementedError("extract_metadata_particle must be overridden")

    def extract_data_particle(self, raw_data, timestamp):
        """
        Class for extracting the data sample data particle, need to override this 
        """
        ##something like this:
        #sample = self._extract_sample(DofstKWfpParserDataParticle, None, raw_data, timestamp)
        #return sample
        raise NotImplementedError("extract_data_particle must be overridden")

    def read_footer(self):
        """
        Read the footer of the file including the end of profile marker (a record filled with \xFF), and
        the on and off timestamps for the profile.  Use these to calculate the time increment, which is
        needed to be able to calculate the timestamp for each data sample record.
        @throws SampleException if the number of samples is not an even integer
        """
        pad_bytes = 10
        # seek backwards from end of file, give us extra 10 bytes padding in case 
        # end of profile / timestamp is not right at the end of the file
        if self._filesize > (FOOTER_BYTES + pad_bytes):
            self._stream_handle.seek(-(FOOTER_BYTES+pad_bytes), 2)
        else:
            # if this file is too short, use a smaller number of pad bytes
            pad_bytes = self._filesize - FOOTER_BYTES
            self._stream_handle.seek(0)

        footer = self._stream_handle.read(FOOTER_BYTES+pad_bytes)
        # make sure we are at the end of the profile marker
        match = EOP_MATCHER.search(footer)
        if match:
            timefields = struct.unpack('>II', match.group(2))
            self._start_time = int(timefields[0])
            end_time = int(timefields[1])
            extra_end_bytes = pad_bytes - match.start(1)
            number_samples = float(self._filesize - FOOTER_BYTES - extra_end_bytes) / float(DATA_RECORD_BYTES)
            if number_samples > 0:
                self._time_increment = float(end_time - self._start_time) / number_samples
            else:
                self._time_increment = 0.0

            if not number_samples.is_integer():
                raise SampleException("File does not evenly fit into number of samples")
            if not self._read_state[StateKey.METADATA_SENT]:
                self.footer_data = (match.group(2), number_samples)
            # reset the file handle to the beginning of the file
            self._stream_handle.seek(0)
        else:
            raise SampleException("Unable to find end of profile and timestamps, this file is no good!")

    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to. 
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")
        if not (StateKey.POSITION in state_obj) or not \
        (StateKey.RECORDS_READ in state_obj) or not \
        (StateKey.METADATA_SENT in state_obj):
            raise DatasetParserException("Invalid state keys")
        self._chunker.clean_all_chunks()
        self._record_buffer = []
        self._saved_header = None
        self._state = state_obj
        self._read_state = state_obj
        self._stream_handle.seek(state_obj[StateKey.POSITION])

    def _increment_state(self, increment, records_read):
        """
        Increment the parser state
        @param increment the number of bytes to increment the file position 
        @param timestamp The timestamp completed up to that position
        @param records_read The number of new records that have been read
        """
        self._read_state[StateKey.POSITION] += increment
        self._read_state[StateKey.RECORDS_READ] += records_read

    def calc_timestamp(self, record_number):
        """
        calculate the timestamp for a specific record
        @param record_number The number of the record to calculate the timestamp for
        @retval A floating point NTP64 formatted timestamp
        """
        timestamp = self._start_time + (self._time_increment * record_number)
        return float(ntplib.system_to_ntp_time(timestamp))

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """     
        result_particles = []

        if not self._read_state[StateKey.METADATA_SENT] and not self.footer_data is None:
            timestamp = float(ntplib.system_to_ntp_time(self._start_time))
            sample = self.extract_metadata_particle(self.footer_data, timestamp)
            self._read_state[StateKey.METADATA_SENT] = True
            result_particles.append((sample, copy.copy(self._read_state)))

        (timestamp, chunk) = self._chunker.get_next_data()

        while (chunk != None):
            # particle-ize the data block received, return the record
            if EOP_MATCHER.match(chunk):
                # this is the end of profile matcher, just increment the state
                self._increment_state(DATA_RECORD_BYTES + TIME_RECORD_BYTES, 0)
            else:
                timestamp = self.calc_timestamp(self._read_state[StateKey.RECORDS_READ])
                sample = self.extract_data_particle(chunk, timestamp)
                if sample:
                    # create particle
                    self._increment_state(DATA_RECORD_BYTES, 1)
                    result_particles.append((sample, copy.copy(self._read_state)))

            (timestamp, chunk) = self._chunker.get_next_data()

        return result_particles

    def get_records(self, num_records):
        """
        Parse the entire file and load the particle buffer. Then adjust the
        c file sample times if given e file time pressure tuples.
        @param num_records The number of records to gather
        @retval Return the list of particles requested, [] if none available
        """
        if num_records <= 0:
            return []
        if not self.file_complete:
            try:
                while len(self._record_buffer) < num_records:
                    self._load_particle_buffer()
            except EOFError:
                self._process_end_of_file()

            self.file_complete = True

            if self._e_file_time_pressure_tuples:
                self.adjust_c_file_sample_times()

        return self._yank_particles(num_records)

    def adjust_c_file_sample_times(self):
        """
        Set the time in the "c" samples (CTD, DOSTA) generated from the "C" data file
        from the time in the "e" samples (FLORT, PARAD) generated from the "E" data file
        when the pressures in both samples match
        @throws Exception if there are not any e_samples or if we could not find
        matching pressures in the c and e samples
        """
        # precision for comparing the pressures in c and e samples
        precision = 0.02

        e_samples_size = len(self._e_file_time_pressure_tuples)
        curr_e_sample_index = 0
        curr_e_sample = self._e_file_time_pressure_tuples[curr_e_sample_index]
        curr_e_sample_time = curr_e_sample[0]
        curr_e_sample_pressure = curr_e_sample[1]
        prev_e_sample_time = None

        # These will get set below as we iterate the c and e samples
        c_sample_time_interval = None
        prev_c_sample_time = None

        final_e_sample_matched = False

        # Create a buffer to temporarily hold the ctd samples until we find one
        # having the same pressure as the current flort sample
        c_samples_before_curr_e_sample = []

        # Counter for the number of c_samples since the last e_sample pressure match
        # This will equal len(c_samples_before_curr_e_sample) after the second pressure match
        # when c_sample_time_interval will have been determined and the buffer can be cleared.
        num_c_samples_between_e_samples = 0

        for particle_status_tuple in self._record_buffer:

            # Only adjust the times in the data particles, not metadata particles
            if not isinstance(particle_status_tuple[0],
                              self._config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][DATA_PARTICLE_CLASS_KEY]):
                continue

            # Place the c_sample in the temp buffer. We will Adjust the times of the samples in buffer later when
            # we find a pressure match between the c_sample and the e_sample
            c_sample = particle_status_tuple[0]
            c_samples_before_curr_e_sample.append(c_sample)
            num_c_samples_between_e_samples += 1

            # Get the pressure from the list of values (name-value pairs) in the sample
            c_sample_pressure = c_sample.get_value_from_values(
                self._config[WfpCFileCommonConfigKeys.PRESSURE_FIELD_C_FILE])

            # e_sample pressures are in dbar so convert the c_sample pressure from counts to dbar
            c_sample_pressure_dbar = ctd_sbe52mp_preswat(c_sample_pressure)

            # If we have not already matched the final e_sample and the pressures are the same in
            # the curr_e_sample and curr_c_sample, calculate and back fill the timestamps of the
            # c_samples in the temp buffer
            if not final_e_sample_matched and abs(curr_e_sample_pressure - c_sample_pressure_dbar) < precision:
                log.debug("Pressures of flort(%s) and ctd(%s) match at %s" %
                          (str(curr_e_sample_pressure), str(c_sample_pressure_dbar), str(curr_e_sample_time)))

                # If we have 2 e_sample times, we can calculate the time interval between c_samples
                # over that time range
                if prev_e_sample_time:
                    # Calculate the time interval between c_samples in the temporary c_sample buffer
                    c_sample_time_interval = (curr_e_sample_time - prev_e_sample_time) / num_c_samples_between_e_samples

                    # Initialize the prev_c_sample_time if it has not yet been found up to this point.
                    # This will be the first corrected c_sample time.
                    if not prev_c_sample_time:
                        prev_c_sample_time = curr_e_sample_time - (
                                c_sample_time_interval * len(c_samples_before_curr_e_sample))

                    # Back fill the times of the c_samples in the temp buffer and then clear the buffer
                    temp_counter = 0
                    for c_data_particle in c_samples_before_curr_e_sample:
                        temp_counter += 1
                        prev_c_sample_time += c_sample_time_interval
                        # Explicitly set the time of the last c_sample in the buffer to prevent rounding errors
                        # from the increment above from carrying forward beyond this e_sample time
                        if temp_counter == len(c_samples_before_curr_e_sample):
                            prev_c_sample_time = curr_e_sample_time
                        c_data_particle.set_value(DataParticleKey.INTERNAL_TIMESTAMP, prev_c_sample_time)
                    # Clear the temp c_sample buffer
                    c_samples_before_curr_e_sample[:] = []

                num_c_samples_between_e_samples = 0

                # Get the next e_sample if there are more
                if curr_e_sample_index < e_samples_size - 1:
                    prev_e_sample_time = curr_e_sample_time
                    curr_e_sample_index += 1
                    curr_e_sample = self._e_file_time_pressure_tuples[curr_e_sample_index]
                    curr_e_sample_time = curr_e_sample[0]
                    curr_e_sample_pressure = curr_e_sample[1]
                else:
                    final_e_sample_matched = True

        # If we did not find a pressure match between a c and e sample, the last c_sample time
        # will not have been set, indicating that the c_sample times have not been adjusted.
        if not prev_c_sample_time:
            error_message = "Could not find a match between the e_sample and c_sample to adjust c_sample time"
            log.error(error_message)
            raise Exception(error_message)

        # Roll the last c_sample time by the c_sample_time_interval and set the time on the remaining c_samples
        for c_data_particle in c_samples_before_curr_e_sample:
            prev_c_sample_time += c_sample_time_interval
            c_data_particle.set_value(DataParticleKey.INTERNAL_TIMESTAMP, prev_c_sample_time)


def get_value_from_sample(value_id, sample):
    values = [i for i in sample.get("values") if i["value_id"] == value_id]
    if not len(values):
        raise Exception("Sample did not contain value for %s" % value_id)
    return values[0]["value"]
