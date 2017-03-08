
"""
@package mi.dataset.parser.presf_abc_dcl
@file marine-integrations/mi/dataset/parser/presf_abc_dcl.py
@author Christopher Fortin, Jeff Roy
@brief Parser for the presf_abc_dcl dataset driver

This file contains code for the presf_abc_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
The input file formats are the same for both recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 3 types of records.
Two of the record types are separated by a newline.
The third type, a data burst, is a continuing list of data values,
one per line, that continues for an arbitrary period, ending with
an explicit 'end' line
All lines start with a timestamp.
Status records: timestamp [text] more text newline.
Tide Data records: timestamp sensor_data newline.
Wave Data records: timestamp sensor_data newline.
Wave Burst records: timestamp sensor_data newline.
Wave Burst End record: timestamp 'wave: end burst'
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all status records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser import utilities

from mi.dataset.parser.common_regexes import FLOAT_REGEX, \
    ANY_CHARS_REGEX, END_OF_LINE_REGEX

from mi.dataset.parser.dcl_file_common import TIMESTAMP

# Basic patterns
FLOAT = r'(' + FLOAT_REGEX + ')'    # generic float
WHITESPACE = r'(\s*)'                # any whitespace
COMMA = ','                          # simple comma
TAB = '\t'                           # simple tab

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE_TIME_STR = r'(\d{2} [a-zA-Z]{3} \d{4} \d{2}:\d{2}:\d{2})'              # DateTimeStr:   DD Mon YYYY HH:MM:SS
START_METADATA = r'\['                                                # metadata delimited by []'s
END_METADATA = r'\]'

# Metadata record:
#   Timestamp [Text]MoreText newline
STATUSDATA_PATTERN = TIMESTAMP + WHITESPACE                           # dcl controller timestamp
STATUSDATA_PATTERN += START_METADATA                                  # Metadata record starts with '['
STATUSDATA_PATTERN += ANY_CHARS_REGEX                                       # followed by text
STATUSDATA_PATTERN += END_METADATA                                    # followed by ']'
STATUSDATA_PATTERN += ANY_CHARS_REGEX                                       # followed by more text
STATUSDATA_PATTERN += END_OF_LINE_REGEX                                        # metadata record ends newline
STATUSDATA_MATCHER = re.compile(STATUSDATA_PATTERN)

# match a single line TIDE record
TIDE_REGEX = TIMESTAMP + WHITESPACE                                 # dcl controller timestamp
TIDE_REGEX += 'tide:' + WHITESPACE                                  # record type
TIDE_REGEX += 'start time =' + WHITESPACE + DATE_TIME_STR + COMMA   # timestamp
TIDE_REGEX += WHITESPACE + 'p =' + WHITESPACE + FLOAT + COMMA       # pressure
TIDE_REGEX += WHITESPACE + 'pt =' + WHITESPACE + FLOAT + COMMA      # pressure temp
TIDE_REGEX += WHITESPACE + 't =' + WHITESPACE + FLOAT               # temp
TIDE_REGEX += END_OF_LINE_REGEX
TIDE_MATCHER = re.compile(TIDE_REGEX)

# match the single start line of a wave record
WAVE_START_REGEX = TIMESTAMP + WHITESPACE                           # dcl controller timestamp
WAVE_START_REGEX += 'wave:' + WHITESPACE                            # record type
WAVE_START_REGEX += 'start time =' + WHITESPACE + DATE_TIME_STR     # timestamp
WAVE_START_REGEX += END_OF_LINE_REGEX                                        #
WAVE_START_MATCHER = re.compile(WAVE_START_REGEX)

# match a wave ptfreq record
WAVE_PTFREQ_REGEX = TIMESTAMP + WHITESPACE                          # dcl controller timestamp
WAVE_PTFREQ_REGEX += 'wave:' + WHITESPACE                           # record type
WAVE_PTFREQ_REGEX += 'ptfreq =' + WHITESPACE + FLOAT                # pressure temp
WAVE_PTFREQ_REGEX += END_OF_LINE_REGEX
WAVE_PTFREQ_MATCHER = re.compile(WAVE_PTFREQ_REGEX)

# match a wave continuation line
WAVE_CONT_REGEX = TIMESTAMP + WHITESPACE + FLOAT                    # dcl controller timestamp
WAVE_CONT_REGEX += END_OF_LINE_REGEX
WAVE_CONT_MATCHER = re.compile(WAVE_CONT_REGEX)

# match the single end line of a wave record
WAVE_END_REGEX = TIMESTAMP + WHITESPACE                             # dcl controller timestamp
WAVE_END_REGEX += 'wave: end burst'                                 # record type
WAVE_END_REGEX += END_OF_LINE_REGEX
WAVE_END_MATCHER = re.compile(WAVE_END_REGEX)

# TIDE_DATA_MATCHER produces the following groups:
TIDE_GROUP_DCL_TIMESTAMP = 1
TIDE_GROUP_YEAR = 2
TIDE_GROUP_MONTH = 3
TIDE_GROUP_DAY = 4
TIDE_GROUP_HOUR = 6
TIDE_GROUP_MINUTE = 7
TIDE_GROUP_SECOND = 8
TIDE_GROUP_DATA_TIME_STRING = 12
TIDE_GROUP_ABSOLUTE_PRESSURE = 15
TIDE_GROUP_PRESSURE_TEMPERATURE = 18
TIDE_GROUP_SEAWATER_TEMPERATURE = 21

# WAVE_DATA_MATCHER produces the following groups:
WAVE_START_GROUP_DCL_TIMESTAMP = 1
WAVE_START_GROUP_YEAR = 2
WAVE_START_GROUP_MONTH = 3
WAVE_START_GROUP_DAY = 4
WAVE_START_GROUP_HOUR = 6
WAVE_START_GROUP_MINUTE = 7
WAVE_START_GROUP_SECOND = 8
WAVE_START_GROUP_DATE_TIME_STRING = 12

# WAVE_PTFREQ_MATCHER produces the following groups:
WAVE_PTFREQ_GROUP_PTEMP_FREQUENCY = 12

# CONT produces the following groups:
WAVE_CONT_GROUP_ABSOLUTE_PRESSURE = 10

# WAVE_END_MATCHER produces the following groups:
WAVE_END_GROUP_DCL_TIMESTAMP = 1

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
TIDE_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    TIDE_GROUP_DCL_TIMESTAMP,            str),
    ('date_time_string',            TIDE_GROUP_DATA_TIME_STRING,         str),
    ('absolute_pressure',           TIDE_GROUP_ABSOLUTE_PRESSURE,        float),
    ('pressure_temp',               TIDE_GROUP_PRESSURE_TEMPERATURE,     float),
    ('seawater_temperature',        TIDE_GROUP_SEAWATER_TEMPERATURE,     float)
]


class PresfAbcDclWaveParticleKey(BaseEnum):
    DCL_CONTROLLER_START_TIMESTAMP = 'dcl_controller_start_timestamp'
    DCL_CONTROLLER_END_TIMESTAMP = 'dcl_controller_end_timestamp'
    DATE_TIME_STRING = 'date_time_string'
    PTEMP_FREQUENCY = 'ptemp_frequency'
    ABSOLUTE_PRESSURE_BURST = 'absolute_pressure_burst'

# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the tide particle class
TIDE_PARTICLE_CLASS_KEY = 'tide_particle_class'
# The key for the wave particle class
WAVE_PARTICLE_CLASS_KEY = 'wave_particle_class'


class DataParticleType(BaseEnum):
    TIDE_TELEMETERED = 'presf_abc_dcl_tide_measurement'
    TIDE_RECOVERED = 'presf_abc_dcl_tide_measurement_recovered'
    WAVE_TELEMETERED = 'presf_abc_dcl_wave_burst'
    WAVE_RECOVERED = 'presf_abc_dcl_wave_burst_recovered'


class StateKey(BaseEnum):
    POSITION = 'position'       # holds the file position


class DataTypeKey(BaseEnum):
    """
    These are the possible harvester/parser pairs for this driver
    """
    PRESF_ABC_DCL_TELEMETERED = 'presf_abc_dcl_telemetered'
    PRESF_ABC_DCL_RECOVERED = 'presf_abc_dcl_recovered'


class PresfAbcDclParserTideDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl tide data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(PresfAbcDclParserTideDataParticle, self).__init__(raw_data,
                                                                port_timestamp,
                                                                internal_timestamp,
                                                                preferred_timestamp,
                                                                quality_flag,
                                                                new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.

        utc_time = utilities.dcl_controller_timestamp_to_utc_time(self.raw_data.group(TIDE_GROUP_DCL_TIMESTAMP))

        self.set_internal_timestamp(unix_time=utc_time)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """
        return [self._encode_value(name, self.raw_data.group(group), function)
                for name, group, function in TIDE_PARTICLE_MAP]


class PresfAbcDclParserWaveDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl wave data set
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(PresfAbcDclParserWaveDataParticle, self).__init__(raw_data,
                                                                port_timestamp,
                                                                internal_timestamp,
                                                                preferred_timestamp,
                                                                quality_flag,
                                                                new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        dcl_timestamp = self.raw_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP]

        utc_time = utilities.dcl_controller_timestamp_to_utc_time(dcl_timestamp)

        self.set_internal_timestamp(unix_time=utc_time)

    # noinspection PyListCreation
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        """

        result = []
        
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP,
                                         self.raw_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP],
                                         str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DCL_CONTROLLER_END_TIMESTAMP,
                                         self.raw_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_END_TIMESTAMP],
                                         str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DATE_TIME_STRING,
                                         self.raw_data[PresfAbcDclWaveParticleKey.DATE_TIME_STRING],
                                         str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY,
                                         self.raw_data[PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY], float))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST,
                                         map(float,
                                             self.raw_data[PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST]),
                                         list))

        return result


class PresfAbcDclRecoveredTideDataParticle(PresfAbcDclParserTideDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.TIDE_RECOVERED


class PresfAbcDclTelemeteredTideDataParticle(PresfAbcDclParserTideDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TIDE_TELEMETERED


class PresfAbcDclRecoveredWaveDataParticle(PresfAbcDclParserWaveDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.WAVE_RECOVERED


class PresfAbcDclTelemeteredWaveDataParticle(PresfAbcDclParserWaveDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.WAVE_TELEMETERED


class PresfAbcDclParser(SimpleParser):
    """
    """
    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            # this is a telemetered parser
            self._wave_particle_class = PresfAbcDclTelemeteredWaveDataParticle
            self._tide_particle_class = PresfAbcDclTelemeteredTideDataParticle

        else:
            self._wave_particle_class = PresfAbcDclRecoveredWaveDataParticle
            self._tide_particle_class = PresfAbcDclRecoveredTideDataParticle

        super(PresfAbcDclParser, self).__init__({},
                                                stream_handle,
                                                exception_callback)

    @staticmethod
    def empty_wave_data():
        wave_data = {}
        wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP] = None
        wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_END_TIMESTAMP] = None
        wave_data[PresfAbcDclWaveParticleKey.DATE_TIME_STRING] = None
        wave_data[PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY] = None
        wave_data[PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST] = []

        return wave_data

    def parse_file(self):
        """
        The main parsing function which loops over each line in the file and extracts particles if the correct
        format is found.
        """

        wave_data = self.empty_wave_data()  # start with an empty wave data raw data dictionary

        for line in self._stream_handle:
            # first check for status lines
            test_status = STATUSDATA_MATCHER.match(line)
            if test_status:
                #  we have a status line, do nothing
                continue  # read next line

            # check for a single line tide record
            test_tide = TIDE_MATCHER.match(line)
            if test_tide:
                # we have a tide record, create a particle
                particle = self._extract_sample(self._tide_particle_class,
                                                None,
                                                test_tide,
                                                None)
                self._record_buffer.append(particle)
                continue  # read next line

            # check for a wave burst start
            test_wstart = WAVE_START_MATCHER.match(line)
            if test_wstart:
                # we got the start of a wave burst, make sure we expected it
                if wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP] is None:
                    # fill in initial values
                    wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP] = \
                        test_wstart.group(WAVE_START_GROUP_DCL_TIMESTAMP)
                    wave_data[PresfAbcDclWaveParticleKey.DATE_TIME_STRING] = \
                        test_wstart.group(WAVE_START_GROUP_DATE_TIME_STRING)
                else:
                    # something went wrong
                    log.debug("got unexpected wave start ")
                    self._exception_callback(RecoverableSampleException(
                        "got unexpected wave start on line %s"
                        % line))
                    wave_data = self.empty_wave_data()  # reset the wave data

                continue  # read next line

            # check for a wave ptfreq
            test_ptfreq = WAVE_PTFREQ_MATCHER.match(line)
            if test_ptfreq:
                # we got the ptfreq line, make sure we expected it
                if wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP] is not None and \
                        wave_data[PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY] is None:
                    # save value
                    wave_data[PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY] = \
                        test_ptfreq.group(WAVE_PTFREQ_GROUP_PTEMP_FREQUENCY)
                else:
                    # something went wrong
                    log.debug("got unexpected ptfreq ")
                    self._exception_callback(RecoverableSampleException(
                        "got unexpected ptfreq on line %s"
                        % line))
                    wave_data = self.empty_wave_data()  # reset the wave data
                continue  # read next line

            # check for a wave pressure
            test_wcont = WAVE_CONT_MATCHER.match(line)
            if test_wcont:
                # we got a wave burst continuation, make sure we expected one
                if wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP] is not None and \
                        wave_data[PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY] is not None:
                    # append the value to the data
                    wave_data[PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST].append(
                        test_wcont.group(WAVE_CONT_GROUP_ABSOLUTE_PRESSURE))
                else:
                    # something went wrong
                    log.debug("got unexpected wave pressure data")
                    self._exception_callback(RecoverableSampleException(
                        "got unexpected wave pressure data on line %s"
                        % line))
                    wave_data = self.empty_wave_data()  # reset the wave data
                continue  # read next line

            # check for a wave end burst
            test_wend = WAVE_END_MATCHER.match(line)
            if test_wend:
                # found the wave end line, save data and try to create a particle
                wave_data[PresfAbcDclWaveParticleKey.DCL_CONTROLLER_END_TIMESTAMP] = \
                    test_wend.group(WAVE_END_GROUP_DCL_TIMESTAMP)

                # check to make sure we have all the parts of a wave burst
                wave_data_values = wave_data.values()
                if None in wave_data_values or wave_data[PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST] == []:

                    log.debug("got wave end burst without complete burst data")
                    self._exception_callback(RecoverableSampleException(
                        "got wave end burst without complete burst data on line %s"
                        % line))
                else:  # if they are all there create a particle

                    particle = self._extract_sample(self._wave_particle_class,
                                                    None,
                                                    wave_data,
                                                    None)

                    self._record_buffer.append(particle)

                wave_data = self.empty_wave_data()  # reset the wave data

                continue  # read next line

            # if we got here the line does not match any of the expected patterns
            self._exception_callback(RecoverableSampleException("Found unexpected data in line  %s" % line))






