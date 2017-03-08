#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_acfgm_dcl_pd8
@file marine-integrations/mi/dataset/parser/adcpt_acfgm_dcl_pd8.py
@author Sung Ahn
@brief Parser for the adcpt_acfgm_dcl_pd8 dataset driver

This file contains code for the adcpt_acfgm_dcl_pd8 parsers and code to produce data particles.
instrument and instrument recovered.

All records start with a timestamp.
DCL log records: timestamp [text] more text newline.
Sensor Data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all dcl log records produce no particles.


The sensor data record has the following format:

<DCL Timestamp> <Instrument Timestamp> <Ensemble Number>
<DCL Timestamp> Hdg: <Heading> Pitch: <Pitch> Roll: <Roll>
<DCL Timestamp> Temp: <Temperature> SoS: <Speed of Sound> BIT: <BIT>
<DCL Timestamp> Bin    Dir    Mag     E/W     N/S    Vert     Err   Echo1  Echo2  Echo3  Echo4..
<DCL Timestamp>   1   <DIR 1>   <MAG 1>      <EW 1>      <NS 1>      <VERT 1>       <ERR 1>    <ECHO1 1>    <ECHO2 1>    <ECHO3 1>    <ECHO4 1>
<DCL Timestamp>   ...
<DCL Timestamp>   <N>   <DIR N>   <MAG N>      <EW N>      <NS N>      <VERT N>       <ERR N>    <ECHO1 N>    <ECHO2 N>    <ECHO3 N>    <ECHO4 N>


Release notes:

Initial Release
"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import re
import numpy
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    TIMESTAMP, START_METADATA, END_METADATA, START_GROUP, END_GROUP, SENSOR_GROUP_TIMESTAMP

from mi.core.exceptions import RecoverableSampleException
from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import FLOAT_REGEX, UNSIGNED_INT_REGEX, INT_REGEX, \
    SPACE_REGEX, ANY_CHARS_REGEX, ASCII_HEX_CHAR_REGEX

# Basic patterns
UINT = '(' + UNSIGNED_INT_REGEX + ')'   # unsigned integer as a group
SINT = '(' + INT_REGEX + ')'            # signed integer as a group
FLOAT = '(' + FLOAT_REGEX + ')'         # floating point as a captured group
FLOAT_OR_DASH = '(' + FLOAT_REGEX + '|\-\-' + ')'         # floating point as a captured group
MULTI_SPACE = SPACE_REGEX + '+'
ANY_NON_BRACKET_CHAR = r'[^\[\]]+'

# DCL Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
SENSOR_DATE = r'\d{4}/\d{2}/\d{2}'          # Sensor Date: YYYY/MM/DD
SENSOR_TIME = r'\d{2}:\d{2}:\d{2}.\d{2}'    # Sensor Time: HH:MM:SS.mm
TWO_HEX = '(' + ASCII_HEX_CHAR_REGEX + ASCII_HEX_CHAR_REGEX + ')'

# DCL Log record:
#   Timestamp [Text]MoreText newline
DCL_LOG_PATTERN = TIMESTAMP + SPACE_REGEX   # DCL controller timestamp
DCL_LOG_PATTERN += START_METADATA           # Metadata record starts with '['
DCL_LOG_PATTERN += ANY_NON_BRACKET_CHAR     # followed by text
DCL_LOG_PATTERN += END_METADATA             # followed by ']'
DCL_LOG_PATTERN += ANY_CHARS_REGEX          # followed by more text
DCL_LOG_MATCHER = re.compile(DCL_LOG_PATTERN)

# Header 1
# 2013/12/01 01:04:18.213 2013/12/01 01:00:27.53 00001\n
SENSOR_TIME_PATTERN = TIMESTAMP + MULTI_SPACE  # DCL controller timestamp
SENSOR_TIME_PATTERN += START_GROUP + SENSOR_DATE + MULTI_SPACE  # sensor date
SENSOR_TIME_PATTERN += SENSOR_TIME + END_GROUP + MULTI_SPACE    # sensor time
SENSOR_TIME_PATTERN += UINT                                     # Ensemble Number
SENSOR_TIME_MATCHER = re.compile(SENSOR_TIME_PATTERN)

# Header 2
# 2013/12/01 01:04:18.246 Hdg: 34.7 Pitch: 0.0 Roll: -1.1\n
SENSOR_HEAD_PATTERN = TIMESTAMP + MULTI_SPACE  # DCL controller timestamp
SENSOR_HEAD_PATTERN += 'Hdg:' + MULTI_SPACE + FLOAT + MULTI_SPACE           # Hdg
SENSOR_HEAD_PATTERN += 'Pitch:' + MULTI_SPACE + FLOAT + MULTI_SPACE         # Pitch
SENSOR_HEAD_PATTERN += 'Roll:' + MULTI_SPACE + FLOAT                        # Roll
SENSOR_HEAD_MATCHER = re.compile(SENSOR_HEAD_PATTERN)

# Header 3
# 2013/12/01 01:04:18.279 Temp: 13.8 SoS: 1505 BIT: 00\n
SENSOR_TEMP_PATTERN = TIMESTAMP + MULTI_SPACE  # DCL controller timestamp
SENSOR_TEMP_PATTERN += 'Temp:' + MULTI_SPACE + FLOAT + MULTI_SPACE          # temp
SENSOR_TEMP_PATTERN += 'SoS:' + MULTI_SPACE + SINT + MULTI_SPACE            # SoS
SENSOR_TEMP_PATTERN += 'BIT:' + MULTI_SPACE + TWO_HEX                       # sensor BIT
SENSOR_TEMP_MATCHER = re.compile(SENSOR_TEMP_PATTERN)

# Header 4
# 2013/12/01 01:04:18.363 Bin    Dir    Mag     E/W     N/S    Vert     Err   Echo1  Echo2  Echo3  Echo4\n
IGNORE_HEADING_PATTERN = TIMESTAMP + MULTI_SPACE  # DCL controller timestamp
IGNORE_HEADING_PATTERN += 'Bin' + MULTI_SPACE + ANY_CHARS_REGEX
IGNORE_HEADING_MATCHER = re.compile(IGNORE_HEADING_PATTERN)

# Sensor Data Record:
# 2013/12/01 01:04:18.446   1   14.6   47.5      12      46      -3       5    162    168    164    168\n
SENSOR_DATA_PATTERN = TIMESTAMP + MULTI_SPACE   # DCL controller timestamp
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE       # bin
SENSOR_DATA_PATTERN += FLOAT_OR_DASH + MULTI_SPACE      # Dir
SENSOR_DATA_PATTERN += FLOAT_OR_DASH + MULTI_SPACE      # Mag
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE       # E/W
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE       # N/S
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE       # Vert
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE       # Err
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE       # Echo1
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE       # Echo2
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE       # Echo3
SENSOR_DATA_PATTERN += UINT                     # Echo4
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# Empty Timestamp
# 2013/12/01 01:04:20.834\n
IGNORE_EMPTY_PATTERN = TIMESTAMP + '\s*$'       # DCL controller timestamp
IGNORE_EMPTY_MATCHER = re.compile(IGNORE_EMPTY_PATTERN)


# The following are indices into groups()
# incremented after common timestamp values.
# i.e, match.groups()[INDEX]

# SENSOR_TIME_MATCHER produces the following groups.
SENSOR_TIME_SENSOR_DATE_TIME = 8
SENSOR_TIME_ENSEMBLE = 9

# SENSOR_HEAD_MATCHER produces the following groups.
HEAD_HEADING = 8
HEAD_PITCH = 9
HEAD_ROLL = 10

# SENSOR_TEMP_MATCHER produces the following groups.
TEMP_TEMP = 8
TEMP_SOS = 9
TEMP_HEX = 10

# SENSOR_DATA_MATCHER produces the following groups.
SENSOR_DATA_BIN = 8


#  encoding function for water_velocity and water_direction
#  the adcp puts "--" in the output when these can't be computed
#  return fill value when found
def float_or_dash(val):
    if val == '--':
        return None
    else:
        return float(val)


# The following is used for DclInstrumentDataParticle._build_parsed_values() and defined as below:
# (parameter name, index into parsed_data[], encoding function)
PD8_DATA_MAP = [
    ('dcl_controller_timestamp',            0,  str),       # Last timestamp from the DCL controller
    ('instrument_timestamp',                8,  str),
    ('ensemble_number',                     9, int),
    ('heading',                             10, float),
    ('pitch',                               11, float),
    ('roll',                                12, float),
    ('temperature',                         13, float),
    ('speed_of_sound',                      14, int),
    ('bit_result_demod_1',                  15, int),
    ('bit_result_demod_0',                  16, int),
    ('bit_result_timing',                   17, int),
    ('water_direction',                     18, lambda x: [float_or_dash(y) for y in x]),
    ('water_velocity',                      19, lambda x: [float_or_dash(y) for y in x]),
    ('water_velocity_east',                 20, lambda x: [int(y) for y in x]),
    ('water_velocity_north',                21, lambda x: [int(y) for y in x]),
    ('water_velocity_up',                   22, lambda x: [int(y) for y in x]),
    ('error_velocity',                      23, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam1',                24, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam2',                25, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam3',                26, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam4',                27, lambda x: [int(y) for y in x]),
    ('num_cells',                           28, int)
]


class DataParticleType(BaseEnum):
    ADCPT_ACFGM_PD8_DCL_INSTRUMENT = 'adcpt_acfgm_pd8_dcl_instrument'
    ADCPT_ACFGM_PD8_DCL_INSTRUMENT_RECOVERED = 'adcpt_acfgm_pd8_dcl_instrument_recovered'


class AdcptAcfgmPd8InstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the adcpt_acfgm_dcl_pd8 instrument particle.
    """

    def __init__(self, raw_data, *args, **kwargs):
        super(AdcptAcfgmPd8InstrumentDataParticle, self).__init__(
            raw_data, PD8_DATA_MAP, *args, **kwargs)


class AdcptAcfgmPd8DclInstrumentParticle(AdcptAcfgmPd8InstrumentDataParticle):
    """
    Class for generating Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD8_DCL_INSTRUMENT


class AdcptAcfgmPd8DclInstrumentRecoveredParticle(AdcptAcfgmPd8InstrumentDataParticle):
    """
    Class for generating Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.ADCPT_ACFGM_PD8_DCL_INSTRUMENT_RECOVERED


class AdcptAcfgmPd8Parser(SimpleParser):
    """
    ADCPT ACFGM PD8 Parser.
    """

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def parse_file(self):
        """
        Open and read the file and parser the data within, and at the end of
        this method self._record_buffer will be filled with all the particles in the file.
        """

        while True:  # loop through file looking for beginning of an adcp data burst

            line = self._stream_handle.readline()  # READ NEXT LINE

            if line == "":
                break

            # Check if this is a DCL Log message
            dcl_log_match = DCL_LOG_MATCHER.match(line)
            if dcl_log_match:
                # verified to be a regular DCL Log. Discard & move to next line.
                continue  # skip to next line in outer loop

            line_match = SENSOR_TIME_MATCHER.match(line)
            if line_match is None:
                self.recov_exception_callback("Expected starting DCL Timestamp, received: %r" % line)
                continue  # skip to next line in outer loop

            matches = line_match.groups()
            sensor_data_list = []

            # Save timestamp from the DCL controller log and it's parts
            parsed_data = list(matches[SENSOR_GROUP_TIMESTAMP:SENSOR_TIME_SENSOR_DATE_TIME])

            # Get instrument_timestamp & ensemble_number
            parsed_data.append(matches[SENSOR_TIME_SENSOR_DATE_TIME])
            parsed_data.append(matches[SENSOR_TIME_ENSEMBLE])

            line = self._stream_handle.readline()  # READ NEXT LINE

            line_match = SENSOR_HEAD_MATCHER.match(line)
            if line_match is None:
                self.recov_exception_callback("Expecting Heading, Pitch, & Roll data, received: %r" % line)
                continue  # skip to next line in outer loop

            matches = line_match.groups()
            # Get head, pitch, & roll
            parsed_data.append(matches[HEAD_HEADING])
            parsed_data.append(matches[HEAD_PITCH])
            parsed_data.append(matches[HEAD_ROLL])

            line = self._stream_handle.readline()  # READ NEXT LINE

            line_match = SENSOR_TEMP_MATCHER.match(line)
            if line_match is None:
                self.recov_exception_callback("Expecting Temperature, Speed of Sound, & BIT data,"
                                              " received: %r" % line)
                continue  # skip to next line in outer loop

            matches = line_match.groups()
            # Get temperature,  speed of sound, & BIT values
            parsed_data.append(matches[TEMP_TEMP])
            parsed_data.append(matches[TEMP_SOS])

            binary_string = '{0:08b}'.format(int(matches[TEMP_HEX], 16))
            parsed_data.append(binary_string[3])
            parsed_data.append(binary_string[4])
            parsed_data.append(binary_string[6])

            line = self._stream_handle.readline()  # READ NEXT LINE

            line_match = IGNORE_HEADING_MATCHER.match(line)
            if line_match is None:
                self.recov_exception_callback("Expecting Header, received: %s" % line)
                continue  # skip to next line in outer loop

            # Start looking for sensor data
            while True:  # loop through all the velocity and echo data records

                line = self._stream_handle.readline()  # READ NEXT LINE

                line_match = SENSOR_DATA_MATCHER.match(line)
                if line_match is not None:
                    # Collect velocity data sextets and echo power quartets
                    sensor_data_list.append(line_match.groups()[SENSOR_DATA_BIN:])
                else:
                    try:
                        # Transpose velocity data sextets and echo power quartets
                        np_array = numpy.array(sensor_data_list)
                        parsed_data.extend(np_array.transpose().tolist()[1:])

                        # Get number of cells
                        parsed_data.append(sensor_data_list[-1][0])
                        particle = self._extract_sample(self._particle_class,
                                                        None,
                                                        parsed_data,
                                                        None)
                        if particle is not None:
                            self._record_buffer.append(particle)

                    except Exception:
                        self.recov_exception_callback("Error parsing sensor data row,"
                                                      " received: %s" % line)

                    break  # exit inner loop once a particle has been produced