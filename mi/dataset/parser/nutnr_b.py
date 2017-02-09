#!/usr/bin/env python

"""
@package mi.dataset.parser.nutnrb
@file mi/dataset/parser/nutnr_b.py
@author Roger Unwin
@brief Parser for the CE_ISSM_RI_NUTNR_B dataset driver
"""

__author__ = 'mworden'
__license__ = 'Apache 2.0'

import calendar
import datetime
import ntplib
import re
from types import NoneType

from mi.core.exceptions import UnexpectedDataException, \
    RecoverableSampleException, SampleException

from mi.core.common import BaseEnum
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.parser.nutnr_b_particles import \
    NutnrBMetadataRecoveredDataParticle, \
    NutnrBInstrumentRecoveredDataParticle, \
    NutnrBDarkInstrumentRecoveredDataParticle, \
    NutnrBDataParticleKey
from mi.dataset.dataset_parser import Parser
from mi.dataset.parser.common_regexes import THREE_CHAR_DAY_OF_WEEK_REGEX, \
    THREE_CHAR_MONTH_REGEX, TIME_HR_MIN_SEC_REGEX, END_OF_LINE_REGEX, \
    DATE_DAY_REGEX, DATE_YEAR_REGEX, FLOAT_REGEX, INT_REGEX

# SATNHR,First Power: DDD MMM DD HH:MM:SS YYYY
FIRST_POWER_LINE_REGEX = \
    r'SATNHR,First Power: ' + '(' + THREE_CHAR_DAY_OF_WEEK_REGEX + \
    ' ' + THREE_CHAR_MONTH_REGEX + ' ' + DATE_DAY_REGEX + ' ' + \
    TIME_HR_MIN_SEC_REGEX + ' ' + DATE_YEAR_REGEX + ')' + \
    END_OF_LINE_REGEX
log.debug("%s", FIRST_POWER_LINE_REGEX)
FIRST_POWER_LINE_MATCHER = re.compile(FIRST_POWER_LINE_REGEX)

# SATNHR,ISUS Satlantic Firmware Version N.N.N (MMM DD YYYY HH:MM:SS)

FIRMWARE_VERSION_REGEX = r'(\d\.\d\.\d)'
FIRMWARE_VERSION_LINE_DATE_TIME_REGEX = \
    r'(' + THREE_CHAR_MONTH_REGEX + ' ' + DATE_DAY_REGEX + ' ' + \
    DATE_YEAR_REGEX + ' ' + TIME_HR_MIN_SEC_REGEX + ')'

FIRMWARE_VERSION_LINE_REGEX = r'SATNHR,ISUS Satlantic Firmware Version ' + \
                              FIRMWARE_VERSION_REGEX + ' \(' + \
                              FIRMWARE_VERSION_LINE_DATE_TIME_REGEX + '\)' + \
                              END_OF_LINE_REGEX
log.debug("%s", FIRMWARE_VERSION_LINE_REGEX)
FIRMWARE_VERSION_LINE_MATCHER = re.compile(FIRMWARE_VERSION_LINE_REGEX)


class MetadataStateDataKey(BaseEnum):
    FIRST_POWER_LINE_MATCH_STATE = 0x1
    FIRMWARE_VERSION_LINE_MATCH_STATE = 0x2

ALL_METADATA_RECEIVED = 0x3

SEPARATOR = ','

INST_COMMON_REGEX = '([0-9a-zA-Z]{4})'                      # serial number
INST_COMMON_REGEX += SEPARATOR
INST_COMMON_REGEX += '(\d{7})' + SEPARATOR                   # julian date sample was recorded
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal time of sample
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal nitrate concentration
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal first fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal second fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal third fitting result
INST_COMMON_REGEX += '(' + FLOAT_REGEX + ')'                 # decimal RMS error

INSTRUMENT_LINE_REGEX = '(SAT)'                                 # frame header
INSTRUMENT_LINE_REGEX += '(NLF|NDF)'                             # frame type
INSTRUMENT_LINE_REGEX += INST_COMMON_REGEX + SEPARATOR
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp interior
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp spectrometer
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal temp lamp
INSTRUMENT_LINE_REGEX += '(' + INT_REGEX + ')' + SEPARATOR       # decimal lamp time
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal humidity
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage lamp
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage analog
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal voltage main
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal ref channel average
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal ref channel variance
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal sea water dark
INSTRUMENT_LINE_REGEX += '(' + FLOAT_REGEX + ')' + SEPARATOR     # decimal spec channel average
INSTRUMENT_LINE_REGEX += '((?:' + INT_REGEX + SEPARATOR + '){255}' + \
                         INT_REGEX + ')'                     # 256 int spectral values
INSTRUMENT_LINE_MATCHER = re.compile(INSTRUMENT_LINE_REGEX)


class InstrumentDataMatchGroups(BaseEnum):
    INST_GROUP_FRAME_HEADER = 1
    INST_GROUP_FRAME_TYPE = 2
    INST_GROUP_SERIAL_NUMBER = 3
    INST_GROUP_JULIAN_DATE = 4
    INST_GROUP_TIME_OF_DAY = 5
    INST_GROUP_NITRATE = 6
    INST_GROUP_AUX_FITTING1 = 7
    INST_GROUP_AUX_FITTING2 = 8
    INST_GROUP_AUX_FITTING3 = 9
    INST_GROUP_RMS_ERROR = 10
    INST_GROUP_TEMP_INTERIOR = 11
    INST_GROUP_TEMP_SPECTROMETER = 12
    INST_GROUP_TEMP_LAMP = 13
    INST_GROUP_LAMP_TIME = 14
    INST_GROUP_HUMIDITY = 15
    INST_GROUP_VOLTAGE_LAMP = 16
    INST_GROUP_VOLTAGE_ANALOG = 17
    INST_GROUP_VOLTAGE_MAIN = 18
    INST_GROUP_REF_CHANNEL_AVERAGE = 19
    INST_GROUP_REF_CHANNEL_VARIANCE = 20
    INST_GROUP_SEA_WATER_DARK = 21
    INST_GROUP_SPEC_CHANNEL_AVERAGE = 22
    INST_GROUP_SPECTRAL_CHANNELS = 23

# The following lines should be ignored.
# SATNHR,Extinction coefficient file: COEF\ISUS260B.CAL
# SATNHR,Using shutter darks
# SATNHR,Integration Time: 750
# SATNHR,Dark measurements: 1
# SATNHR,Light measurements: 10
# SATNHR,Baseline order: 1
# SATNHR,Exclude Extinct Channels
# SATNHR,FittingChannels: 35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63
# SATNHR,Coefficient Vals: 1.896960e+02,7.795700e-01,1.325180e-04,-2.963280e-07
# SATNHR,L,190.5,191.3,192.0,192.8,193.6,194.4,195.2,195.9,196.7,197.5,198.3,199.1,199.9,200.6,201.4,202.2,203.0,203.8,204.6,205.3,206.1,206.9,207.7,208.5,209.3,210.0,210.8,211.6,212.4,213.2,214.0,214.8,215.6,216.3,217.1,217.9,218.7,219.5,220.3,221.1,221.9,222.6,223.4,224.2,225.0,225.8,226.6,227.4,228.2,229.0,229.8,230.6,231.3,232.1,232.9,233.7,234.5,235.3,236.1,236.9,237.7,238.5,239.3,240.1,240.8,241.6,242.4,243.2,244.0,244.8,245.6,246.4,247.2,248.0,248.8,249.6,250.4,251.2,252.0,252.8,253.6,254.3,255.1,255.9,256.7,257.5,258.3,259.1,259.9,260.7,261.5,262.3,263.1,263.9,264.7,265.5,266.3,267.1,267.9,268.7,269.5,270.3,271.1,271.9,272.7,273.5,274.3,275.1,275.9,276.7,277.5,278.3,279.1,279.9,280.6,281.4,282.2,283.0,283.8,284.6,285.4,286.2,287.0,287.8,288.6,289.4,290.2,291.0,291.8,292.6,293.4,294.2,295.0,295.8,296.6,297.4,298.2,299.0,299.8,300.6,301.4,302.2,303.0,303.8,304.6,305.4,306.2,307.0,307.8,308.6,309.4,310.2,311.0,311.8,312.6,313.4,314.2,315.0,315.8,316.6,317.4,318.2,319.0,319.8,320.6,321.4,322.2,323.0,323.8,324.6,325.4,326.2,327.0,327.8,328.6,329.4,330.2,331.0,331.8,332.6,333.4,334.2,335.0,335.8,336.6,337.4,338.2,339.0,339.8,340.6,341.4,342.2,343.0,343.8,344.6,345.4,346.1,346.9,347.7,348.5,349.3,350.1,350.9,351.7,352.5,353.3,354.1,354.9,355.7,356.5,357.3,358.1,358.9,359.7,360.5,361.3,362.1,362.9,363.7,364.5,365.3,366.0,366.8,367.6,368.4,369.2,370.0,370.8,371.6,372.4,373.2,374.0,374.8,375.6,376.4,377.2,378.0,378.7,379.5,380.3,381.1,381.9,382.7,383.5,384.3,385.1,385.9,386.7,387.5,388.2,389.0,389.8,390.6,391.4,392.2,393.0
IGNORE_REGEX = \
    r'(?:SATNHR,Extinction coefficient file:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Using shutter darks' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Integration Time:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Dark measurements:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Light measurements:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Baseline order:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Exclude Extinct Channels' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,FittingChannels:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,Coefficient Vals:.*' + END_OF_LINE_REGEX + '|' + \
    'SATNHR,L,.*' + END_OF_LINE_REGEX + ')'
IGNORE_MATCHER = re.compile(IGNORE_REGEX)


class ParticleClassKey(BaseEnum):
    METADATA_PARTICLE_CLASS = 'metadata_particle'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle'


class NutnrBParser(Parser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(NutnrBParser, self).__init__(config,
                                           stream_handle,
                                           None,           # State not used
                                           None,           # Sieve function not used
                                           None,           # state callback not used
                                           None,           # publish callback not used
                                           exception_callback)

        self._file_parsed = False
        self._record_buffer = []
        self._metadata_state = 0
        self._metadata = {NutnrBDataParticleKey.STARTUP_TIME_STRING: None,
                          NutnrBDataParticleKey.FIRMWARE_VERSION: None,
                          NutnrBDataParticleKey.FIRMWARE_DATE: None}

        self._metadata_particle_generated = False
        self._reported_missing_metadata = False

    def _process_startup_time_match(self, first_power_line_match):

        self._metadata[NutnrBDataParticleKey.STARTUP_TIME_STRING] = \
            first_power_line_match.group(1)

        self._metadata_state |= MetadataStateDataKey.FIRST_POWER_LINE_MATCH_STATE

    def _process_firmware_version_line_match(self, firmware_version_line_match):

        self._metadata[NutnrBDataParticleKey.FIRMWARE_VERSION] = \
            firmware_version_line_match.group(1)
        self._metadata[NutnrBDataParticleKey.FIRMWARE_DATE] = \
            firmware_version_line_match.group(2)

        self._metadata_state |= MetadataStateDataKey.FIRMWARE_VERSION_LINE_MATCH_STATE

    def _date_time_sample_values_to_ntp_timestamp(self, date_sample_str, time_sample_str):

        year = int(date_sample_str[0:4])
        days = int(date_sample_str[4:7])

        hours_float = float(time_sample_str)

        date_time_val = datetime.datetime(year, 1, 1) + datetime.timedelta(days=days-1, hours=hours_float)
        ntp_timestamp = ntplib.system_to_ntp_time(calendar.timegm(date_time_val.timetuple()))

        return ntp_timestamp

    def _create_metadata_particle(self, serial_number, timestamp):

        startup_time = self._metadata[NutnrBDataParticleKey.STARTUP_TIME_STRING]
        firmware_version = self._metadata[NutnrBDataParticleKey.FIRMWARE_VERSION]
        firmware_date = self._metadata[NutnrBDataParticleKey.FIRMWARE_DATE]

        serial_number_encoding = NoneType
        startup_time_encoding = NoneType
        firmware_version_encoding = NoneType
        firmware_date_encoding = NoneType

        if serial_number is not None:
            serial_number_encoding = str
        if startup_time is not None:
            startup_time_encoding = str
        if firmware_version is not None:
            firmware_version_encoding = str
        if serial_number is not None:
            firmware_date_encoding = str

        # Generate the metadata particle
        metadata_tuple = (
            (NutnrBDataParticleKey.SERIAL_NUMBER,
             serial_number,
             serial_number_encoding),
            (NutnrBDataParticleKey.STARTUP_TIME_STRING,
             startup_time,
             startup_time_encoding),
            (NutnrBDataParticleKey.FIRMWARE_VERSION,
             firmware_version,
             firmware_version_encoding),
            (NutnrBDataParticleKey.FIRMWARE_DATE,
             firmware_date,
             firmware_date_encoding))

        # Generate the metadata particle class and add the
        # result to the list of particles to be returned.
        particle = self._extract_sample(NutnrBMetadataRecoveredDataParticle,
                                        None,
                                        metadata_tuple,
                                        timestamp)
        if particle is not None:
            log.debug("Metadata Particle: %s", particle.generate())
            self._record_buffer.append(particle)

            self._metadata_particle_generated = True

    def _process_instrument_line_match(self, instrument_line_match):

        timestamp = self._date_time_sample_values_to_ntp_timestamp(
            instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_JULIAN_DATE),
            instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_TIME_OF_DAY)
        )

        metadata_values = self._metadata.values()

        if None in metadata_values \
                and self._metadata_particle_generated is False:

            if not self._reported_missing_metadata:
                message = 'Missing metadata'
                log.warn(message)
                self._exception_callback(RecoverableSampleException(message))
                self._reported_missing_metadata = True

        if self._metadata_particle_generated is False:

            self._create_metadata_particle(
                instrument_line_match.group(
                    InstrumentDataMatchGroups.INST_GROUP_SERIAL_NUMBER),
                timestamp)

        frame_type = instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_TYPE)

        # need to determine if this is a light or dark frame
        # spectral channel parameter and stream names are
        # different for each
        if frame_type == 'NLF':
            particle_class = NutnrBInstrumentRecoveredDataParticle
            spectral_key = NutnrBDataParticleKey.SPECTRAL_CHANNELS
        elif frame_type == "NDF":
            particle_class = NutnrBDarkInstrumentRecoveredDataParticle
            spectral_key = NutnrBDataParticleKey.DARK_FRAME_SPECTRAL_CHANNELS

        else:  # this should never happen but just in case
            message = "invalid frame type passed to particle"
            log.error(message)
            raise SampleException(message)

        # NutnrBInstrumentRecoveredDataParticle
        instrument_tuple = (
            (NutnrBDataParticleKey.FRAME_HEADER,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_HEADER),
             str),
            (NutnrBDataParticleKey.FRAME_TYPE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_TYPE),
             str),
            (NutnrBDataParticleKey.SERIAL_NUMBER,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_SERIAL_NUMBER),
             str),
            (NutnrBDataParticleKey.DATE_OF_SAMPLE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_JULIAN_DATE),
             int),
            (NutnrBDataParticleKey.TIME_OF_SAMPLE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_TIME_OF_DAY),
             float),
            (NutnrBDataParticleKey.NITRATE_CONCENTRATION,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_NITRATE),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_1,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_AUX_FITTING1),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_2,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_AUX_FITTING2),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_3,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_AUX_FITTING3),
             float),
            (NutnrBDataParticleKey.RMS_ERROR,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_RMS_ERROR),
             float),
            (NutnrBDataParticleKey.TEMP_INTERIOR,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_INTERIOR),
             float),
            (NutnrBDataParticleKey.TEMP_SPECTROMETER,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_SPECTROMETER),
             float),
            (NutnrBDataParticleKey.TEMP_LAMP,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_LAMP),
             float),
            (NutnrBDataParticleKey.LAMP_TIME,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_LAMP_TIME),
             int),
            (NutnrBDataParticleKey.HUMIDITY,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_HUMIDITY),
             float),
            (NutnrBDataParticleKey.VOLTAGE_LAMP,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_LAMP),
             float),
            (NutnrBDataParticleKey.VOLTAGE_ANALOG,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_ANALOG),
             float),
            (NutnrBDataParticleKey.VOLTAGE_MAIN,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_MAIN),
             float),
            (NutnrBDataParticleKey.REF_CHANNEL_AVERAGE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_REF_CHANNEL_AVERAGE),
             float),
            (NutnrBDataParticleKey.REF_CHANNEL_VARIANCE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_REF_CHANNEL_VARIANCE),
             float),
            (NutnrBDataParticleKey.SEA_WATER_DARK,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_SEA_WATER_DARK),
             float),
            (NutnrBDataParticleKey.SPEC_CHANNEL_AVERAGE,
             instrument_line_match.group(InstrumentDataMatchGroups.INST_GROUP_SPEC_CHANNEL_AVERAGE),
             float),
            (spectral_key,
             map(int, instrument_line_match.group(
                 InstrumentDataMatchGroups.INST_GROUP_SPECTRAL_CHANNELS).split(',')),
             list)
        )
        # Generate the instrument data particle
        particle = self._extract_sample(particle_class,
                                        None,
                                        instrument_tuple,
                                        timestamp)

        if particle is not None:
            log.debug("Instrument Particle: %s", particle.generate())
            self._record_buffer.append(particle)

    def parse_file(self):
        """
        This method will parse a nutnr_b input file and collect the
        particles.
        """

        # Read the first line in the file
        line = self._stream_handle.readline()

        # While a new line in the file exists
        while line:

            first_power_line_match = FIRST_POWER_LINE_MATCHER.match(line)
            firmware_version_line_match = FIRMWARE_VERSION_LINE_MATCHER.match(line)
            ignore_match = IGNORE_MATCHER.match(line)
            instrument_line_match = INSTRUMENT_LINE_MATCHER.match(line)

            if ignore_match:

                log.debug("Found ignore match.  Line: %s", line)

            elif first_power_line_match:

                log.debug("Found match.  Line: %s", line)
                self._process_startup_time_match(first_power_line_match)

            elif firmware_version_line_match:

                log.debug("Found match.  Line: %s", line)
                self._process_firmware_version_line_match(firmware_version_line_match)

            elif instrument_line_match:

                log.debug("Found match.  Line: %s", line)

                self._process_instrument_line_match(instrument_line_match)

            # OK.  We found a line in the file we were not expecting.  Let's log a warning
            # and report a unexpected data exception.
            else:
                # If we did not get a match against part of an instrument
                # data record, we may have a bad file
                message = "Unexpected data in file, line: " + line
                log.warn(message)
                self._exception_callback(UnexpectedDataException(message))

            # Read the next line in the file
            line = self._stream_handle.readline()

        # Set an indication that the file was fully parsed
        self._file_parsed = True

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            # If the file was not read, let's parse it
            if self._file_parsed is False:
                self.parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return
