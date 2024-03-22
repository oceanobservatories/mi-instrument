#!/usr/bin/env python

"""
@package mi.dataset.parser.plims_a_hdr
@file mi-dataset/mi/dataset/parser/plims_a_hdr.py
@author Samuel Dahlberg
@brief Parser for the plims_a_hdr dataset driver.

This file contains code for the PLIMS parser and code to produce data particles
for the instrument recovered data from the PLIMS instrument.

The input file has ASCII data.
Each record is an individual file.
Instrument records: data_name: data newline.
Data records only produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.
"""

import re
from calendar import timegm

import pandas as pd

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
from mi.core.common import BaseEnum
from mi.dataset.dataset_parser import SimpleParser
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey

# Regex pattern for extracting datetime from filename
FNAME_DTIME_PATTERN = (
        r'.*' +
        r'\w(\d\d\d\d\d\d\d\dT\d\d\d\d\d\d)_' +  # Date format
        r'.+' +
        r'(?:\r\n|\n)?'  # Newline
)
FNAME_DATE_REGEX = re.compile(FNAME_DTIME_PATTERN)

UNNEEDED_DATA = (
    r'(?:.+:\s.+(?:\r\n|\n)?)+?'
)

PATTERN = (
        UNNEEDED_DATA +
        r'(?:sampleNumber:\s(?P<sampleNumber>.+?))(?:\r\n|\n)' +
        r'(?:sampleType:\s(?P<sampleType>.+?))(?:\r\n|\n)' +
        r'(?:triggerCount:\s(?P<triggerCount>.+?))(?:\r\n|\n)' +
        r'(?:roiCount:\s(?P<roiCount>.+?))(?:\r\n|\n)' +
        r'(?:humidity:\s(?P<humidity>.+?))(?:\r\n|\n)' +
        r'(?:temperature:\s(?P<temperature>.+?))(?:\r\n|\n)' +
        r'(?:runTime:\s(?P<runTime>.+?))(?:\r\n|\n)' +
        r'(?:inhibitTime:\s(?P<inhibitTime>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA +
        r'(?:pump1State:\s(?P<pump1State>.+?))(?:\r\n|\n)' +
        r'(?:pump2State:\s(?P<pump2State>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA +
        r'(?:PMTAhighVoltage:\s(?P<PMTAhighVoltage>.+?))(?:\r\n|\n)' +
        r'(?:PMTBhighVoltage:\s(?P<PMTBhighVoltage>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA +
        r'(?:Alt_FlashlampControlVoltage:\s(?P<Alt_FlashlampControlVoltage>.+?))(?:\r\n|\n)' +
        r'(?:pumpDriveVoltage:\s(?P<pumpDriveVoltage>.+?))(?:\r\n|\n)' +
        r'(?:altPMTAHighVoltage:\s(?P<altPMTAHighVoltage>.+?))(?:\r\n|\n)' +
        r'(?:altPMTBHighVoltage:\s(?P<altPMTBHighVoltage>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA +
        r'(?:syringeSamplingSpeed:\s(?P<syringeSamplingSpeed>.+?))(?:\r\n|\n)' +
        r'(?:syringeOffset:\s(?P<syringeOffset>.+?))(?:\r\n|\n)' +
        r'(?:NumberSyringesToAutoRun:\s(?P<NumberSyringesToAutoRun>.+?))(?:\r\n|\n)' +
        r'(?:SyringeSampleVolume:\s(?P<SyringeSampleVolume>.+?))(?:\r\n|\n)' +
        r'(?:altSyringeSampleVolume:\s(?P<altSyringeSampleVolume>.+?))(?:\r\n|\n)' +
        r'(?:sampleVolume2skip:\s(?P<sampleVolume2skip>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA +
        r'(?:focusMotorSmallStep_ms:\s(?P<focusMotorSmallStep_ms>.+?))(?:\r\n|\n)' +
        r'(?:focusMotorLargeStep_ms:\s(?P<focusMotorLargeStep_ms>.+?))(?:\r\n|\n)' +
        r'(?:laserMotorSmallStep_ms:\s(?P<laserMotorSmallStep_ms>.+?))(?:\r\n|\n)' +
        r'(?:laserMotorLargeStep_ms:\s(?P<laserMotorLargeStep_ms>.+?))(?:\r\n|\n)' +
        UNNEEDED_DATA
)

DATA_REGEX = re.compile(PATTERN)


class PlimsAParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted for the data particle.
    """

    SAMPLE_NUMBER = 'sample_number'
    SAMPLE_TYPE = 'sample_type'
    TRIGGER_COUNT = 'trigger_count'
    ROI_COUNT = 'roi_count'
    HUMIDITY = 'humidity'
    TEMPERATURE = 'temperature'
    RUNTIME = 'run_time'
    INHIBIT_TIME = 'inhibit_time'
    PUMP1_STATE = 'pump1_state'
    PUMP2_STATE = 'pump2_state'
    PMTA_HIGH_VOLTAGE = 'pmta_high_voltage'
    PMTB_HIGH_VOLTAGE = 'pmtb_high_voltage'
    ALT_FLASHLIGHT_CONTROL_VOLTAGE = 'alt_flashlamp_control_voltage'
    PUMP_DRIVE_VOLTAGE = 'pump_drive_voltage'
    ALT_PMTA_HIGH_VOLTAGE = 'alt_pmta_high_voltage'
    ALT_PMTB_HIGH_VOLTAGE = 'alt_pmtb_high_voltage'
    SYRINGE_SAMPLING_SPEED = 'syringe_sampling_speed'
    SYRINGE_OFFSET = 'syringe_offset'
    NUMBER_SYRINGES_TO_AUTORUN = 'number_syringes_to_autorun'
    SYRINGE_SAMPLE_VOLUME = 'syringe_sample_volume'
    ALT_SYRINGE_SAMPLE_VOLUME = 'alt_syringe_sample_volume'
    SAMPLE_VOLUME_2_SKIP = 'sample_volume_2_skip'
    FOCUS_MOTOR_SMALL_STEP_MS = 'focus_motor_small_step_ms'
    FOCUS_MOTOR_LARGE_STEP_MS = 'focus_motor_large_step_ms'
    LASER_MOTOR_SMALL_STEP_MS = 'laser_motor_small_step_ms'
    LASER_MOTOR_LARGE_STEP_MS = 'laser_motor_large_step_ms'


class DataParticleType(BaseEnum):
    PLIMS_A_PARTICLE_TYPE = 'plims_a_instrument'
    __metaclass__ = get_logging_metaclass(log_level='trace')


class PlimsADataParticle(DataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_PARTICLE_TYPE

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]


class PlimsAHdrParser(SimpleParser):

    def parse_record(self, record):

        plims_particle_data = {
            PlimsAParticleKey.SAMPLE_NUMBER: int(record.group('sampleNumber')),
            PlimsAParticleKey.SAMPLE_TYPE: str(record.group('sampleType')),
            PlimsAParticleKey.TRIGGER_COUNT: int(record.group('triggerCount')),
            PlimsAParticleKey.ROI_COUNT: int(record.group('roiCount')),
            PlimsAParticleKey.HUMIDITY: float(record.group('humidity')),
            PlimsAParticleKey.TEMPERATURE: float(record.group('temperature')),
            PlimsAParticleKey.RUNTIME: float(record.group('runTime')),
            PlimsAParticleKey.INHIBIT_TIME: float(record.group('inhibitTime')),
            PlimsAParticleKey.PUMP1_STATE: bool(record.group('pump1State')),
            PlimsAParticleKey.PUMP2_STATE: bool(record.group('pump2State')),
            PlimsAParticleKey.PMTA_HIGH_VOLTAGE: float(record.group('PMTAhighVoltage')),
            PlimsAParticleKey.PMTB_HIGH_VOLTAGE: float(record.group('PMTBhighVoltage')),
            PlimsAParticleKey.ALT_FLASHLIGHT_CONTROL_VOLTAGE: float(record.group('Alt_FlashlampControlVoltage')),
            PlimsAParticleKey.PUMP_DRIVE_VOLTAGE: float(record.group('pumpDriveVoltage')),
            PlimsAParticleKey.ALT_PMTA_HIGH_VOLTAGE: float(record.group('altPMTAHighVoltage')),
            PlimsAParticleKey.ALT_PMTB_HIGH_VOLTAGE: float(record.group('altPMTBHighVoltage')),
            PlimsAParticleKey.SYRINGE_SAMPLING_SPEED: float(record.group('syringeSamplingSpeed')),
            PlimsAParticleKey.SYRINGE_OFFSET: float(record.group('syringeOffset')),
            PlimsAParticleKey.NUMBER_SYRINGES_TO_AUTORUN: int(record.group('NumberSyringesToAutoRun')),
            PlimsAParticleKey.SYRINGE_SAMPLE_VOLUME: float(record.group('SyringeSampleVolume')),
            PlimsAParticleKey.ALT_SYRINGE_SAMPLE_VOLUME: float(record.group('altSyringeSampleVolume')),
            PlimsAParticleKey.SAMPLE_VOLUME_2_SKIP: int(record.group('sampleVolume2skip')),
            PlimsAParticleKey.FOCUS_MOTOR_SMALL_STEP_MS: int(record.group('focusMotorSmallStep_ms')),
            PlimsAParticleKey.FOCUS_MOTOR_LARGE_STEP_MS: int(record.group('focusMotorLargeStep_ms')),
            PlimsAParticleKey.LASER_MOTOR_SMALL_STEP_MS: int(record.group('laserMotorSmallStep_ms')),
            PlimsAParticleKey.LASER_MOTOR_LARGE_STEP_MS: int(record.group('laserMotorLargeStep_ms'))
        }

        return plims_particle_data

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the established patterns,
        generating particles for data lines
        """

        file = self._stream_handle

        match = FNAME_DATE_REGEX.match(file.name)
        if match is not None:
            # convert file name date/time string to seconds since 1970-01-01 in UTC
            utc = pd.to_datetime(match.group(1), format='%Y%m%dT%H%M%S', utc=True)
            internal_timestamp = timegm(utc.timetuple()) + 2208988800
        else:
            self._exception_callback(RecoverableSampleException('Could not extract date from file'))

        match = DATA_REGEX.match(file.read())
        if match is not None:
            plims_particle_data = self.parse_record(match)
            if plims_particle_data is None:
                log.error('Erroneous data found in file')
            else:
                particle = self._extract_sample(PlimsADataParticle, None, plims_particle_data,
                                                internal_timestamp=internal_timestamp,
                                                preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)
                if particle is not None:
                    self._record_buffer.append(particle)
                    log.trace('Parsed particle: %s' % particle.generate_dict())
        else:
            self._exception_callback(RecoverableSampleException('Unknown data found in file'))
