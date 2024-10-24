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

import pandas as pd
from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger

log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.core.time_tools import datetime_utc_to_ntp
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.parser.plims_a_particles import (PlimsAHdrDataParticle,
                                                 PlimsAHdrParticleKey)

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

class PlimsAHdrParser(SimpleParser):
    """
    Plims A (IFCB) HDR (header) file parser. 
    The telemetered and recovered files have the same fields and contents, 
    and can use the same parser.
    """

    def parse_record(self, record):

        plims_particle_data = {
            PlimsAHdrParticleKey.SAMPLE_NUMBER: int(record.group('sampleNumber')),
            PlimsAHdrParticleKey.SAMPLE_TYPE: str(record.group('sampleType')),
            PlimsAHdrParticleKey.TRIGGER_COUNT: int(record.group('triggerCount')),
            PlimsAHdrParticleKey.ROI_COUNT: int(record.group('roiCount')),
            PlimsAHdrParticleKey.HUMIDITY: float(record.group('humidity')),
            PlimsAHdrParticleKey.TEMPERATURE: float(record.group('temperature')),
            PlimsAHdrParticleKey.RUNTIME: float(record.group('runTime')),
            PlimsAHdrParticleKey.INHIBIT_TIME: float(record.group('inhibitTime')),
            PlimsAHdrParticleKey.PUMP1_STATE: bool(record.group('pump1State')),
            PlimsAHdrParticleKey.PUMP2_STATE: bool(record.group('pump2State')),
            PlimsAHdrParticleKey.PMTA_HIGH_VOLTAGE: float(record.group('PMTAhighVoltage')),
            PlimsAHdrParticleKey.PMTB_HIGH_VOLTAGE: float(record.group('PMTBhighVoltage')),
            PlimsAHdrParticleKey.ALT_FLASHLIGHT_CONTROL_VOLTAGE: float(record.group('Alt_FlashlampControlVoltage')),
            PlimsAHdrParticleKey.PUMP_DRIVE_VOLTAGE: float(record.group('pumpDriveVoltage')),
            PlimsAHdrParticleKey.ALT_PMTA_HIGH_VOLTAGE: float(record.group('altPMTAHighVoltage')),
            PlimsAHdrParticleKey.ALT_PMTB_HIGH_VOLTAGE: float(record.group('altPMTBHighVoltage')),
            PlimsAHdrParticleKey.SYRINGE_SAMPLING_SPEED: float(record.group('syringeSamplingSpeed')),
            PlimsAHdrParticleKey.SYRINGE_OFFSET: float(record.group('syringeOffset')),
            PlimsAHdrParticleKey.NUMBER_SYRINGES_TO_AUTORUN: int(record.group('NumberSyringesToAutoRun')),
            PlimsAHdrParticleKey.SYRINGE_SAMPLE_VOLUME: float(record.group('SyringeSampleVolume')),
            PlimsAHdrParticleKey.ALT_SYRINGE_SAMPLE_VOLUME: float(record.group('altSyringeSampleVolume')),
            PlimsAHdrParticleKey.SAMPLE_VOLUME_2_SKIP: int(record.group('sampleVolume2skip')),
            PlimsAHdrParticleKey.FOCUS_MOTOR_SMALL_STEP_MS: int(record.group('focusMotorSmallStep_ms')),
            PlimsAHdrParticleKey.FOCUS_MOTOR_LARGE_STEP_MS: int(record.group('focusMotorLargeStep_ms')),
            PlimsAHdrParticleKey.LASER_MOTOR_SMALL_STEP_MS: int(record.group('laserMotorSmallStep_ms')),
            PlimsAHdrParticleKey.LASER_MOTOR_LARGE_STEP_MS: int(record.group('laserMotorLargeStep_ms'))
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
            file_timestamp = pd.to_datetime(match.group(1), format='%Y%m%dT%H%M%S')
            internal_timestamp = datetime_utc_to_ntp(file_timestamp)
        else:
            self._exception_callback(RecoverableSampleException('Could not extract date from file'))

        match = DATA_REGEX.match(file.read())
        if match is not None:
            plims_particle_data = self.parse_record(match)
            if plims_particle_data is None:
                log.error('Erroneous data found in file')
            else:
                particle = self._extract_sample(PlimsAHdrDataParticle, None, plims_particle_data,
                                                internal_timestamp=internal_timestamp,
                                                preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)
                if particle is not None:
                    self._record_buffer.append(particle)
                    log.trace('Parsed particle: %s' % particle.generate_dict())
        else:
            self._exception_callback(RecoverableSampleException('Unknown data found in file'))
