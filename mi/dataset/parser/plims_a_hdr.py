#!/usr/bin/env python

"""
@package mi.dataset.parser.plims_a_hdr
@file mi-dataset/mi/dataset/parser/plims_a_hdr.py
@author Samuel Dahlberg, Joffrey Peters
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

from mi.core.exceptions import (
    ConfigurationException,
    DatasetParserException,
    SampleException,
    UnexpectedDataException,
)
from mi.core.log import get_logger

log = get_logger()
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.core.time_tools import datetime_utc_to_ntp
from mi.dataset.dataset_parser import DataSetDriverConfigKeys, SimpleParser
from mi.dataset.parser.plims_a_particles import (
    PlimsAHdrClassKey,
    PlimsAHdrEngineeringParticleKey,
    PlimsAHdrInstrumentParticleKey,
    PlimsAParticleKey,
)

# Regex pattern for extracting datetime from filename
FNAME_DTIME_PATTERN = (
        r'.*' +
        r'\w(\d\d\d\d\d\d\d\dT\d\d\d\d\d\d)_' +  # Date format
        r'.+' +
        r'(?:\r\n|\n)?'  # Newline
)
FNAME_DATE_REGEX = re.compile(FNAME_DTIME_PATTERN)

class PlimsAHdrParser(SimpleParser):
    """
    Plims A (IFCB) HDR (header) base file parser. 
    The telemetered and recovered files have the same fields and contents, 
    and can use the same parser.
    """
    SEGMENTATION_STRING = ": "
    BOOLEAN_COMPARATORS = ['true', '1', 't', 'y', 'yes', 1]

    def __init__(self, config, stream_handle, exception_callback):

        # set the class types from the config
        particle_class_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
        particle_module = config.get(DataSetDriverConfigKeys.PARTICLE_MODULE)
        if particle_class_dict is not None and particle_module is not None:
            try:
                # get the particle module
                module = __import__(particle_module,
                                    fromlist=[particle_class_dict[PlimsAHdrClassKey.ENGINEERING],
                                              particle_class_dict[PlimsAHdrClassKey.INSTRUMENT]])
                # get the class from the string name of the class
                self._engineering_class = getattr(module, particle_class_dict[PlimsAHdrClassKey.ENGINEERING])
                self._instrument_class = getattr(module, particle_class_dict[PlimsAHdrClassKey.INSTRUMENT])
            except AttributeError:
                raise ConfigurationException('Config provided a class which does not exist %s' % config)
        else:
            raise ConfigurationException('Missing particle_classes_dict in config')

        super(PlimsAHdrParser, self).__init__(config, stream_handle, exception_callback)
                                                      
        # self._particle_class = config[
        #         DataSetDriverConfigKeys.PARTICLE_CLASS]

    def parse_file(self):
        """
        Parse through the file, pulling single lines and comparing to the expected variables,
        to generate science and engineering particles for the HDR file.
        """
        print("Running PlimsAHdrParser parse_file")

        file = self._stream_handle

        match = FNAME_DATE_REGEX.match(file.name)
        if match is not None:
            # convert file name date/time string to seconds since 1970-01-01 in UTC
            file_timestamp = pd.to_datetime(match.group(1), format='%Y%m%dT%H%M%S')
            internal_timestamp = datetime_utc_to_ntp(file_timestamp)
        else:
            rse = SampleException('Could not extract date from file')
            self._exception_callback(rse)
            particle = None
            raise rse

        instrument_record = {PlimsAParticleKey.SAMPLE_TIMESTAMP: internal_timestamp}
        engineering_record = {PlimsAParticleKey.SAMPLE_TIMESTAMP: internal_timestamp}


        for line in file:
            line_split = line.split(self.SEGMENTATION_STRING)
            key = line_split[0]
            value = self.SEGMENTATION_STRING.join(line_split[1:]).strip()

            if key in self._instrument_class.PARAMETER_NAME_MAP:
                instrument_record[self._instrument_class.PARAMETER_NAME_MAP[key]] = value
            elif key in self._engineering_class.PARAMETER_NAME_MAP:
                engineering_record[self._engineering_class.PARAMETER_NAME_MAP[key]] = value
            else:
                # Mostly keys that are not ingested
                error_message = 'PlimsAHdr Parser: Unknown key: {}'.format(key)
                log.trace(error_message)

        try:
            if instrument_record:
                plims_instrument_data = self.parse_instrument_record(instrument_record)
        except KeyError as ke:
            error_message = 'PlimsAHdr Instrument Parser KeyError: {}'.format(ke)
            log.error(error_message)
            self._exception_callback(UnexpectedDataException(error_message))
            plims_instrument_data = None
            raise ke
        except ValueError as ve:
            error_message = 'PlimsAHdr Instrument Parser ValueError: {}'.format(ve)
            log.error(error_message)
            self._exception_callback(UnexpectedDataException(error_message))
            plims_instrument_data = None
            raise ve

        try:
            if engineering_record:
                plims_engineering_data = self.parse_engineering_record(engineering_record)
        except KeyError as ke:
            error_message = 'PlimsAHdr Engineering Parser KeyError: {}'.format(ke)
            log.error(error_message)
            self._exception_callback(UnexpectedDataException(error_message))
            plims_engineering_data = None
            raise ke
        except ValueError as ve:
            error_message = 'PlimsAHdr Engineering Parser ValueError: {}'.format(ve)
            log.error(error_message)
            self._exception_callback(UnexpectedDataException(error_message))
            plims_engineering_data = None
            raise ve

        if plims_instrument_data is None:
            error_message = 'Erroneous instrument data found in file'
            log.error(error_message)
            dpe = DatasetParserException(error_message)
            self._exception_callback(dpe)
            raise dpe
        else:
            particle = self._extract_sample(self._instrument_class, None, plims_instrument_data,
                                            internal_timestamp=internal_timestamp,
                                            preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)
            if particle is not None:
                self._record_buffer.append(particle)
                # log.trace('Parsed instrument particle: %s' % particle.generate_dict())
            else:
                se = SampleException('Unknown instrument data found in file')
                self._exception_callback(se)
                raise se
        
        if plims_engineering_data is None:
            error_message = 'Erroneous engineering data found in file'
            log.error(error_message)
            dpe = DatasetParserException(error_message)
            self._exception_callback(dpe)
            raise dpe
        else:
            particle = self._extract_sample(self._engineering_class, None, plims_engineering_data,
                                            internal_timestamp=internal_timestamp,
                                            preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)
            if particle is not None:
                self._record_buffer.append(particle)
                # log.trace('Parsed engineering particle: %s' % particle.generate_dict())
            else:
                se = SampleException('Unknown engineering data found in file')
                self._exception_callback(se)
                raise(se)

    def parse_instrument_record(self, record):

        plims_particle_data = { 
            PlimsAHdrInstrumentParticleKey.SAMPLE_TIMESTAMP: record[PlimsAHdrInstrumentParticleKey.SAMPLE_TIMESTAMP],
            PlimsAHdrInstrumentParticleKey.SAMPLE_NUMBER: int(record[PlimsAHdrInstrumentParticleKey.SAMPLE_NUMBER]),
            PlimsAHdrInstrumentParticleKey.TRIGGER_COUNT: int(record[PlimsAHdrInstrumentParticleKey.TRIGGER_COUNT]),
            PlimsAHdrInstrumentParticleKey.ROI_COUNT: int(record[PlimsAHdrInstrumentParticleKey.ROI_COUNT]),
            PlimsAHdrInstrumentParticleKey.HUMIDITY: float(record[PlimsAHdrInstrumentParticleKey.HUMIDITY]),
            PlimsAHdrInstrumentParticleKey.RUNTIME: float(record[PlimsAHdrInstrumentParticleKey.RUNTIME]),
            PlimsAHdrInstrumentParticleKey.INHIBIT_TIME: float(record[PlimsAHdrInstrumentParticleKey.INHIBIT_TIME]),
            PlimsAHdrInstrumentParticleKey.BINARIZE_THRESHOLD: int(record[PlimsAHdrInstrumentParticleKey.BINARIZE_THRESHOLD]),
            PlimsAHdrInstrumentParticleKey.MINIMUM_BLOB_AREA: int(record[PlimsAHdrInstrumentParticleKey.MINIMUM_BLOB_AREA]),
            PlimsAHdrInstrumentParticleKey.BLOB_XGROW_AMOUNT: int(record[PlimsAHdrInstrumentParticleKey.BLOB_XGROW_AMOUNT]),
            PlimsAHdrInstrumentParticleKey.BLOB_YGROW_AMOUNT: int(record[PlimsAHdrInstrumentParticleKey.BLOB_YGROW_AMOUNT]),
            PlimsAHdrInstrumentParticleKey.MINIMUM_GAP_BETWEEN_ADJACENT_BLOBS: int(record[PlimsAHdrInstrumentParticleKey.MINIMUM_GAP_BETWEEN_ADJACENT_BLOBS]),
            PlimsAHdrInstrumentParticleKey.PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY: float(record[PlimsAHdrInstrumentParticleKey.PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY]),
            PlimsAHdrInstrumentParticleKey.PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY: float(record[PlimsAHdrInstrumentParticleKey.PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY]),
            PlimsAHdrInstrumentParticleKey.PMT_TRIGGER_SELECTION_DAQ_MCCONLY: int(record[PlimsAHdrInstrumentParticleKey.PMT_TRIGGER_SELECTION_DAQ_MCCONLY]),
            PlimsAHdrInstrumentParticleKey.PMTA_HIGH_VOLTAGE: float(record[PlimsAHdrInstrumentParticleKey.PMTA_HIGH_VOLTAGE]),
            PlimsAHdrInstrumentParticleKey.PMTB_HIGH_VOLTAGE: float(record[PlimsAHdrInstrumentParticleKey.PMTB_HIGH_VOLTAGE]),
            PlimsAHdrInstrumentParticleKey.SYRINGE_SAMPLING_SPEED: float(record[PlimsAHdrInstrumentParticleKey.SYRINGE_SAMPLING_SPEED]),
            PlimsAHdrInstrumentParticleKey.SYRINGE_SAMPLE_VOLUME: float(record[PlimsAHdrInstrumentParticleKey.SYRINGE_SAMPLE_VOLUME]),
            PlimsAHdrInstrumentParticleKey.RUN_SAMPLE_FAST: self._get_boolean(record[PlimsAHdrInstrumentParticleKey.RUN_SAMPLE_FAST]),
            PlimsAHdrInstrumentParticleKey.RUN_FAST_FACTOR: int(record[PlimsAHdrInstrumentParticleKey.RUN_FAST_FACTOR]),
            PlimsAHdrInstrumentParticleKey.COUNTER_CLEANING: int(record[PlimsAHdrInstrumentParticleKey.COUNTER_CLEANING]),
            PlimsAHdrInstrumentParticleKey.COUNTER_BEADS: int(record[PlimsAHdrInstrumentParticleKey.COUNTER_BEADS]),
        }

        return plims_particle_data

    def parse_engineering_record(self, record):

        plims_particle_data = { 
            PlimsAHdrEngineeringParticleKey.SAMPLE_TIMESTAMP: record[PlimsAHdrEngineeringParticleKey.SAMPLE_TIMESTAMP],
            PlimsAHdrEngineeringParticleKey.SAMPLE_TYPE: str(record[PlimsAHdrEngineeringParticleKey.SAMPLE_TYPE]),
            PlimsAHdrEngineeringParticleKey.TEMPERATURE: float(record[PlimsAHdrEngineeringParticleKey.TEMPERATURE]),
            PlimsAHdrEngineeringParticleKey.ADC_FILE_FORMAT: str(record[PlimsAHdrEngineeringParticleKey.ADC_FILE_FORMAT]),
            PlimsAHdrEngineeringParticleKey.AUTO_START: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.AUTO_START]),
            PlimsAHdrEngineeringParticleKey.AUTO_SHUTDOWN: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.AUTO_SHUTDOWN]),
            PlimsAHdrEngineeringParticleKey.HUMIDITY_ALARM_THRESHOLD: float(record[PlimsAHdrEngineeringParticleKey.HUMIDITY_ALARM_THRESHOLD]),
            PlimsAHdrEngineeringParticleKey.FLASHLAMP_CONTROL_VOLTAGE: float(record[PlimsAHdrEngineeringParticleKey.FLASHLAMP_CONTROL_VOLTAGE]),
            PlimsAHdrEngineeringParticleKey.HK_TRIGGER_TO_FLASHLAMP_DELAY_TIME_X434NS_DAC_MCCONLY: int(record[PlimsAHdrEngineeringParticleKey.HK_TRIGGER_TO_FLASHLAMP_DELAY_TIME_X434NS_DAC_MCCONLY]),
            PlimsAHdrEngineeringParticleKey.LASER_STATE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.LASER_STATE]),
            PlimsAHdrEngineeringParticleKey.RUNNING_CAMERA: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.RUNNING_CAMERA]),
            PlimsAHdrEngineeringParticleKey.PUMP1_STATE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.PUMP1_STATE]),
            PlimsAHdrEngineeringParticleKey.PUMP2_STATE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.PUMP2_STATE]),
            PlimsAHdrEngineeringParticleKey.STIRRER: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.STIRRER]),
            PlimsAHdrEngineeringParticleKey.TRIGGER_CONTINUOUS: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.TRIGGER_CONTINUOUS]),
            PlimsAHdrEngineeringParticleKey.ALT_PMT_TRIGGER_SELECTION_DAQ_MCCONLY: int(record[PlimsAHdrEngineeringParticleKey.ALT_PMT_TRIGGER_SELECTION_DAQ_MCCONLY]),
            PlimsAHdrEngineeringParticleKey.INTERVAL_BETWEEN_ALTERNATE_HARDWARE_SETTINGS: int(record[PlimsAHdrEngineeringParticleKey.INTERVAL_BETWEEN_ALTERNATE_HARDWARE_SETTINGS]),
            PlimsAHdrEngineeringParticleKey.ALT_FLASHLAMP_CONTROL_VOLTAGE: float(record[PlimsAHdrEngineeringParticleKey.ALT_FLASHLAMP_CONTROL_VOLTAGE]),
            PlimsAHdrEngineeringParticleKey.PUMP_DRIVE_VOLTAGE: float(record[PlimsAHdrEngineeringParticleKey.PUMP_DRIVE_VOLTAGE]),
            PlimsAHdrEngineeringParticleKey.ALT_PMTA_HIGH_VOLTAGE: float(record[PlimsAHdrEngineeringParticleKey.ALT_PMTA_HIGH_VOLTAGE]),
            PlimsAHdrEngineeringParticleKey.ALT_PMTB_HIGH_VOLTAGE: float(record[PlimsAHdrEngineeringParticleKey.ALT_PMTB_HIGH_VOLTAGE]),
            PlimsAHdrEngineeringParticleKey.ALT_PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY: float(record[PlimsAHdrEngineeringParticleKey.ALT_PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY]),
            PlimsAHdrEngineeringParticleKey.ALT_PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY: float(record[PlimsAHdrEngineeringParticleKey.ALT_PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY]),
            PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_TO_AUTORUN: int(record[PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_TO_AUTORUN]),
            PlimsAHdrEngineeringParticleKey.ALT_SYRINGE_SAMPLE_VOLUME: float(record[PlimsAHdrEngineeringParticleKey.ALT_SYRINGE_SAMPLE_VOLUME]),
            PlimsAHdrEngineeringParticleKey.SAMPLE_VOLUME_2_SKIP: float(record[PlimsAHdrEngineeringParticleKey.SAMPLE_VOLUME_2_SKIP]),
            PlimsAHdrEngineeringParticleKey.BLEACH_VOLUME: float(record[PlimsAHdrEngineeringParticleKey.BLEACH_VOLUME]),
            PlimsAHdrEngineeringParticleKey.BIOCIDE_VOLUME: float(record[PlimsAHdrEngineeringParticleKey.BIOCIDE_VOLUME]),
            PlimsAHdrEngineeringParticleKey.DEBUBBLE_WITH_SAMPLE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.DEBUBBLE_WITH_SAMPLE]),
            PlimsAHdrEngineeringParticleKey.REFILL_AFTER_DEBUBBLE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.REFILL_AFTER_DEBUBBLE]),
            PlimsAHdrEngineeringParticleKey.PRIME_SAMPLE_TUBE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.PRIME_SAMPLE_TUBE]),
            PlimsAHdrEngineeringParticleKey.BACKFLUSH_WITH_SAMPLE: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.BACKFLUSH_WITH_SAMPLE]),
            PlimsAHdrEngineeringParticleKey.RUN_BEADS: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.RUN_BEADS]),
            PlimsAHdrEngineeringParticleKey.BEADS_SAMPLE_VOLUME: float(record[PlimsAHdrEngineeringParticleKey.BEADS_SAMPLE_VOLUME]),
            PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_BETWEEN_BEADS_RUN: int(record[PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_BETWEEN_BEADS_RUN]),
            PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_BETWEEN_CLEANING_RUN: int(record[PlimsAHdrEngineeringParticleKey.NUMBER_SYRINGES_BETWEEN_CLEANING_RUN]),
            PlimsAHdrEngineeringParticleKey.FOCUS_MOTOR_SMALL_STEP_MS: int(record[PlimsAHdrEngineeringParticleKey.FOCUS_MOTOR_SMALL_STEP_MS]),
            PlimsAHdrEngineeringParticleKey.FOCUS_MOTOR_LARGE_STEP_MS: int(record[PlimsAHdrEngineeringParticleKey.FOCUS_MOTOR_LARGE_STEP_MS]),
            PlimsAHdrEngineeringParticleKey.LASER_MOTOR_SMALL_STEP_MS: int(record[PlimsAHdrEngineeringParticleKey.LASER_MOTOR_SMALL_STEP_MS]),
            PlimsAHdrEngineeringParticleKey.LASER_MOTOR_LARGE_STEP_MS: int(record[PlimsAHdrEngineeringParticleKey.LASER_MOTOR_LARGE_STEP_MS]),
            PlimsAHdrEngineeringParticleKey.BLEACH_RINSE_COUNT: int(record[PlimsAHdrEngineeringParticleKey.BLEACH_RINSE_COUNT]),
            PlimsAHdrEngineeringParticleKey.BLEACH_RINSE_VOLUME: float(record[PlimsAHdrEngineeringParticleKey.BLEACH_RINSE_VOLUME]),
            PlimsAHdrEngineeringParticleKey.BLEACH_TO_EXHAUST: self._get_boolean(record[PlimsAHdrEngineeringParticleKey.BLEACH_TO_EXHAUST]),
            PlimsAHdrEngineeringParticleKey.COUNTER_ALT: int(record[PlimsAHdrEngineeringParticleKey.COUNTER_ALT]),
        }

        return plims_particle_data

    def _get_boolean(self, value):
        """
        Convert a string value to a boolean value, comparing with various possible.
        """
        return int(value.lower() in self.BOOLEAN_COMPARATORS)