"""
@package mi.dataset.parser
@file mi-dataset/mi/dataset/parser/phsen_abcdef_imodem_particles.py
@author Joe Padula
@brief Particles for the phsen_abcdef_imodem recovered and telemetered dataset
Release notes:

initial release
"""

__author__ = 'jpadula'

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey


class DataParticleType(BaseEnum):
    PHSEN_ABCDEF_IMODEM_INSTRUMENT = 'phsen_abcdef_imodem_instrument'
    PHSEN_ABCDEF_IMODEM_INSTRUMENT_RECOVERED = 'phsen_abcdef_imodem_instrument_recovered'
    PHSEN_ABCDEF_IMODEM_CONTROL = 'phsen_abcdef_imodem_control'
    PHSEN_ABCDEF_IMODEM_CONTROL_RECOVERED = 'phsen_abcdef_imodem_control_recovered'
    PHSEN_ABCDEF_IMODEM_METADATA = 'phsen_abcdef_imodem_metadata'
    PHSEN_ABCDEF_IMODEM_METADATA_RECOVERED = 'phsen_abcdef_imodem_metadata_recovered'


class PhsenAbcdefImodemDataParticleKey(BaseEnum):
    # Common to Instrument and Control data particles
    UNIQUE_ID = 'unique_id'                                 # PD353
    RECORD_TYPE = 'record_type'                             # PD355
    RECORD_TIME = 'record_time'                             # PD356
    VOLTAGE_BATTERY = 'voltage_battery'                     # PD358
    PASSED_CHECKSUM = 'passed_checksum'                     # PD2228

    # For instrument data particles
    THERMISTOR_START = 'thermistor_start'                   # PD932
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'   # PD933
    LIGHT_MEASUREMENTS = 'light_measurements'               # PD357
    THERMISTOR_END = 'thermistor_end'                       # PD935

    # For control data particles
    CLOCK_ACTIVE = 'clock_active'                           # PD366
    RECORDING_ACTIVE = 'recording_active'                   # PD367
    RECORD_END_ON_TIME = 'record_end_on_time'               # PD368
    RECORD_MEMORY_FULL = 'record_memory_full'               # PD369
    RECORD_END_ON_ERROR = 'record_end_on_error'             # PD370
    DATA_DOWNLOAD_OK = 'data_download_ok'                   # PD371
    FLASH_MEMORY_OPEN = 'flash_memory_open'                 # PD372
    BATTERY_LOW_PRESTART = 'battery_low_prestart'           # PD373
    BATTERY_LOW_MEASUREMENT = 'battery_low_measurement'     # PD374
    BATTERY_LOW_BLANK = 'battery_low_blank'                 # PD2834
    BATTERY_LOW_EXTERNAL = 'battery_low_external'           # PD376
    EXTERNAL_DEVICE1_FAULT = 'external_device1_fault'       # PD377
    EXTERNAL_DEVICE2_FAULT = 'external_device2_fault'       # PD1113
    EXTERNAL_DEVICE3_FAULT = 'external_device3_fault'       # PD1114
    FLASH_ERASED = 'flash_erased'                           # PD378
    POWER_ON_INVALID = 'power_on_invalid'                   # PD379
    NUM_ERROR_RECORDS = 'num_error_records'                 # PD1116
    NUM_BYTES_STORED = 'num_bytes_stored'                   # PD1117

    # For control and metadata particles
    NUM_DATA_RECORDS = 'num_data_records'                   # PD1115

    # For metadata data particles
    FILE_TIME = 'file_time'                                 # PD3060
    INSTRUMENT_ID = 'instrument_id'                         # PD1089
    SERIAL_NUMBER = 'serial_number'                         # PD312
    VOLTAGE_FLT32 = 'voltage_flt32'                         # PD2649
    RECORD_LENGTH = 'record_length'                         # PD583
    NUM_EVENTS = 'num_events'                               # PD263
    NUM_SAMPLES = 'num_samples'                             # PD203

# Encoding rules for the Science (Metadata and Control) particles
SCIENCE_PARTICLE_ENCODING_RULES = [

    (PhsenAbcdefImodemDataParticleKey.UNIQUE_ID, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_TYPE, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_TIME, int),
    (PhsenAbcdefImodemDataParticleKey.PASSED_CHECKSUM, int)
]

# Encoding rules for the Instrument Data particles
INSTRUMENT_DATA_PARTICLE_ENCODING_RULES = [
    (PhsenAbcdefImodemDataParticleKey.THERMISTOR_START, int),
    (PhsenAbcdefImodemDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS, list),
    (PhsenAbcdefImodemDataParticleKey.LIGHT_MEASUREMENTS, list),
    (PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY, int),
    (PhsenAbcdefImodemDataParticleKey.THERMISTOR_END, int)
]

# Encoding rules for the Control Data particles
CONTROL_DATA_PARTICLE_ENCODING_RULES = [
    (PhsenAbcdefImodemDataParticleKey.CLOCK_ACTIVE, int),
    (PhsenAbcdefImodemDataParticleKey.RECORDING_ACTIVE, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_TIME, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_MEMORY_FULL, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_END_ON_ERROR, int),
    (PhsenAbcdefImodemDataParticleKey.DATA_DOWNLOAD_OK, int),
    (PhsenAbcdefImodemDataParticleKey.FLASH_MEMORY_OPEN, int),
    (PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_PRESTART, int),
    (PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_MEASUREMENT, int),
    (PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_BLANK, int),
    (PhsenAbcdefImodemDataParticleKey.BATTERY_LOW_EXTERNAL, int),
    (PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE1_FAULT, int),
    (PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE2_FAULT, int),
    (PhsenAbcdefImodemDataParticleKey.EXTERNAL_DEVICE3_FAULT, int),
    (PhsenAbcdefImodemDataParticleKey.FLASH_ERASED, int),
    (PhsenAbcdefImodemDataParticleKey.POWER_ON_INVALID, int),
    (PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS, int),
    (PhsenAbcdefImodemDataParticleKey.NUM_ERROR_RECORDS, int),
    (PhsenAbcdefImodemDataParticleKey.NUM_BYTES_STORED, int)
]

# Encoding rules for the Metadata particle
METADATA_PARTICLE_ENCODING_RULES = [
    (PhsenAbcdefImodemDataParticleKey.FILE_TIME, str),
    (PhsenAbcdefImodemDataParticleKey.INSTRUMENT_ID, str),
    (PhsenAbcdefImodemDataParticleKey.SERIAL_NUMBER, str),
    (PhsenAbcdefImodemDataParticleKey.VOLTAGE_FLT32, float),
    (PhsenAbcdefImodemDataParticleKey.NUM_DATA_RECORDS, int),
    (PhsenAbcdefImodemDataParticleKey.RECORD_LENGTH, int),
    (PhsenAbcdefImodemDataParticleKey.NUM_EVENTS, int),
    (PhsenAbcdefImodemDataParticleKey.NUM_SAMPLES, int)
]


class PhsenAbcdefImodemScienceBaseDataParticle(DataParticle):
    """
    BaseDataParticle class for Science records, which are pH records or control records.
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = []

        # Process each of science (instrument and control) particle parameters
        for key, encoding_function in SCIENCE_PARTICLE_ENCODING_RULES:
            particle_params.append(self._encode_value(key, self.raw_data[key], encoding_function))

        return particle_params


class PhsenAbcdefImodemInstrumentDataParticle(PhsenAbcdefImodemScienceBaseDataParticle):
    """
    Class for the instrument particle.
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(PhsenAbcdefImodemInstrumentDataParticle, self)._build_parsed_values()

        # Process each of instrument data particle parameters
        for key, encoding_function in INSTRUMENT_DATA_PARTICLE_ENCODING_RULES:
            particle_params.append(self._encode_value(key, self.raw_data[key], encoding_function))

        return particle_params


class PhsenAbcdefImodemControlDataParticle(PhsenAbcdefImodemScienceBaseDataParticle):
    """
    Class for the control particle.
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(PhsenAbcdefImodemControlDataParticle, self)._build_parsed_values()

        # Process each of control data particle parameters
        for key, encoding_function in CONTROL_DATA_PARTICLE_ENCODING_RULES:
            particle_params.append(self._encode_value(key, self.raw_data[key], encoding_function))

        # battery voltage is optional in control record.
        if self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY]:
            particle_params.append(
                self._encode_value(PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                                   self.raw_data[PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY],
                                   int))
        else:
            particle_params.append(
                {DataParticleKey.VALUE_ID: PhsenAbcdefImodemDataParticleKey.VOLTAGE_BATTERY,
                 DataParticleKey.VALUE: None})
        return particle_params


class PhsenAbcdefImodemMetadataDataParticle(DataParticle):
    """
    Class for the metadata particle.
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = []

        # Process each of metadata particle parameters
        for key, encoding_function in METADATA_PARTICLE_ENCODING_RULES:

            key_value = self.raw_data.get(key, None)
            log.trace("key: %s, key_value: %s", key, key_value)
            if key_value is None:
                particle_params.append({DataParticleKey.VALUE_ID: key,
                                        DataParticleKey.VALUE: None})

            else:
                particle_params.append(self._encode_value(key,
                                                          self.raw_data[key],
                                                          encoding_function))

        return particle_params


class PhsenAbcdefImodemInstrumentTelemeteredDataParticle(PhsenAbcdefImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_INSTRUMENT


class PhsenAbcdefImodemInstrumentRecoveredDataParticle(PhsenAbcdefImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_INSTRUMENT_RECOVERED


class PhsenAbcdefImodemControlTelemeteredDataParticle(PhsenAbcdefImodemControlDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_CONTROL


class PhsenAbcdefImodemControlRecoveredDataParticle(PhsenAbcdefImodemControlDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_CONTROL_RECOVERED


class PhsenAbcdefImodemMetadataTelemeteredDataParticle(PhsenAbcdefImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_METADATA


class PhsenAbcdefImodemMetadataRecoveredDataParticle(PhsenAbcdefImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PHSEN_ABCDEF_IMODEM_METADATA_RECOVERED
