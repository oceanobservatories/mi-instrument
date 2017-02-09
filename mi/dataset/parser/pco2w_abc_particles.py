
__author__ = 'Mark Worden'

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.parser.utilities import mac_timestamp_to_utc_timestamp


class Pco2wAbcParticleClassKey (BaseEnum):
    """
    An enum for the keys application to the pco2w abc particle classes
    """
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'
    POWER_PARTICLE_CLASS = 'power_particle_class'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'
    INSTRUMENT_BLANK_PARTICLE_CLASS = 'instrument_blank_particle_class'
    CONTROL_PARTICLE_CLASS = 'control_particle_class'


class DataParticleType(BaseEnum):
    PCO2W_ABC_INSTRUMENT = 'pco2w_abc_instrument'
    PCO2W_ABC_INSTRUMENT_BLANK = 'pco2w_abc_instrument_blank'
    PCO2W_ABC_METADATA = 'pco2w_abc_metadata'
    PCO2W_ABC_POWER = 'pco2w_abc_power'
    PCO2W_ABC_DCL_INSTRUMENT = 'pco2w_abc_dcl_instrument'
    PCO2W_ABC_DCL_INSTRUMENT_RECOVERED = 'pco2w_abc_dcl_instrument_recovered'
    PCO2W_ABC_DCL_INSTRUMENT_BLANK = 'pco2w_abc_dcl_instrument_blank'
    PCO2W_ABC_DCL_INSTRUMENT_BLANK_RECOVERED = 'pco2w_abc_dcl_instrument_blank_recovered'
    PCO2W_ABC_DCL_METADATA = 'pco2w_abc_dcl_metadata'
    PCO2W_ABC_DCL_METADATA_RECOVERED = 'pco2w_abc_dcl_metadata_recovered'
    PCO2W_ABC_DCL_POWER = 'pco2w_abc_dcl_power'
    PCO2W_ABC_DCL_POWER_RECOVERED = 'pco2w_abc_dcl_power_recovered'
    PCO2W_ABC_IMODEM_INSTRUMENT = 'pco2w_abc_imodem_instrument'
    PCO2W_ABC_IMODEM_INSTRUMENT_RECOVERED = 'pco2w_abc_imodem_instrument_recovered'
    PCO2W_ABC_IMODEM_INSTRUMENT_BLANK = 'pco2w_abc_imodem_instrument_blank'
    PCO2W_ABC_IMODEM_INSTRUMENT_BLANK_RECOVERED = 'pco2w_abc_imodem_instrument_blank_recovered'
    PCO2W_ABC_IMODEM_CONTROL = 'pco2w_abc_imodem_control'
    PCO2W_ABC_IMODEM_CONTROL_RECOVERED = 'pco2w_abc_imodem_control_recovered'
    PCO2W_ABC_IMODEM_POWER = 'pco2w_abc_imodem_power'
    PCO2W_ABC_IMODEM_POWER_RECOVERED = 'pco2w_abc_imodem_power_recovered'
    PCO2W_ABC_IMODEM_METADATA = 'pco2w_abc_imodem_metadata'
    PCO2W_ABC_IMODEM_METADATA_RECOVERED = 'pco2w_abc_imodem_metadata_recovered'


class Pco2wAbcDataParticleKey(BaseEnum):
    RECORD_TYPE = 'record_type'                             # PD355
    RECORD_TIME = 'record_time'                             # PD356
    LIGHT_MEASUREMENTS = 'light_measurements'               # PD357
    VOLTAGE_BATTERY = 'voltage_battery'                     # PD358
    THERMISTOR_RAW = 'thermistor_raw'                       # PD359
    BLANK_LIGHT_MEASUREMENTS = 'blank_light_measurements'   # PD2712
    CLOCK_ACTIVE = 'clock_active'                           # PD366
    RECORDING_ACTIVE = 'recording_active'                   # PD367
    RECORD_END_ON_TIME = 'record_end_on_time'               # PD368
    RECORD_MEMORY_FULL = 'record_memory_full'               # PD369
    RECORD_END_ON_ERROR = 'record_end_on_error'             # PD370
    DATA_DOWNLOAD_OK = 'data_download_ok'                   # PD371
    FLASH_MEMORY_OPEN = 'flash_memory_open'                 # PD372
    BATTERY_LOW_PRESTART = 'battery_low_prestart'           # PD373
    BATTERY_LOW_MEASUREMENT = 'battery_low_measurement'     # PD374
    BATTERY_LOW_BLANK = 'battery_low_blank'                 # PD375
    BATTERY_LOW_EXTERNAL = 'battery_low_external'           # PD376
    EXTERNAL_DEVICE1_FAULT = 'external_device1_fault'       # PD377
    EXTERNAL_DEVICE2_FAULT = 'external_device2_fault'       # PD1113
    EXTERNAL_DEVICE3_FAULT = 'external_device3_fault'       # PD1114
    FLASH_ERASED = 'flash_erased'                           # PD378
    POWER_ON_INVALID = 'power_on_invalid'                   # PD379
    NUM_DATA_RECORDS = 'num_data_records'                   # PD1115
    NUM_ERROR_RECORDS = 'num_error_records'                 # PD1116
    NUM_BYTES_STORED = 'num_bytes_stored'                   # PD1117
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'   # PD2605
    UNIQUE_ID = 'unique_id'                                 # PD353
    PASSED_CHECKSUM = 'passed_checksum'                     # PD2228
    FILE_TIME = 'file_time'                                 # PD3060
    INSTRUMENT_ID = 'instrument_id'                         # PD1089
    SERIAL_NUMBER = 'serial_number'                         # PD312
    VOLTAGE_FLT32 = 'voltage_flt32'                         # PD2649
    RECORD_LENGTH = 'record_length'                         # PD583
    NUM_EVENTS = 'num_events'                               # PD263
    NUM_SAMPLES = 'num_samples'                             # PD203


class Pco2wAbcBaseDataParticle(DataParticle):

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_parameters = list()

        particle_parameters.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORD_TYPE,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORD_TYPE],
                               int))
        particle_parameters.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORD_TIME,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORD_TIME],
                               int))

        return particle_parameters


class Pco2wAbcDclBaseDataParticle(Pco2wAbcBaseDataParticle):

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_parameters = super(Pco2wAbcDclBaseDataParticle, self)._build_parsed_values()

        particle_parameters.append(
            self._encode_value(Pco2wAbcDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
                               self.raw_data[Pco2wAbcDataParticleKey.DCL_CONTROLLER_TIMESTAMP],
                               str))
        particle_parameters.append(
            self._encode_value(Pco2wAbcDataParticleKey.UNIQUE_ID,
                               self.raw_data[Pco2wAbcDataParticleKey.UNIQUE_ID],
                               int))
        particle_parameters.append(
            self._encode_value(Pco2wAbcDataParticleKey.PASSED_CHECKSUM,
                               self.raw_data[Pco2wAbcDataParticleKey.PASSED_CHECKSUM],
                               int))

        return particle_parameters


class Pco2wAbcInstrumentBaseDataParticle(Pco2wAbcBaseDataParticle):

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(Pco2wAbcInstrumentBaseDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                               self.raw_data[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.THERMISTOR_RAW,
                               self.raw_data[Pco2wAbcDataParticleKey.THERMISTOR_RAW],
                               int))

        return particle_params


class Pco2wAbcInstrumentDataParticle(Pco2wAbcInstrumentBaseDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_INSTRUMENT

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(Pco2wAbcInstrumentDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS,
                               self.raw_data[Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS],
                               list))

        return particle_params


class Pco2wAbcInstrumentBlankDataParticle(Pco2wAbcInstrumentBaseDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_INSTRUMENT_BLANK

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(Pco2wAbcInstrumentBlankDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS,
                               self.raw_data[Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS],
                               list))

        return particle_params


class Pco2wAbcMetadataDataParticle(Pco2wAbcBaseDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_METADATA

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        particle_params = super(Pco2wAbcMetadataDataParticle, self)._build_parsed_values()

        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.CLOCK_ACTIVE,
                               self.raw_data[Pco2wAbcDataParticleKey.CLOCK_ACTIVE],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORDING_ACTIVE,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORDING_ACTIVE],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORD_END_ON_TIME,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORD_END_ON_TIME],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR,
                               self.raw_data[Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK,
                               self.raw_data[Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN,
                               self.raw_data[Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART,
                               self.raw_data[Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT,
                               self.raw_data[Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK,
                               self.raw_data[Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL,
                               self.raw_data[Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT,
                               self.raw_data[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT,
                               self.raw_data[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT,
                               self.raw_data[Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.FLASH_ERASED,
                               self.raw_data[Pco2wAbcDataParticleKey.FLASH_ERASED],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.POWER_ON_INVALID,
                               self.raw_data[Pco2wAbcDataParticleKey.POWER_ON_INVALID],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.NUM_DATA_RECORDS,
                               self.raw_data[Pco2wAbcDataParticleKey.NUM_DATA_RECORDS],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS,
                               self.raw_data[Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS],
                               int))
        particle_params.append(
            self._encode_value(Pco2wAbcDataParticleKey.NUM_BYTES_STORED,
                               self.raw_data[Pco2wAbcDataParticleKey.NUM_BYTES_STORED],
                               int))

        if self.raw_data[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY]:
            particle_params.append(
                self._encode_value(Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                                   self.raw_data[Pco2wAbcDataParticleKey.VOLTAGE_BATTERY],
                                   int))
        else:
            particle_params.append(
                {DataParticleKey.VALUE_ID: Pco2wAbcDataParticleKey.VOLTAGE_BATTERY,
                 DataParticleKey.VALUE: None})

        return particle_params


class Pco2wAbcPowerDataParticle(Pco2wAbcBaseDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_POWER

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        return super(Pco2wAbcPowerDataParticle, self)._build_parsed_values()


class Pco2wAbcDclInstrumentDataParticle(Pco2wAbcInstrumentDataParticle, Pco2wAbcDclBaseDataParticle):

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        return super(Pco2wAbcDclInstrumentDataParticle, self)._build_parsed_values()


class Pco2wAbcDclInstrumentTelemeteredDataParticle(Pco2wAbcDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_INSTRUMENT


class Pco2wAbcDclInstrumentRecoveredDataParticle(Pco2wAbcDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_INSTRUMENT_RECOVERED


class Pco2wAbcDclInstrumentBlankDataParticle(Pco2wAbcInstrumentBlankDataParticle, Pco2wAbcDclBaseDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_INSTRUMENT_BLANK

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        return super(Pco2wAbcDclInstrumentBlankDataParticle, self)._build_parsed_values()


class Pco2wAbcDclInstrumentBlankTelemeteredDataParticle(Pco2wAbcDclInstrumentBlankDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_INSTRUMENT_BLANK


class Pco2wAbcDclInstrumentBlankRecoveredDataParticle(Pco2wAbcDclInstrumentBlankDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_INSTRUMENT_BLANK_RECOVERED


class Pco2wAbcDclMetadataDataParticle(Pco2wAbcMetadataDataParticle, Pco2wAbcDclBaseDataParticle):

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        return super(Pco2wAbcDclMetadataDataParticle, self)._build_parsed_values()


class Pco2wAbcDclMetadataTelemeteredDataParticle(Pco2wAbcDclMetadataDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_METADATA


class Pco2wAbcDclMetadataRecoveredDataParticle(Pco2wAbcDclMetadataDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_METADATA_RECOVERED


class Pco2wAbcDclPowerDataParticle(Pco2wAbcPowerDataParticle, Pco2wAbcDclBaseDataParticle):

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """

        return super(Pco2wAbcDclPowerDataParticle, self)._build_parsed_values()


class Pco2wAbcDclPowerTelemeteredDataParticle(Pco2wAbcDclPowerDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_POWER


class Pco2wAbcDclPowerRecoveredDataParticle(Pco2wAbcDclPowerDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_DCL_POWER_RECOVERED


class Pco2wAbcImodemDataParticle(DataParticle):

    _encoding_rules = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """
        result = []

        for key in self.raw_data.keys():

            if key == Pco2wAbcDataParticleKey.RECORD_TIME:
                unix_time = mac_timestamp_to_utc_timestamp(self.raw_data[key])
                self.set_internal_timestamp(unix_time=unix_time)

            if self.raw_data[key] is None:

                result.append({DataParticleKey.VALUE_ID: key,
                               DataParticleKey.VALUE: None})
            else:

                result.append(
                    self._encode_value(key,
                                       self.raw_data[key],
                                       self._encoding_rules[key]))

        return result


class Pco2wAbcImodemInstrumentDataParticle(Pco2wAbcImodemDataParticle):

    _encoding_rules = {
        Pco2wAbcDataParticleKey.UNIQUE_ID: int,
        Pco2wAbcDataParticleKey.RECORD_TYPE: int,
        Pco2wAbcDataParticleKey.RECORD_TIME: int,
        Pco2wAbcDataParticleKey.LIGHT_MEASUREMENTS: list,
        Pco2wAbcDataParticleKey.VOLTAGE_BATTERY: int,
        Pco2wAbcDataParticleKey.THERMISTOR_RAW: int,
        Pco2wAbcDataParticleKey.PASSED_CHECKSUM: int,
    }


class Pco2wAbcImodemInstrumentTelemeteredDataParticle(Pco2wAbcImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_INSTRUMENT


class Pco2wAbcImodemInstrumentRecoveredDataParticle(Pco2wAbcImodemInstrumentDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_INSTRUMENT_RECOVERED


class Pco2wAbcImodemInstrumentBlankDataParticle(Pco2wAbcImodemDataParticle):

    _encoding_rules = {
        Pco2wAbcDataParticleKey.UNIQUE_ID: int,
        Pco2wAbcDataParticleKey.RECORD_TYPE: int,
        Pco2wAbcDataParticleKey.RECORD_TIME: int,
        Pco2wAbcDataParticleKey.BLANK_LIGHT_MEASUREMENTS: list,
        Pco2wAbcDataParticleKey.VOLTAGE_BATTERY: int,
        Pco2wAbcDataParticleKey.THERMISTOR_RAW: int,
        Pco2wAbcDataParticleKey.PASSED_CHECKSUM: int,
    }


class Pco2wAbcImodemInstrumentBlankTelemeteredDataParticle(
        Pco2wAbcImodemInstrumentBlankDataParticle):

    _data_particle_type = \
        DataParticleType.PCO2W_ABC_IMODEM_INSTRUMENT_BLANK


class Pco2wAbcImodemInstrumentBlankRecoveredDataParticle(
        Pco2wAbcImodemInstrumentBlankDataParticle):

    _data_particle_type = \
        DataParticleType.PCO2W_ABC_IMODEM_INSTRUMENT_BLANK_RECOVERED


class Pco2wAbcImodemControlDataParticle(Pco2wAbcImodemDataParticle):

    _encoding_rules = {
        Pco2wAbcDataParticleKey.UNIQUE_ID: int,
        Pco2wAbcDataParticleKey.RECORD_TYPE: int,
        Pco2wAbcDataParticleKey.RECORD_TIME: int,
        Pco2wAbcDataParticleKey.CLOCK_ACTIVE: int,
        Pco2wAbcDataParticleKey.RECORDING_ACTIVE: int,
        Pco2wAbcDataParticleKey.RECORD_END_ON_TIME: int,
        Pco2wAbcDataParticleKey.RECORD_MEMORY_FULL: int,
        Pco2wAbcDataParticleKey.RECORD_END_ON_ERROR: int,
        Pco2wAbcDataParticleKey.DATA_DOWNLOAD_OK: int,
        Pco2wAbcDataParticleKey.FLASH_MEMORY_OPEN: int,
        Pco2wAbcDataParticleKey.BATTERY_LOW_PRESTART: int,
        Pco2wAbcDataParticleKey.BATTERY_LOW_MEASUREMENT: int,
        Pco2wAbcDataParticleKey.BATTERY_LOW_BLANK: int,
        Pco2wAbcDataParticleKey.BATTERY_LOW_EXTERNAL: int,
        Pco2wAbcDataParticleKey.EXTERNAL_DEVICE1_FAULT: int,
        Pco2wAbcDataParticleKey.EXTERNAL_DEVICE2_FAULT: int,
        Pco2wAbcDataParticleKey.EXTERNAL_DEVICE3_FAULT: int,
        Pco2wAbcDataParticleKey.FLASH_ERASED: int,
        Pco2wAbcDataParticleKey.POWER_ON_INVALID: int,
        Pco2wAbcDataParticleKey.NUM_DATA_RECORDS: int,
        Pco2wAbcDataParticleKey.NUM_ERROR_RECORDS: int,
        Pco2wAbcDataParticleKey.NUM_BYTES_STORED: int,
        Pco2wAbcDataParticleKey.VOLTAGE_BATTERY: int,
        Pco2wAbcDataParticleKey.PASSED_CHECKSUM: int,
    }


class Pco2wAbcImodemControlTelemeteredDataParticle(Pco2wAbcImodemControlDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_CONTROL


class Pco2wAbcImodemControlRecoveredDataParticle(Pco2wAbcImodemControlDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_CONTROL_RECOVERED


class Pco2wAbcImodemPowerDataParticle(Pco2wAbcImodemDataParticle):

    _encoding_rules = {
        Pco2wAbcDataParticleKey.UNIQUE_ID: int,
        Pco2wAbcDataParticleKey.RECORD_TYPE: int,
        Pco2wAbcDataParticleKey.RECORD_TIME: int,
        Pco2wAbcDataParticleKey.PASSED_CHECKSUM: int,
    }


class Pco2wAbcImodemPowerTelemeteredDataParticle(Pco2wAbcImodemPowerDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_POWER


class Pco2wAbcImodemPowerRecoveredDataParticle(Pco2wAbcImodemPowerDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_POWER_RECOVERED


class Pco2wAbcImodemMetadataDataParticle(Pco2wAbcImodemDataParticle):

    _encoding_rules = {
        Pco2wAbcDataParticleKey.FILE_TIME: str,
        Pco2wAbcDataParticleKey.INSTRUMENT_ID: str,
        Pco2wAbcDataParticleKey.SERIAL_NUMBER: str,
        Pco2wAbcDataParticleKey.VOLTAGE_FLT32: float,
        Pco2wAbcDataParticleKey.NUM_DATA_RECORDS: int,
        Pco2wAbcDataParticleKey.RECORD_LENGTH: int,
        Pco2wAbcDataParticleKey.NUM_EVENTS: int,
        Pco2wAbcDataParticleKey.NUM_SAMPLES: int,
    }


class Pco2wAbcImodemMetadataTelemeteredDataParticle(Pco2wAbcImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_METADATA


class Pco2wAbcImodemMetadataRecoveredDataParticle(Pco2wAbcImodemMetadataDataParticle):

    _data_particle_type = DataParticleType.PCO2W_ABC_IMODEM_METADATA_RECOVERED