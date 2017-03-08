__author__ = 'mworden'

"""
@package mi.dataset.parser.nutnr_b_particles
@file mi/dataset/parser/nutnr_b_particles.py
@author Mark Worden
@brief Parser for the nutnr_b_particles dataset driver

This file contains the particles that are applicable
nutnr_b_dcl_conc, nutnr_b_dcl_full and nutnr_b.
"""

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum

from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue


class DataParticleType(BaseEnum):
    NUTNR_B_DCL_CONC_INSTRUMENT_RECOVERED = 'nutnr_b_dcl_conc_instrument_recovered'
    NUTNR_B_DCL_DARK_CONC_INSTRUMENT_RECOVERED = 'nutnr_b_dcl_dark_conc_instrument_recovered'
    NUTNR_B_DCL_CONC_METADATA_RECOVERED = 'nutnr_b_dcl_conc_metadata_recovered'
    NUTNR_B_DCL_CONC_INSTRUMENT = 'nutnr_b_dcl_conc_instrument'
    NUTNR_B_DCL_DARK_CONC_INSTRUMENT = 'nutnr_b_dcl_dark_conc_instrument'
    NUTNR_B_DCL_CONC_METADATA = 'nutnr_b_dcl_conc_metadata'
    NUTNR_B_INSTRUMENT_RECOVERED = 'nutnr_b_instrument_recovered'
    NUTNR_B_DARK_INSTRUMENT_RECOVERED = 'nutnr_b_dark_instrument_recovered'
    NUTNR_B_METADATA_RECOVERED = 'nutnr_b_metadata_recovered'
    NUTNR_B_DCL_FULL_INSTRUMENT = 'nutnr_b_dcl_full_instrument'
    NUTNR_B_DCL_DARK_FULL_INSTRUMENT = 'nutnr_b_dcl_dark_full_instrument'
    NUTNR_B_DCL_FULL_INSTRUMENT_RECOVERED = 'nutnr_b_dcl_full_instrument_recovered'
    NUTNR_B_DCL_DARK_FULL_INSTRUMENT_RECOVERED = 'nutnr_b_dcl_dark_full_instrument_recovered'
    NUTNR_B_DCL_FULL_METADATA = 'nutnr_b_dcl_full_metadata'
    NUTNR_B_DCL_FULL_METADATA_RECOVERED = 'nutnr_b_dcl_full_metadata_recovered'


class NutnrBDataParticleKey(BaseEnum):
    STARTUP_TIME = 'startup_time'                                   # PD334
    SPEC_ON_TIME = 'spec_on_time'                                   # PD348
    SPEC_POWERED_TIME = 'spec_powered_time'                         # PD349
    LAMP_ON_TIME = 'lamp_on_time'                                   # PD350
    LAMP_POWERED_TIME = 'lamp_powered_time'                         # PD351
    FRAME_HEADER = 'frame_header'                                   # PD310
    FRAME_TYPE = 'frame_type'                                       # PD311
    SERIAL_NUMBER = 'serial_number'                                 # PD312
    DATE_OF_SAMPLE = 'date_of_sample'                               # PD313
    TIME_OF_SAMPLE = 'time_of_sample'                               # PD314
    NITRATE_CONCENTRATION = 'nitrate_concentration'                 # PD315
    AUX_FITTING_1 = 'aux_fitting_1'                                 # PD316
    AUX_FITTING_2 = 'aux_fitting_2'                                 # PD317
    AUX_FITTING_3 = 'aux_fitting_3'                                 # PD318
    RMS_ERROR = 'rms_error'                                         # PD319
    TEMP_INTERIOR = 'temp_interior'                                 # PD320
    TEMP_SPECTROMETER = 'temp_spectrometer'                         # PD321
    TEMP_LAMP = 'temp_lamp'                                         # PD322
    LAMP_TIME = 'lamp_time'                                         # PD347
    HUMIDITY = 'humidity'                                           # PD324
    VOLTAGE_LAMP = 'voltage_lamp'                                   # PD325
    VOLTAGE_ANALOG = 'voltage_analog'                               # PD326
    VOLTAGE_MAIN = 'voltage_main'                                   # PD327
    REF_CHANNEL_AVERAGE = 'ref_channel_average'                     # PD328
    REF_CHANNEL_VARIANCE = 'ref_channel_variance'                   # PD329
    SEA_WATER_DARK = 'sea_water_dark'                               # PD330
    SPEC_CHANNEL_AVERAGE = 'spec_channel_average'                   # PD331
    SPECTRAL_CHANNELS = 'spectral_channels'                         # PD332 (used in light frame particles)
    DARK_FRAME_SPECTRAL_CHANNELS = 'dark_frame_spectral_channels'   # PD3799 (used in dark frame particles)
    DATA_LOG_FILE = 'data_log_file'                                 # PD352
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'           # PD2605
    STARTUP_TIME_STRING = 'startup_time_string'                     # PD2707
    FIRMWARE_VERSION = 'firmware_version'                           # PD113
    FIRMWARE_DATE = 'firmware_date'                                 # PD293


class NutnrBMetadataRecoveredDataParticle(DataParticle):
    """
    Class for generating the nutnr b metadata recovered particle.
    """
    _data_particle_type = DataParticleType.NUTNR_B_METADATA_RECOVERED

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Metadata Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        values = []

        for name, value, function in self.raw_data:
            if value is not None:
                values.append(self._encode_value(name, value, function))
            else:
                values.append({DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None})

        return values


class NutnrBInstrumentRecoveredDataParticle(DataParticle):
    """
    Class for generating the nutnr b instrument recovered particle.
    """
    _data_particle_type = DataParticleType.NUTNR_B_INSTRUMENT_RECOVERED

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, value, function)
                for name, value, function in self.raw_data]


class NutnrBDarkInstrumentRecoveredDataParticle(DataParticle):
    """
    Class for generating the nutnr b instrument recovered particle.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DARK_INSTRUMENT_RECOVERED

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, value, function)
                for name, value, function in self.raw_data]


class NutnrBDclInstrumentDataParticle(DataParticle):
    """
    Class for generating the nutnr b dcl instrument particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(NutnrBDclInstrumentDataParticle, self).__init__(raw_data,
                                                              port_timestamp,
                                                              internal_timestamp,
                                                              preferred_timestamp,
                                                              quality_flag,
                                                              new_sequence)

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, value, function)
                for name, value, function in self.raw_data]


class NutnrBDclMetadataDataParticle(DataParticle):
    """
    Class for generating the nutnr_b_dcl Metadata particle.
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(NutnrBDclMetadataDataParticle, self).__init__(raw_data,
                                                            port_timestamp,
                                                            internal_timestamp,
                                                            preferred_timestamp,
                                                            quality_flag,
                                                            new_sequence)

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered and Telemetered Metadata Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Metadata Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into raw_data and a function to use for data conversion.

        return [self._encode_value(name, value, function)
                for name, value, function in self.raw_data]


class NutnrBDclConcRecoveredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_CONC_INSTRUMENT_RECOVERED


class NutnrBDclDarkConcRecoveredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_DARK_CONC_INSTRUMENT_RECOVERED


class NutnrBDclConcTelemeteredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_CONC_INSTRUMENT


class NutnrBDclDarkConcTelemeteredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_DARK_CONC_INSTRUMENT


class NutnrBDclConcRecoveredMetadataDataParticle(NutnrBDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_CONC_METADATA_RECOVERED


class NutnrBDclConcTelemeteredMetadataDataParticle(NutnrBDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_CONC_METADATA


class NutnrBDclFullRecoveredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_FULL_INSTRUMENT_RECOVERED


class NutnrBDclDarkFullRecoveredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_DARK_FULL_INSTRUMENT_RECOVERED


class NutnrBDclFullTelemeteredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_FULL_INSTRUMENT


class NutnrBDclDarkFullTelemeteredInstrumentDataParticle(NutnrBDclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_DARK_FULL_INSTRUMENT


class NutnrBDclFullRecoveredMetadataDataParticle(NutnrBDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_FULL_METADATA_RECOVERED


class NutnrBDclFullTelemeteredMetadataDataParticle(NutnrBDclMetadataDataParticle):
    """
    Class for generating Metadata Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.NUTNR_B_DCL_FULL_METADATA