"""
@author Joffrey Peters
@brief Particles for the plims_a recovered and telemetered datasets.

This file contains particle data type definitions for the PLIMS A instrument.
"""

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import (DataParticle,
                                                      DataParticleKey)


class DataParticleType(BaseEnum):
    """ Use same streams for telemetered and recovered data. """
    PLIMS_A_HDR_TELEMETERED_PARTICLE_TYPE = 'plims_a_hdr_instrument'
    PLIMS_A_HDR_RECOVERED_PARTICLE_TYPE = 'plims_a_hdr_instrument'
    PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE = 'plims_a_adc_instrument'
    PLIMS_A_ADC_RECOVERED_PARTICLE_TYPE = 'plims_a_adc_instrument'

    __metaclass__ = get_logging_metaclass(log_level='trace')

class PlimsAHdrParticleKey(BaseEnum):
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


class PlimsAAdcParticleKey(BaseEnum):
    """
    Class defining fields to be extracted for the data particle.
    Data types the same in telemetered and recovered particles.
    """

    FILE_TIME = 'file_time'
    TRIGGER_NUMBER = "trigger_number" #int
    PMTA = "pmta" # Integrated output (in volts)
    PMTB = "pmtb" # Integrated output (in volts)
    PEAK_A = "peak_a" # Peak output (in volts)
    PEAK_B = "peak_b" # Peak output (in volts)
    TIME_OF_FLIGHT = "time_of_flight" #Duration (in us) of the entire pulse for which a trigger signal is generated
    GRAB_TIME_START = "grab_time_start" # duplicate of adc_time
    GRAB_TIME_END = "grab_time_end"
    ROI_X = "roi_x"#int
    ROI_Y = "roi_y"#int
    ROI_WIDTH = "roi_width"#int
    ROI_HEIGHT = "roi_height"#int
    START_BYTE= "start_byte"#int
    STATUS = "status" #int
    RUN_TIME = "run_time"
    INHIBIT_TIME = "inhibit_time"

    PLIMS_A_ADC_COLUMNS = [
        "trigger_number",
        "adc_time",
        "pmta",
        "pmtb",
        "pmtc",
        "pmtd",
        "peak_a",
        "peak_b",
        "peak_c",
        "peak_d",
        "time_of_flight",
        "grab_time_start",
        "grab_time_end",
        "roi_x",
        "roi_y",
        "roi_width",
        "roi_height",
        "start_byte",
        "comparator_out",
        "start_point",
        "signal_length",
        "status",
        "run_time",
        "inhibit_time",
    ]

    PLIMS_A_ADC_DROP_COLUMNS = [
        "adc_time",
        "pmtc",
        "pmtd",
        "peak_c",
        "peak_d",
        "comparator_out",
        "start_point",
        "signal_length",
    ]


class PlimsADataParticle(DataParticle):

    def _build_parsed_values(self):
        """
        Build parsed values for Instrument Data Particle.
        @return: list containing type encoded "particle value id:value" dictionary pairs
        """

        return [{DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: None}
                if self.raw_data[name] is None else
                {DataParticleKey.VALUE_ID: name, DataParticleKey.VALUE: value}
                for name, value in self.raw_data.iteritems()]

class PlimsAHdrDataParticle(PlimsADataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_HDR_TELEMETERED_PARTICLE_TYPE

class PlimsAAdcDataParticle(PlimsADataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE

class PlimsAAdcRecoveredDataParticle(PlimsADataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_ADC_RECOVERED_PARTICLE_TYPE
