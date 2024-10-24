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
    PLIMS_A_HDR_TELEMETERED_PARTICLE_TYPE = 'plims_a_hdr_instrument'
    PLIMS_A_HDR_RECOVERED_PARTICLE_TYPE = 'plims_a_hdr_instrument_recovered'
    PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE = 'plims_a_adc_instrument'
    PLIMS_A_ADC_RECOVERED_PARTICLE_TYPE = 'plims_a_adc_instrument_recovered'

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
    Class that defines fields that need to be extracted for the data particle.
    Data types the same in telemetered and recovered data streams.
    """

    TRIGGER_NUMBER = "trigger_number" #int
    ADC_TIME = "ADCtime" #Elapsed time (in seconds) from the start of the sample run to the current trigger.
    PMTA = "PMTA" # Integrated output (in volts)
    PMTB = "PMTB" # Integrated output (in volts)
    PMTC = "PMTC" # Integrated output (in volts)
    PMTD = "PMTD" # Integrated output (in volts)
    PEAK_A = "PeakA" # Peak output (in volts)
    PEAK_B = "PeakB" # Peak output (in volts)
    PEAK_C = "PeakC" # Peak output (in volts)
    PEAK_D = "PeakD" # Peak output (in volts)
    TIME_OF_FLIGHT = "TimeOfFlight" #Duration (in us) of the entire pulse for which a trigger signal is generated
    GRAB_TIME_START = "GrabTimeStart" # duplicate of ADCtime
    GRAB_TIME_END = "GrabTimeEnd"
    ROI_X = "RoiX"#int
    ROI_Y = "RoiY"#int
    ROI_WIDTH = "RoiWidth"#int
    ROI_HEIGHT = "RoiHeight"#int
    START_BYTE= "StartByte"#int
    COMPARATOR_OUT = "ComparatorOut" #int
    START_POINT = "StartPoint" #int
    SIGNAL_LENGTH = "SignalLength" #int
    STATUS = "Status" #int
    RUN_TIME = "RunTime"
    INHIBIT_TIME = "InhibitTime"

PLIMS_A_ADC_COLUMNS = [
    "trigger_number",
    "ADCtime",
    "PMTA",
    "PMTB",
    "PMTC",
    "PMTD",
    "PeakA",
    "PeakB",
    "PeakC",
    "PeakD",
    "TimeOfFlight",
    "GrabTimeStart",
    "GrabTimeEnd",
    "RoiX",
    "RoiY",
    "RoiWidth",
    "RoiHeight",
    "StartByte",
    "ComparatorOut",
    "StartPoint",
    "SignalLength",
    "Status",
    "RunTime",
    "InhibitTime",
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
