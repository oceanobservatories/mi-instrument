"""
@author Joffrey Peters
@brief Particles for the plims_a recovered and telemetered datasets.

This file contains particle data type definitions for the PLIMS A instrument.
"""

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey


class DataParticleType(BaseEnum):
    """ Use same streams for telemetered and recovered data. """
    PLIMS_A_HDR_INSTRUMENT_TELEMETERED_PARTICLE_TYPE = 'plims_a_hdr_instrument'
    PLIMS_A_HDR_INSTRUMENT_RECOVERED_PARTICLE_TYPE = 'plims_a_hdr_instrument'
    PLIMS_A_HDR_ENGINEERING_TELEMETERED_PARTICLE_TYPE = 'plims_a_hdr_engineering'
    PLIMS_A_HDR_ENGINEERING_RECOVERED_PARTICLE_TYPE = 'plims_a_hdr_engineering'
    PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE = 'plims_a_adc_instrument'
    PLIMS_A_ADC_RECOVERED_PARTICLE_TYPE = 'plims_a_adc_instrument'

    __metaclass__ = get_logging_metaclass(log_level='trace')


class PlimsAParticleKey(BaseEnum):
    """
    Base class for PLIMS A HDR particles.
    Instrument and Engineering particles extend this class.
    """
    SAMPLE_TIMESTAMP = 'sample_timestamp'
    PARAMETER_NAME_MAP = {}

class PlimsAHdrInstrumentParticleKey(PlimsAParticleKey):
    """
    Class defining fields to be extracted for the instrument data particle.
    Data types the same in telemetered and recovered particles.
    """

    SAMPLE_NUMBER = 'sample_number'
    SAMPLE_TYPE = 'sample_type'
    TRIGGER_COUNT = 'trigger_count'
    ROI_COUNT = 'roi_count'
    HUMIDITY = 'humidity'
    RUNTIME = 'run_time'
    INHIBIT_TIME = 'inhibit_time'
    BINARIZE_THRESHOLD = 'binarize_threshold'
    MINIMUM_BLOB_AREA = 'minimum_blob_area'
    BLOB_XGROW_AMOUNT = 'blob_xgrow_amount'
    BLOB_YGROW_AMOUNT = 'blob_ygrow_amount'
    MINIMUM_GAP_BETWEEN_ADJACENT_BLOBS = 'minimum_gap_between_adjacent_blobs'
    PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY = 'pmta_trigger_threshold_daq_mcconly'
    PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY = 'pmtb_trigger_threshold_daq_mcconly'
    PMT_TRIGGER_SELECTION_DAQ_MCCONLY = 'pmt_trigger_selection_daq_mcconly'
    PMTA_HIGH_VOLTAGE = 'pmta_high_voltage'
    PMTB_HIGH_VOLTAGE = 'pmtb_high_voltage'
    SYRINGE_SAMPLING_SPEED = 'syringe_sampling_speed'
    SYRINGE_SAMPLE_VOLUME = 'syringe_sample_volume'
    RUN_SAMPLE_FAST = 'run_sample_fast'
    RUN_FAST_FACTOR = 'run_fast_factor'
    COUNTER_CLEANING = 'counter_cleaning'
    COUNTER_BEADS = 'counter_beads'


    PARAMETER_NAME_MAP = {
        "sampleNumber": SAMPLE_NUMBER,
        "sampleType": SAMPLE_TYPE,
        "triggerCount": TRIGGER_COUNT,
        "roiCount": ROI_COUNT,
        "humidity": HUMIDITY,
        "runTime": RUNTIME,
        "inhibitTime": INHIBIT_TIME,
        "binarizeThreshold": BINARIZE_THRESHOLD,
        "minimumBlobArea": MINIMUM_BLOB_AREA,
        "blobXgrowAmount": BLOB_XGROW_AMOUNT,
        "blobYgrowAmount": BLOB_YGROW_AMOUNT,
        "minimumGapBewtweenAdjacentBlobs": MINIMUM_GAP_BETWEEN_ADJACENT_BLOBS,
        "PMTAtriggerThreshold_DAQ_MCConly": PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY,
        "PMTBtriggerThreshold_DAQ_MCConly": PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY,
        "PMTtriggerSelection_DAQ_MCConly": PMT_TRIGGER_SELECTION_DAQ_MCCONLY,
        "PMTAhighVoltage": PMTA_HIGH_VOLTAGE,
        "PMTBhighVoltage": PMTB_HIGH_VOLTAGE,
        "syringeSamplingSpeed": SYRINGE_SAMPLING_SPEED,
        "SyringeSampleVolume": SYRINGE_SAMPLE_VOLUME,
        "runSampleFast": RUN_SAMPLE_FAST,
        "RunFastFactor": RUN_FAST_FACTOR,
        "CounterCleaning": COUNTER_CLEANING,
        "CounterBeads": COUNTER_BEADS,
    }

class PlimsAHdrEngineeringParticleKey(PlimsAParticleKey):
    """
    Class that defining data fields to be extracted for the engineering data particle.
    Data types the same for telemetered and recovered particles.
    """

    TEMPERATURE = 'temperature'
    ADC_FILE_FORMAT = 'adc_file_format'
    AUTO_START = 'auto_start'
    AUTO_SHUTDOWN = 'auto_shutdown'
    HUMIDITY_ALARM_THRESHOLD = 'humidity_alarm_threshold'
    FLASHLAMP_CONTROL_VOLTAGE = 'flashlamp_control_voltage'
    HK_TRIGGER_TO_FLASHLAMP_DELAY_TIME_X434NS_DAC_MCCONLY = 'hk_trigger_to_flashlamp_delay_time_x434ns_dac_mcconly'
    LASER_STATE = 'laser_state'
    RUNNING_CAMERA = 'running_camera'
    PUMP1_STATE = 'pump1_state'
    PUMP2_STATE = 'pump2_state'
    STIRRER = 'stirrer'
    TRIGGER_CONTINUOUS = 'trigger_continuous'
    ALT_PMT_TRIGGER_SELECTION_DAQ_MCCONLY = 'alt_pmt_trigger_selection_daq_mcconly'
    INTERVAL_BETWEEN_ALTERNATE_HARDWARE_SETTINGS = 'interval_between_alternate_hardware_settings'
    ALT_FLASHLAMP_CONTROL_VOLTAGE = 'alt_flashlamp_control_voltage'
    PUMP_DRIVE_VOLTAGE = 'pump_drive_voltage'
    ALT_PMTA_HIGH_VOLTAGE = 'alt_pmta_high_voltage'
    ALT_PMTB_HIGH_VOLTAGE = 'alt_pmtb_high_voltage'
    ALT_PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY = 'alt_pmta_trigger_threshold_daq_mcconly'
    ALT_PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY = 'alt_pmtb_trigger_threshold_daq_mcconly'
    NUMBER_SYRINGES_TO_AUTORUN = 'number_syringes_to_autorun'
    ALT_SYRINGE_SAMPLE_VOLUME = 'alt_syringe_sample_volume'
    SAMPLE_VOLUME_2_SKIP = 'sample_volume_2_skip'
    BLEACH_VOLUME = 'bleach_volume'
    BIOCIDE_VOLUME = 'biocide_volume'
    DEBUBBLE_WITH_SAMPLE = 'debubble_with_sample'
    REFILL_AFTER_DEBUBBLE = 'refill_after_debubble'
    PRIME_SAMPLE_TUBE = 'prime_sample_tube'
    BACKFLUSH_WITH_SAMPLE = 'backflush_with_sample'
    RUN_BEADS = 'run_beads'
    BEADS_SAMPLE_VOLUME = 'beads_sample_volume'
    NUMBER_SYRINGES_BETWEEN_BEADS_RUN = 'number_syringes_between_beads_run'
    NUMBER_SYRINGES_BETWEEN_CLEANING_RUN = 'number_syringes_between_cleaning_run'
    FOCUS_MOTOR_SMALL_STEP_MS = 'focus_motor_small_step_ms'
    FOCUS_MOTOR_LARGE_STEP_MS = 'focus_motor_large_step_ms'
    LASER_MOTOR_SMALL_STEP_MS = 'laser_motor_small_step_ms'
    LASER_MOTOR_LARGE_STEP_MS = 'laser_motor_large_step_ms'
    BLEACH_RINSE_COUNT = 'bleach_rinse_count'
    BLEACH_RINSE_VOLUME = 'bleach_rinse_volume'
    BLEACH_TO_EXHAUST = 'bleach_to_exhaust'
    COUNTER_ALT = 'counter_alt'


    PARAMETER_NAME_MAP = {
        "temperature": TEMPERATURE,
        "ADCFileFormat": ADC_FILE_FORMAT,
        "autoStart": AUTO_START,
        "autoShutdown": AUTO_SHUTDOWN,
        "HumidityAlarmThreshold(%)": HUMIDITY_ALARM_THRESHOLD,
        "FlashlampControlVoltage": FLASHLAMP_CONTROL_VOLTAGE,
        "HKTRIGGERtoFlashlampDelayTime_x434ns_DAC_MCConly": HK_TRIGGER_TO_FLASHLAMP_DELAY_TIME_X434NS_DAC_MCCONLY,
        "laserState": LASER_STATE,
        "runningCamera": RUNNING_CAMERA,
        "pump1State": PUMP1_STATE,
        "pump2State": PUMP2_STATE,
        "stirrer": STIRRER,
        "triggerContinuous": TRIGGER_CONTINUOUS,
        "altPMTtriggerSelection_DAQ_MCConly": ALT_PMT_TRIGGER_SELECTION_DAQ_MCCONLY,
        "intervalBetweenAlternateHardwareSettings": INTERVAL_BETWEEN_ALTERNATE_HARDWARE_SETTINGS,
        "Alt_FlashlampControlVoltage": ALT_FLASHLAMP_CONTROL_VOLTAGE,
        "pumpDriveVoltage": PUMP_DRIVE_VOLTAGE,
        "altPMTAHighVoltage": ALT_PMTA_HIGH_VOLTAGE,
        "altPMTBHighVoltage": ALT_PMTB_HIGH_VOLTAGE,
        "altPMTATriggerThreshold_DAQ_MCConly": ALT_PMTA_TRIGGER_THRESHOLD_DAQ_MCCONLY,
        "altPMTBTriggerThreshold_DAQ_MCConly": ALT_PMTB_TRIGGER_THRESHOLD_DAQ_MCCONLY,
        "NumberSyringesToAutoRun": NUMBER_SYRINGES_TO_AUTORUN,
        "altSyringeSampleVolume": ALT_SYRINGE_SAMPLE_VOLUME,
        "sampleVolume2skip": SAMPLE_VOLUME_2_SKIP,
        "BleachVolume": BLEACH_VOLUME,
        "BiocideVolume": BIOCIDE_VOLUME,
        "debubbleWithSample": DEBUBBLE_WITH_SAMPLE,
        "RefillAfterDebubble": REFILL_AFTER_DEBUBBLE,
        "primeSampleTube": PRIME_SAMPLE_TUBE,
        "backflushWithSample": BACKFLUSH_WITH_SAMPLE,
        "runBeads": RUN_BEADS,
        "BeadsSampleVolume": BEADS_SAMPLE_VOLUME,
        "NumberSyringesBetweenBeadsRun": NUMBER_SYRINGES_BETWEEN_BEADS_RUN,
        "NumberSyringesBetweenCleaningRun": NUMBER_SYRINGES_BETWEEN_CLEANING_RUN,
        "focusMotorSmallStep_ms": FOCUS_MOTOR_SMALL_STEP_MS,
        "focusMotorLargeStep_ms": FOCUS_MOTOR_LARGE_STEP_MS,
        "laserMotorSmallStep_ms": LASER_MOTOR_SMALL_STEP_MS,
        "laserMotorLargeStep_ms": LASER_MOTOR_LARGE_STEP_MS,
        "BleachRinseCount": BLEACH_RINSE_COUNT,
        "BleachRinseVolume": BLEACH_RINSE_VOLUME,
        "BleachToExhaust": BLEACH_TO_EXHAUST,
        "CounterAlt": COUNTER_ALT,
    }

class PlimsAAdcParticleKey(PlimsAParticleKey):
    """
    Class defining fields to be extracted for the data particle.
    Data types the same in telemetered and recovered particles.
    """

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


class PlimsAHdrClassKey(BaseEnum):
    ENGINEERING = 'engineering_data'
    INSTRUMENT = 'instrument_data'
class PlimsAHdrInstrumentDataParticle(PlimsADataParticle, PlimsAHdrInstrumentParticleKey):
    _data_particle_type = DataParticleType.PLIMS_A_HDR_INSTRUMENT_TELEMETERED_PARTICLE_TYPE

class PlimsAHdrEngineeringDataParticle(PlimsADataParticle, PlimsAHdrEngineeringParticleKey):
    _data_particle_type = DataParticleType.PLIMS_A_HDR_ENGINEERING_TELEMETERED_PARTICLE_TYPE

class PlimsAAdcDataParticle(PlimsADataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_ADC_TELEMETERED_PARTICLE_TYPE

class PlimsAAdcRecoveredDataParticle(PlimsADataParticle):
    _data_particle_type = DataParticleType.PLIMS_A_ADC_RECOVERED_PARTICLE_TYPE
