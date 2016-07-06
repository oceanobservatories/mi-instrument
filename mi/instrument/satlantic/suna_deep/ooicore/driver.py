"""
@package mi.instrument.satlantic.suna_deep.ooicore.driver
@file marine-integrations/mi/instrument/satlantic/suna_deep/ooicore/driver.py
@author Rachel Manoni
@brief Driver for the ooicore
Release notes:

initial_rev
"""
import re
import time
import datetime

from mi.core.common import BaseEnum, Units, Prefixes
from mi.core.log import get_logger, get_logging_metaclass

from mi.core.instrument.instrument_protocol import CommandResponseInstrumentProtocol, InitializationType
from mi.core.instrument.instrument_fsm import ThreadSafeFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictType

from mi.core.exceptions import SampleException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentStateException
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.time_tools import get_timestamp_delayed

log = get_logger()

__author__ = 'Rachel Manoni'
__license__ = 'Apache 2.0'

# newline.
NEWLINE = '\r\n'

# default timeout.
DEFAULT_TIMEOUT = 15

ACQUIRE_SAMPLE_TIMEOUT = 40
ACQUIRE_STATUS_TIMEOUT = 30
MEASURE_N_TIMEOUT = 65
MEASURE_N_CMD_TIMEOUT = 60
MEASURE_0_TIMEOUT = 40
TIMED_N_TIMEOUT = 65
TIMED_N_CMD_TIMEOUT = 60
CLOCK_SYNC_TIMEOUT = 20
DISCOVER_TIMEOUT = 40
STOP_PERIODIC_TIMEOUT = 30
STOP_AUTOSAMPLE_TIMEOUT = 40

MIN_TIME_SAMPLE = 0
MIN_LIGHT_SAMPLE = 1

MAX_TIME_SAMPLE = 30
MAX_LIGHT_SAMPLE = 40

# SUNA ASCII FRAME REGEX
SUNA_SAMPLE_PATTERN = r'SAT'  # Sentinal
SUNA_SAMPLE_PATTERN += r'([A-Z]{3})'  # 1: Frame Type (string)
SUNA_SAMPLE_PATTERN += r'(\d{4}),'  # 2: Serial Number (int)
SUNA_SAMPLE_PATTERN += r'(\d{7}),'  # 3: Date, year and day-of-year (int)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 4. Time, hours of day (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 5. Nitrate concentration [uM] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 6. Nitrogen in nitrate [mg/l] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 7. Absorbance at 254 nm (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 8. Absorbance at 350 nm (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 9. Bromide trace [mg/l] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*),'  # 10. Spectrum average (int)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*),'  # 11. Dark value used for fit (int)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*),'  # 12. Integration time factor (int)
SUNA_SAMPLE_PATTERN += r'('  # 13. Spectrum channels (open group)
for i in range(255):
    SUNA_SAMPLE_PATTERN += r'[+-]?\d*,'  # 14. Spectrum channels (255 x int)
SUNA_SAMPLE_PATTERN += r'[+-]?\d*),'  # 15. Spectrum channels (close group, last int = 256th)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 16. Internal temperature [C] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 17. Spectrometer temperature [C] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 18. Lamp temperature [C] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*),'  # 19. Cumulative lamp on-time [s] (int)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 20. Relative Humidity [%] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 21. Main Voltage [V] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 22. Lamp Voltage [V] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 23. Internal Voltage [V] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 24. Main Current [mA] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 25. Fit Aux 1 (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 26. Fit Aux 2 (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 27. Fit Base 1 (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 28. Fit Base 2 (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*.\d*),'  # 29. Fit RMSE (float)
SUNA_SAMPLE_PATTERN += r','  # 30. CTD Time [seconds since 1970] (int)
SUNA_SAMPLE_PATTERN += r','  # 31. CTD Salinity [PSU] (float)
SUNA_SAMPLE_PATTERN += r','  # 32. CTD Temperature [C] (float)
SUNA_SAMPLE_PATTERN += r','  # 33. CTD Pressure [dBar] (float)
SUNA_SAMPLE_PATTERN += r'([+-]?\d*)'  # 34. Check Sum (int)
SUNA_SAMPLE_PATTERN += r'\r\n'  # <Carriage Return> <Line Feed>

SUNA_SAMPLE_REGEX = re.compile(SUNA_SAMPLE_PATTERN)

# SUNA STATUS REGEX
SUNA_STATUS_PATTERN = r'SENSTYPE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'SENSVERS\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'SERIALNO\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'THEBRAND\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'PATHLGTH\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'INTWIPER\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'EXTPPORT\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:LMPSHUTR\s+(\w+)\s+)?'  # 8
SUNA_STATUS_PATTERN += r'(?:REFDTECT\s+(\w+)\s+)?'  # 9
SUNA_STATUS_PATTERN += r'(?:PROTECTR\s+(\w+)\s+)?'  # 10
SUNA_STATUS_PATTERN += r'SUPRCAPS\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'PWRSVISR\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'USBSWTCH\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'RELAYBRD\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'SDI12BRD\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'ANALGBRD\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'INTDATLG\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'APFIFACE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'SCHDLING\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:FANATLMP\s+(\w+)\s+)?'  # 20
SUNA_STATUS_PATTERN += r'OWIRETLP\s+([0-9a-f]+)\s+'
SUNA_STATUS_PATTERN += r'OWIRETSP\s+([0-9a-f]+)\s+'
SUNA_STATUS_PATTERN += r'OWIRETHS\s+([0-9a-f]+)\s+'
SUNA_STATUS_PATTERN += r'ZSPEC_SN\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'FIBERLSN\s+([\w.]+)\s+'
SUNA_STATUS_PATTERN += r'(?:CUSTOMID\s+\w+\s+)?'  # w+ not captured
SUNA_STATUS_PATTERN += r'STUPSTUS\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'BAUDRATE\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'MSGLEVEL\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'MSGFSIZE\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DATFSIZE\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'OUTFRTYP\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'LOGFRTYP\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'OUTDRKFR\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'LOGDRKFR\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:TIMERESL\s+(\w+)\s+)?'  #35
SUNA_STATUS_PATTERN += r'LOGFTYPE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:AFILEDUR\s+\d+\s+)?'  # d+ not captured
SUNA_STATUS_PATTERN += r'ACQCOUNT\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'CNTCOUNT\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DCMINNO3\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'DCMAXNO3\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'WDAT_LOW\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'WDAT_HGH\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'SDI12ADD\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DATAMODE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'OPERMODE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'OPERCTRL\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'EXDEVTYP\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'EXDEVPRE\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'EXDEVRUN\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:EXDVIVAL\s+\d+\s+)?'  # d+ not captured
SUNA_STATUS_PATTERN += r'(?:WATCHDOG\s+(\w+)\s+)?'  # 50
SUNA_STATUS_PATTERN += r'COUNTDWN\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'FIXDDURA\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'PERDIVAL\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'PERDOFFS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'PERDDURA\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'PERDSMPL\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'POLLTOUT\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'APFATOFF\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'STBLTIME\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'(?:REFLIMIT\s+(\d+)\s+)?'  # 60
SUNA_STATUS_PATTERN += r'SKPSLEEP\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'LAMPTOFF\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'SPINTPER\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DRKAVERS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'LGTAVERS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'(?:REFSMPLS\s+(\d+)\s+)?'  # 66
SUNA_STATUS_PATTERN += r'DRKSMPLS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'LGTSMPLS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DRKDURAT\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'LGTDURAT\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'TEMPCOMP\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'SALINFIT\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'BRMTRACE\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'BL_ORDER\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'FITCONCS\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'DRKCORMT\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'(?:DRKCOEFS\s+(\w+)\s+)?'  # 77
SUNA_STATUS_PATTERN += r'(?:DAVGPRM0\s+([+-]?\d+.\d+)\s+)?'  # 78
SUNA_STATUS_PATTERN += r'(?:DAVGPRM1\s+([+-]?\d+.\d+)\s+)?'  # 79
SUNA_STATUS_PATTERN += r'(?:DAVGPRM2\s+([+-]?\d+.\d+)\s+)?'  # 80
SUNA_STATUS_PATTERN += r'(?:DAVGPRM3\s+([+-]?\d+.\d+)\s+)?'  # 81
SUNA_STATUS_PATTERN += r'A_CUTOFF\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'INTPRADJ\s+(\w+)\s+'
SUNA_STATUS_PATTERN += r'INTPRFAC\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'INTADSTP\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'INTADMAX\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'WFIT_LOW\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'WFIT_HGH\s+([+-]?\d+.\d+)\s+'
SUNA_STATUS_PATTERN += r'LAMPTIME\s+(\d+)\s+'
SUNA_STATUS_PATTERN += r'.*?Ok\s+(\S*\.cal)'

SUNA_STATUS_REGEX = re.compile(SUNA_STATUS_PATTERN, re.DOTALL)

# SUNA TEST REGEX
SUNA_TEST_PATTERN = r'Extrn Disk Size; Free , (\d+); (\d+)\s+'
SUNA_TEST_PATTERN += r'Intrn Disk Size; Free , (\d+); (\d+)\s+'
SUNA_TEST_PATTERN += r'Fiberlite\s+Odometer , (\d+:\d+:\d+)\s+'
SUNA_TEST_PATTERN += r'Temperatures Hs Sp Lm , ([+-]?\d+.\d+) ([+-]?\d+.\d+) ([+-]?\d+.\d+)\s+'
SUNA_TEST_PATTERN += r'Humidity\s+, ([+-]?\d+.\d+)\s+'
SUNA_TEST_PATTERN += r'Electrical Mn Bd Pr C , ([+-]?\d+.\d+) ([+-]?\d+.\d+) ([+-]?\d+.\d+) ([+-]?\d+.\d+)\s+'
SUNA_TEST_PATTERN += r'Lamp\s+Power , (\d+) mW\s+'
SUNA_TEST_PATTERN += r'Spec Dark av sd mi ma ,\s+(\d+) \(\+/-\s+(\d+)\) \[\s*(\d+):\s*(\d+)\]\s+'
SUNA_TEST_PATTERN += r'Spec Lght av sd mi ma ,\s+(\d+) \(\+/-\s+(\d+)\) \[\s*(\d+):\s*(\d+)\]\s+'
SUNA_TEST_PATTERN += r'\$(Ok|Error)'

SUNA_TEST_REGEX = re.compile(SUNA_TEST_PATTERN)


###
#    Driver Constant Definitions
###
class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    SUNA_SAMPLE = "nutnr_a_sample"
    SUNA_DARK_SAMPLE = "nutnr_a_dark_sample"
    SUNA_STATUS = "nutnr_a_status"
    SUNA_TEST = "nutnr_a_test"


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    PERIODIC = 'DRIVER_STATE_PERIODIC'
    MEASURING_N = 'DRIVER_STATE_MEASURING_N'
    MEASURING_TIMED_N = 'DRIVER_STATE_MEASURING_TIMED_N'


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    DISCOVER = DriverEvent.DISCOVER
    INITIALIZE = DriverEvent.INITIALIZE
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    START_PERIODIC = "DRIVER_EVENT_START_PERIODIC"
    STOP_PERIODIC = "DRIVER_EVENT_STOP_PERIODIC"
    TEST = DriverEvent.TEST
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT
    CLOCK_SYNC = DriverEvent.CLOCK_SYNC
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    MEASURE_N = "DRIVER_EVENT_MEASURE_N"
    MEASURE_N_ASYNC = "DRIVER_EVENT_MEASURE_N_ASYNC"
    MEASURE_0 = "DRIVER_EVENT_MEASURE_0"
    TIMED_N = "DRIVER_EVENT_TIMED_N"
    TIMED_N_ASYNC = "DRIVER_EVENT_TIMED_N_ASYNC"
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    # Get Sample & Status Data
    ACQUIRE_SAMPLE = ProtocolEvent.ACQUIRE_SAMPLE
    ACQUIRE_STATUS = ProtocolEvent.ACQUIRE_STATUS
    MEASURE_N = ProtocolEvent.MEASURE_N
    MEASURE_0 = ProtocolEvent.MEASURE_0
    TIMED_N = ProtocolEvent.TIMED_N
    TEST = ProtocolEvent.TEST

    # Change States
    START_AUTOSAMPLE = ProtocolEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = ProtocolEvent.STOP_AUTOSAMPLE
    START_PERIODIC = ProtocolEvent.START_PERIODIC
    STOP_PERIODIC = ProtocolEvent.STOP_PERIODIC
    DISCOVER = ProtocolEvent.DISCOVER

    # Parameter Accessors/Mutators
    GET = ProtocolEvent.GET
    SET = ProtocolEvent.SET

    START_DIRECT = ProtocolEvent.START_DIRECT
    STOP_DIRECT = ProtocolEvent.STOP_DIRECT

    CLOCK_SYNC = ProtocolEvent.CLOCK_SYNC


class Parameter(DriverParameter):
    # Data Acquisition
    OPERATION_MODE = "opermode"
    OPERATION_CONTROL = "operctrl"
    LIGHT_SAMPLES = "lgtsmpls"
    DARK_SAMPLES = "drksmpls"
    LIGHT_DURATION = "lgtdurat"
    DARK_DURATION = "drkdurat"
    COUNTDOWN = "countdwn"

    # Data Processing
    TEMP_COMPENSATION = "tempcomp"
    FIT_WAVELENGTH_LOW = "wfit_low"  # read/get only
    FIT_WAVELENGTH_HIGH = "wfit_hgh"  # read/get only
    FIT_WAVELENGTH_BOTH = "wfitboth"  # set only DO NOT USE, COMBINE LOW/HIGH FOR EASIER OPERATOR USE
    CONCENTRATIONS_IN_FIT = "fitconcs"
    BASELINE_ORDER = "bl_order"
    DARK_CORRECTION_METHOD = "drkcormt"
    SALINITY_FITTING = "salinfit"
    BROMIDE_TRACING = "brmtrace"
    ABSORBANCE_CUTOFF = "a_cutoff"
    INTEG_TIME_ADJUSTMENT = "intpradj"
    INTEG_TIME_FACTOR = "intprfac"
    INTEG_TIME_STEP = "intadstp"
    INTEG_TIME_MAX = "intadmax"
    PERIODIC_INTERVAL = "perdival"
    PERIODIC_OFFSET = "perdoffs"
    PERIODIC_DURATION = "perddura"
    PERIODIC_SAMPLES = "perdsmpl"

    # Driver Parameters
    NUM_LIGHT_SAMPLES = "nmlgtspl"
    TIME_LIGHT_SAMPLE = "tlgtsmpl"

    # Data Acquisition
    REF_MIN_AT_LAMP_ON = "reflimit"  # read only
    SPECTROMETER_INTEG_PERIOD = "spintper"  # read only

    # Data Acquisition
    POLLED_TIMEOUT = "polltout"
    SKIP_SLEEP_AT_START = "skpsleep"
    LAMP_STABIL_TIME = "stbltime"
    LAMP_SWITCH_OFF_TEMPERATURE = "lamptoff"

    # I/O
    MESSAGE_LEVEL = "msglevel"
    MESSAGE_FILE_SIZE = "msgfsize"
    DATA_FILE_SIZE = "datfsize"
    OUTPUT_FRAME_TYPE = "outfrtyp"
    OUTPUT_DARK_FRAME = "outdrkfr"


class Prompt(BaseEnum):
    """
    Device I/O prompts..
    """
    COMMAND_LINE = "SUNA>"
    POLLED = "CMD?"
    OK = '$Ok'
    ERROR = '$Error:'
    WAKEUP = "Charging power loss protector."
    SAMPLING = 'SAT'


OK_GET = r'.*\r\n\$Ok ([\w.]+)\s+'
OK_GET_REGEX = re.compile(OK_GET, re.DOTALL)


class InstrumentCommands(BaseEnum):
    """
    Instrument command strings
    """
    # Status and Maintenance
    CMD_LINE = "$"
    SET_CLOCK = "set clock"
    EXIT = "exit"
    SELFTEST = "selftest"
    STATUS = "get cfg"
    GET_CAL_FILE = "get activecalfile"

    # Polled Mode
    MEASURE = "Measure"  # takes param n indicating amount of light frames
    TIMED = "Timed"  # takes param n indicating duration in seconds to take light frames for
    SLEEP = "Sleep"

    # Command Line Commands
    GET = "get"  # takes param that indicates which field to get
    SET = "set"  # takes params that indicate which field to set and what value to set it to


class InstrumentCommandNames(BaseEnum):
    """
    Instrument command strings
    """
    # Status and Maintenance
    CMD_LINE = 'Command Line'
    SET_CLOCK = 'Set Clock>'
    EXIT = 'Exit'
    SELFTEST = 'Self Test'
    STATUS = 'Get Config'
    GET_CAL_FILE = 'Get Calibration'

    # Polled Mode
    MEASURE = 'Sample # Frames>'
    TIMED = 'Sample for Duration>'
    SLEEP = 'Sleep'

    # Command Line Commands
    GET = 'Get>'
    SET = 'Set>'


class InstrumentCommandArgs(BaseEnum):
    POLLED = 'Polled'
    CONTINUOUS = 'Continuous'
    PERIODIC = 'Periodic'
    ON = 'On'
    OFF = 'Off'


class SUNASampleDataParticleKey(BaseEnum):
    FRAME_TYPE = "frame_type"
    SERIAL_NUM = "serial_number"
    SAMPLE_DATE = "date_of_sample"
    SAMPLE_TIME = "time_of_sample"
    NITRATE_CONCEN = "nitrate_concentration"
    NITROGEN = "nutnr_nitrogen_in_nitrate"
    ABSORB_254 = "nutnr_absorbance_at_254_nm"
    ABSORB_350 = "nutnr_absorbance_at_350_nm"
    BROMIDE_TRACE = "nutnr_bromide_trace"
    SPECTRUM_AVE = "nutnr_spectrum_average"
    FIT_DARK_VALUE = "nutnr_dark_value_used_for_fit"
    TIME_FACTOR = "nutnr_integration_time_factor"
    SPECTRAL_CHANNELS = "spectral_channels"
    TEMP_SPECTROMETER = "temp_spectrometer"
    TEMP_INTERIOR = "temp_interior"
    TEMP_LAMP = "temp_lamp"
    LAMP_TIME = "lamp_time"
    HUMIDITY = "humidity"
    VOLTAGE_MAIN = "voltage_main"
    VOLTAGE_LAMP = "voltage_lamp"
    VOLTAGE_INT = "nutnr_voltage_int"
    CURRENT_MAIN = "nutnr_current_main"
    FIT_1 = "aux_fitting_1"
    FIT_2 = "aux_fitting_2"
    FIT_BASE_1 = "nutnr_fit_base_1"
    FIT_BASE_2 = "nutnr_fit_base_2"
    FIT_RMSE = "nutnr_fit_rmse"
    CHECKSUM = "checksum"


###############################################################################
# Data Particles
###############################################################################
class SUNASampleDataParticle(DataParticle):
    _data_particle_type = DataParticleType.SUNA_SAMPLE

    def _build_parsed_values(self, timex=None):
        matched = SUNA_SAMPLE_REGEX.match(self.raw_data)

        if not matched:
            raise SampleException("No regex match for sample [%s]" % self.raw_data)

        # 'SDF', 'SDC', 'SDB', 'SDR' are dark samples
        if str(matched.group(1)).startswith('D', 1):
            self._data_particle_type = DataParticleType.SUNA_DARK_SAMPLE

        try:
            parsed_data_list = [
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FRAME_TYPE,
                 DataParticleKey.VALUE: str(matched.group(1))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.SERIAL_NUM,
                 DataParticleKey.VALUE: str(matched.group(2))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.SAMPLE_DATE,
                 DataParticleKey.VALUE: int(matched.group(3))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.SAMPLE_TIME,
                 DataParticleKey.VALUE: float(matched.group(4))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.NITRATE_CONCEN,
                 DataParticleKey.VALUE: float(matched.group(5))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.NITROGEN,
                 DataParticleKey.VALUE: float(matched.group(6))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.ABSORB_254,
                 DataParticleKey.VALUE: float(matched.group(7))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.ABSORB_350,
                 DataParticleKey.VALUE: float(matched.group(8))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.BROMIDE_TRACE,
                 DataParticleKey.VALUE: float(matched.group(9))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.SPECTRUM_AVE,
                 DataParticleKey.VALUE: int(matched.group(10))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_DARK_VALUE,
                 DataParticleKey.VALUE: int(matched.group(11))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.TIME_FACTOR,
                 DataParticleKey.VALUE: int(matched.group(12))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.SPECTRAL_CHANNELS,
                 DataParticleKey.VALUE: [int(s) for s in matched.group(13).split(',')]},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.TEMP_SPECTROMETER,
                 DataParticleKey.VALUE: float(matched.group(14))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.TEMP_INTERIOR,
                 DataParticleKey.VALUE: float(matched.group(15))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.TEMP_LAMP,
                 DataParticleKey.VALUE: float(matched.group(16))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.LAMP_TIME,
                 DataParticleKey.VALUE: int(matched.group(17))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.HUMIDITY,
                 DataParticleKey.VALUE: float(matched.group(18))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.VOLTAGE_MAIN,
                 DataParticleKey.VALUE: float(matched.group(19))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.VOLTAGE_LAMP,
                 DataParticleKey.VALUE: float(matched.group(20))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.VOLTAGE_INT,
                 DataParticleKey.VALUE: float(matched.group(21))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.CURRENT_MAIN,
                 DataParticleKey.VALUE: float(matched.group(22))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_1,
                 DataParticleKey.VALUE: float(matched.group(23))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_2,
                 DataParticleKey.VALUE: float(matched.group(24))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_BASE_1,
                 DataParticleKey.VALUE: float(matched.group(25))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_BASE_2,
                 DataParticleKey.VALUE: float(matched.group(26))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.FIT_RMSE,
                 DataParticleKey.VALUE: float(matched.group(27))},
                {DataParticleKey.VALUE_ID: SUNASampleDataParticleKey.CHECKSUM,
                 DataParticleKey.VALUE: int(matched.group(28))}]

            date = datetime.datetime.strptime(matched.group(3), '%Y%j')

            secs = (date - datetime.datetime(1900, 1, 1)).total_seconds()
            secs += float(matched.group(4)) * 3600
            self.set_internal_timestamp(secs)

        except ValueError:
            raise SampleException("ValueError while parsing data [%s]" % self.raw_data)

        log.debug('SUNASampleDataParticle raw data: %r', self.raw_data)
        log.debug('SUNASampleDataParticle parsed data: %r', parsed_data_list)

        return parsed_data_list


class SUNAStatusDataParticleKey(BaseEnum):
    SENSOR_TYPE = "nutnr_sensor_type"
    SENSOR_VERSION = "nutnr_sensor_version"
    SERIAL_NUMBER = "serial_number"
    INTEGRATED_WIPER = "nutnr_integrated_wiper"
    EXT_POWER_PORT = "nutnr_ext_power_port"
    LAMP_SHUTTER = "nutnr_lamp_shutter"
    REF_DETECTOR = "nutnr_reference_detector"
    PROTECTR = "nutnr_wiper_protector"
    SUPER_CAPACITORS = "nutnr_super_capacitors"
    PSB_SUPERVISOR = "nutnr_psb_supervisor"
    USB_COMM = "nutnr_usb_communication"
    RELAY_MODULE = "nutnr_relay_module"
    SDII2_INTERFACE = "nutnr_sdi12_interface"
    ANALOG_OUTPUT = "nutnr_analog_output"
    DATA_LOGGING = "nutnr_int_data_logging"
    APF_INTERFACE = "nutnr_apf_interface"
    SCHEDULING = "nutnr_scheduling"
    LAMP_FAN = "nutnr_lamp_fan"
    ADDR_LAMP_TEMP = "nutnr_sensor_address_lamp_temp"
    ADDR_SPEC_TEMP = "nutnr_sensor_address_spec_temp"
    SENSOR_ADDR_HOUS_TEMP = "nutnr_sensor_address_hous_temp"
    SERIAL_NUM_SPECT = "nutnr_serial_number_spec"
    SERIAL_NUM_LAMP = "nutnr_serial_number_lamp"
    STUPSTUS = "nutnr_stupstus"
    BAUD_RATE = "baud_rate"
    MSG_LEVEL = "nutnr_msg_level"
    MSG_FILE_SIZE = "nutnr_msg_file_size"
    DATA_FILE_SIZE = "nutnr_data_file_size"
    OUTPUT_FRAME_TYPE = "nutnr_output_frame_type"
    LOGGING_FRAME_TYPE = "nutnr_logging_frame_type"
    OUTPUT_DARK_FRAME = "nutnr_output_dark_frame"
    LOGGING_DARK_FRAME = "nutnr_logging_dark_frame"
    TIMERESL = "nutnr_timeresl"
    LOG_FILE_TYPE = "nutnr_log_file_type"
    ACQCOUNT = "nutnr_acqcount"
    CNTCOUNT = "nutnr_cntcount"
    NITRATE_MIN = "nutnr_dac_nitrate_min"
    NITRATE_MAX = "nutnr_dac_nitrate_max"
    WAVELENGTH_LOW = "nutnr_data_wavelength_low"
    WAVELENGTH_HIGH = "nutnr_data_wavelength_high"
    SDI12_ADDR = "nutnr_sdi12_address"
    DATAMODE = "nutnr_data_mode"
    OPERATING_MODE = "nutnr_operating_mode"
    OPERATION_CTRL = "nutnr_operation_ctrl"
    EXTL_DEV = "nutnr_extl_dev"
    PRERUN_TIME = "nutnr_ext_dev_prerun_time"
    DEV_DURING_ACQ = "nutnr_ext_dev_during_acq"
    WATCHDOG_TIME = "nutnr_watchdog_timer"
    COUNTDOWN = "nutnr_countdown"
    FIXED_TIME = "nutnr_fixed_time_duration"
    PERIODIC_INTERVAL = "nutnr_periodic_interval"
    PERIODIC_OFFSET = "nutnr_periodic_offset"
    PERIODIC_DURATION = "nutnr_periodic_duration"
    PERIODIC_SAMPLES = "nutnr_periodic_samples"
    POLLED_TIMEOUT = "nutnr_polled_timeout"
    APF_TIMEOUT = "nutnr_apf_timeout"
    STABILITY_TIME = "nutnr_lamp_stability_time"
    MIN_LAMP_ON = "nutnr_ref_min_lamp_on"
    SKIP_SLEEP = "nutnr_skip_sleep"
    SWITCHOFF_TEMP = "nutnr_lamp_switchoff_temp"
    SPEC_PERIOD = "nutnr_spec_integration_period"
    DRKAVERS = "nutnr_dark_avg"
    LGTAVERS = "nutnr_light_avg"
    REFSAMPLES = "nutnr_reference_samples"
    DARK_SAMPLES = "nutnr_dark_samples"
    LIGHT_SAMPLES = "nutnr_light_samples"
    DARK_DURATION = "nutnr_dark_duration"
    LIGHT_DURATION = "nutnr_light_duration"
    TEMP_COMP = "nutnr_temp_comp"
    SALINITY_FIT = "nutnr_salinity_fit"
    BROMIDE_TRACING = "nutnr_bromide_tracing"
    BASELINE_ORDER = "nutnr_baseline_order"
    CONCENTRATIONS_FIT = "nutnr_concentrations_fit"
    DARK_CORR_METHOD = "nutnr_dark_corr_method"
    DRKCOEFS = "nutnr_dark_coefs"
    DAVGPRM_0 = "nutnr_davgprm0"
    DAVGPRM_1 = "nutnr_davgprm1"
    DAVGPRM_2 = "nutnr_davgprm2"
    DAVGPRM_3 = "nutnr_davgprm3"
    ABSORBANCE_CUTOFF = "nutnr_absorbance_cutoff"
    TIME_ADJ = "nutnr_int_time_adj"
    TIME_FACTOR = "nutnr_int_time_factor"
    TIME_STEP = "nutnr_int_time_step"
    TIME_MAX = "nutnr_int_time_max"
    FIT_WAVE_LOW = "nutnr_fit_wavelength_low"
    FIT_WAVE_HIGH = "nutnr_fit_wavelength_high"
    LAMP_TIME = "nutnr_lamp_time"
    CALIBRATION_FILE = "nutnr_activecalfile"


class SUNAStatusDataParticle(DataParticle):
    _data_particle_type = DataParticleType.SUNA_STATUS

    def _none_check(self, func, x):
        if x is None:
            return None

        return func(x)

    def _build_parsed_values(self):
        matched = SUNA_STATUS_REGEX.match(self.raw_data)

        log.debug('SUNAStatusDataParticle raw data: %r', self.raw_data)

        if not matched:
            raise SampleException("No regex match for status [%s]" % self.raw_data)
        try:
            parsed_data_list = [
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SENSOR_TYPE,
                 DataParticleKey.VALUE: str(matched.group(1))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SENSOR_VERSION,
                 DataParticleKey.VALUE: str(matched.group(2))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SERIAL_NUMBER,
                 DataParticleKey.VALUE: str(matched.group(3))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.INTEGRATED_WIPER,
                 DataParticleKey.VALUE: str(matched.group(6))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.EXT_POWER_PORT,
                 DataParticleKey.VALUE: str(matched.group(7))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LAMP_SHUTTER,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(8))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.REF_DETECTOR,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(9))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PROTECTR,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(10))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SUPER_CAPACITORS,
                 DataParticleKey.VALUE: str(matched.group(11))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PSB_SUPERVISOR,
                 DataParticleKey.VALUE: str(matched.group(12))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.USB_COMM,
                 DataParticleKey.VALUE: str(matched.group(13))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.RELAY_MODULE,
                 DataParticleKey.VALUE: str(matched.group(14))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SDII2_INTERFACE,
                 DataParticleKey.VALUE: str(matched.group(15))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.ANALOG_OUTPUT,
                 DataParticleKey.VALUE: str(matched.group(16))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DATA_LOGGING,
                 DataParticleKey.VALUE: str(matched.group(17))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.APF_INTERFACE,
                 DataParticleKey.VALUE: str(matched.group(18))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SCHEDULING,
                 DataParticleKey.VALUE: str(matched.group(19))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LAMP_FAN,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(20))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.ADDR_LAMP_TEMP,
                 DataParticleKey.VALUE: str(matched.group(21))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.ADDR_SPEC_TEMP,
                 DataParticleKey.VALUE: str(matched.group(22))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SENSOR_ADDR_HOUS_TEMP,
                 DataParticleKey.VALUE: str(matched.group(23))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SERIAL_NUM_SPECT,
                 DataParticleKey.VALUE: str(matched.group(24))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SERIAL_NUM_LAMP,
                 DataParticleKey.VALUE: str(matched.group(25))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.STUPSTUS,
                 DataParticleKey.VALUE: str(matched.group(26))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.BAUD_RATE,
                 DataParticleKey.VALUE: int(matched.group(27))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.MSG_LEVEL,
                 DataParticleKey.VALUE: str(matched.group(28))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.MSG_FILE_SIZE,
                 DataParticleKey.VALUE: int(matched.group(29))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DATA_FILE_SIZE,
                 DataParticleKey.VALUE: int(matched.group(30))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.OUTPUT_FRAME_TYPE,
                 DataParticleKey.VALUE: str(matched.group(31))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LOGGING_FRAME_TYPE,
                 DataParticleKey.VALUE: str(matched.group(32))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.OUTPUT_DARK_FRAME,
                 DataParticleKey.VALUE: str(matched.group(33))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LOGGING_DARK_FRAME,
                 DataParticleKey.VALUE: str(matched.group(34))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TIMERESL,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(35))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LOG_FILE_TYPE,
                 DataParticleKey.VALUE: str(matched.group(36))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.ACQCOUNT,
                 DataParticleKey.VALUE: int(matched.group(37))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.CNTCOUNT,
                 DataParticleKey.VALUE: int(matched.group(38))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.NITRATE_MIN,
                 DataParticleKey.VALUE: float(matched.group(39))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.NITRATE_MAX,
                 DataParticleKey.VALUE: float(matched.group(40))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.WAVELENGTH_LOW,
                 DataParticleKey.VALUE: float(matched.group(41))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.WAVELENGTH_HIGH,
                 DataParticleKey.VALUE: float(matched.group(42))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SDI12_ADDR,
                 DataParticleKey.VALUE: int(matched.group(43))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DATAMODE,
                 DataParticleKey.VALUE: str(matched.group(44))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.OPERATING_MODE,
                 DataParticleKey.VALUE: str(matched.group(45))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.OPERATION_CTRL,
                 DataParticleKey.VALUE: str(matched.group(46))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.EXTL_DEV,
                 DataParticleKey.VALUE: str(matched.group(47))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PRERUN_TIME,
                 DataParticleKey.VALUE: int(matched.group(48))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DEV_DURING_ACQ,
                 DataParticleKey.VALUE: str(matched.group(49))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.WATCHDOG_TIME,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(50))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.COUNTDOWN,
                 DataParticleKey.VALUE: int(matched.group(51))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.FIXED_TIME,
                 DataParticleKey.VALUE: int(matched.group(52))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PERIODIC_INTERVAL,
                 DataParticleKey.VALUE: str(matched.group(53))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PERIODIC_OFFSET,
                 DataParticleKey.VALUE: int(matched.group(54))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PERIODIC_DURATION,
                 DataParticleKey.VALUE: int(matched.group(55))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.PERIODIC_SAMPLES,
                 DataParticleKey.VALUE: int(matched.group(56))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.POLLED_TIMEOUT,
                 DataParticleKey.VALUE: int(matched.group(57))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.APF_TIMEOUT,
                 DataParticleKey.VALUE: float(matched.group(58))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.STABILITY_TIME,
                 DataParticleKey.VALUE: int(matched.group(59))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.MIN_LAMP_ON,
                 DataParticleKey.VALUE: self._none_check(int, matched.group(60))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SKIP_SLEEP,
                 DataParticleKey.VALUE: str(matched.group(61))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SWITCHOFF_TEMP,
                 DataParticleKey.VALUE: int(matched.group(62))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SPEC_PERIOD,
                 DataParticleKey.VALUE: int(matched.group(63))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DRKAVERS,
                 DataParticleKey.VALUE: int(matched.group(64))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LGTAVERS,
                 DataParticleKey.VALUE: int(matched.group(65))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.REFSAMPLES,
                 DataParticleKey.VALUE: self._none_check(int, matched.group(66))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DARK_SAMPLES,
                 DataParticleKey.VALUE: int(matched.group(67))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LIGHT_SAMPLES,
                 DataParticleKey.VALUE: int(matched.group(68))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DARK_DURATION,
                 DataParticleKey.VALUE: int(matched.group(69))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LIGHT_DURATION,
                 DataParticleKey.VALUE: int(matched.group(70))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TEMP_COMP,
                 DataParticleKey.VALUE: str(matched.group(71))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.SALINITY_FIT,
                 DataParticleKey.VALUE: str(matched.group(72))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.BROMIDE_TRACING,
                 DataParticleKey.VALUE: str(matched.group(73))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.BASELINE_ORDER,
                 DataParticleKey.VALUE: int(matched.group(74))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.CONCENTRATIONS_FIT,
                 DataParticleKey.VALUE: int(matched.group(75))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DARK_CORR_METHOD,
                 DataParticleKey.VALUE: str(matched.group(76))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DRKCOEFS,
                 DataParticleKey.VALUE: self._none_check(str, matched.group(77))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DAVGPRM_0,
                 DataParticleKey.VALUE: self._none_check(float, matched.group(78))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DAVGPRM_1,
                 DataParticleKey.VALUE: self._none_check(float, matched.group(79))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DAVGPRM_2,
                 DataParticleKey.VALUE: self._none_check(float, matched.group(80))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.DAVGPRM_3,
                 DataParticleKey.VALUE: self._none_check(float, matched.group(81))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.ABSORBANCE_CUTOFF,
                 DataParticleKey.VALUE: float(matched.group(82))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TIME_ADJ,
                 DataParticleKey.VALUE: str(matched.group(83))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TIME_FACTOR,
                 DataParticleKey.VALUE: int(matched.group(84))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TIME_STEP,
                 DataParticleKey.VALUE: int(matched.group(85))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.TIME_MAX,
                 DataParticleKey.VALUE: int(matched.group(86))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.FIT_WAVE_LOW,
                 DataParticleKey.VALUE: float(matched.group(87))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.FIT_WAVE_HIGH,
                 DataParticleKey.VALUE: float(matched.group(88))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.LAMP_TIME,
                 DataParticleKey.VALUE: int(matched.group(89))},
                {DataParticleKey.VALUE_ID: SUNAStatusDataParticleKey.CALIBRATION_FILE,
                 DataParticleKey.VALUE: str(matched.group(90))}]

            parsed_data_list = [x for x in parsed_data_list if not x[DataParticleKey.VALUE] is None]

        except ValueError:
            raise SampleException("ValueError while parsing data [%s]" % self.raw_data)

        log.debug('SUNAStatusDataParticle raw data: %r', self.raw_data)
        log.debug('SUNAStatusDataParticle parsed data: %r', parsed_data_list)

        return parsed_data_list


class SUNATestDataParticleKey(BaseEnum):
    EXT_DISK_SIZE = "nutnr_external_disk_size"
    EXT_DISK_FREE = "nutnr_external_disk_free"
    INT_DISK_SIZE = "nutnr_internal_disk_size"
    INT_DISK_FREE = "nutnr_internal_disk_free"
    TEMP_HS = "temp_interior"
    TEMP_SP = "temp_spectrometer"
    TEMP_LM = "temp_lamp"
    LAMP_TIME = "lamp_time"
    HUMIDITY = "humidity"
    ELECTRICAL_MN = "nutnr_electrical_mn"
    ELECTRICAL_BD = "nutnr_electrical_bd"
    ELECTRICAL_PR = "nutnr_electrical_pr"
    ELECTRICAL_C = "nutnr_electrical_c"
    LAMP_POWER = "nutnr_lamp_power"
    SPEC_DARK_AV = "nutnr_spec_dark_av"
    SPEC_DARK_SD = "nutnr_spec_dark_sd"
    SPEC_DARK_MI = "nutnr_spec_dark_mi"
    SPEC_DARK_MA = "nutnr_spec_dark_ma"
    SPEC_LIGHT_AV = "nutnr_spec_lght_av"
    SPEC_LIGHT_SD = "nutnr_spec_lght_sd"
    SPEC_LIGHT_MI = "nutnr_spec_lght_mi"
    SPEC_LIGHT_MA = "nutnr_spec_lght_ma"
    TEST_RESULT = "nutnr_test_result"


class SUNATestDataParticle(DataParticle):
    _data_particle_type = DataParticleType.SUNA_TEST

    def _build_parsed_values(self):
        matched = SUNA_TEST_REGEX.match(self.raw_data)

        if not matched:
            raise SampleException("No regex match for test [%s]" % self.raw_data)
        try:

            time_str = str(matched.group(5)).split(":")
            hours = int(time_str[0])
            minutes = int(time_str[1])
            seconds = int(time_str[2])
            time_in_seconds = (hours * 3600) + (minutes * 60) + seconds

            parsed_data_list = [
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.EXT_DISK_SIZE,
                 DataParticleKey.VALUE: int(matched.group(1))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.EXT_DISK_FREE,
                 DataParticleKey.VALUE: int(matched.group(2))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.INT_DISK_SIZE,
                 DataParticleKey.VALUE: int(matched.group(3))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.INT_DISK_FREE,
                 DataParticleKey.VALUE: int(matched.group(4))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.LAMP_TIME,
                 DataParticleKey.VALUE: time_in_seconds},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.TEMP_HS,
                 DataParticleKey.VALUE: float(matched.group(6))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.TEMP_SP,
                 DataParticleKey.VALUE: float(matched.group(7))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.TEMP_LM,
                 DataParticleKey.VALUE: float(matched.group(8))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.HUMIDITY,
                 DataParticleKey.VALUE: float(matched.group(9))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.ELECTRICAL_MN,
                 DataParticleKey.VALUE: float(matched.group(10))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.ELECTRICAL_BD,
                 DataParticleKey.VALUE: float(matched.group(11))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.ELECTRICAL_PR,
                 DataParticleKey.VALUE: float(matched.group(12))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.ELECTRICAL_C,
                 DataParticleKey.VALUE: float(matched.group(13))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.LAMP_POWER,
                 DataParticleKey.VALUE: int(matched.group(14))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_DARK_AV,
                 DataParticleKey.VALUE: int(matched.group(15))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_DARK_SD,
                 DataParticleKey.VALUE: int(matched.group(16))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_DARK_MI,
                 DataParticleKey.VALUE: int(matched.group(17))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_DARK_MA,
                 DataParticleKey.VALUE: int(matched.group(18))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_LIGHT_AV,
                 DataParticleKey.VALUE: int(matched.group(19))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_LIGHT_SD,
                 DataParticleKey.VALUE: int(matched.group(20))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_LIGHT_MI,
                 DataParticleKey.VALUE: int(matched.group(21))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.SPEC_LIGHT_MA,
                 DataParticleKey.VALUE: int(matched.group(22))},
                {DataParticleKey.VALUE_ID: SUNATestDataParticleKey.TEST_RESULT,
                 DataParticleKey.VALUE: str(matched.group(23))}]

        except ValueError:
            raise SampleException("ValueError while parsing data [%s]" % self.raw_data)

        log.debug('SUNATestDataParticle raw data: %r', self.raw_data)
        log.debug('SUNATestDataParticle parsed data: %r', parsed_data_list)

        return parsed_data_list


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class Protocol(CommandResponseInstrumentProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """
    __metaclass__ = get_logging_metaclass(log_level='debug')

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """

        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)

        # Set attributes
        self._newline = NEWLINE

        self._protocol_fsm = ThreadSafeFSM(ProtocolState, ProtocolEvent, ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.ENTER, self._handler_generic_enter)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.UNKNOWN, ProtocolEvent.DISCOVER, self._handler_unknown_discover)

        # COMMAND State
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ENTER, self._handler_enter)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_SAMPLE,
                                       self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.ACQUIRE_STATUS,
                                       self._handler_command_acquire_status)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_DIRECT,
                                       self._handler_command_start_direct)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_AUTOSAMPLE,
                                       self._handler_command_start_autosample)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.START_PERIODIC,
                                       self._handler_command_start_periodic)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.SET, self._handler_command_set)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.TEST, self._handler_command_test)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.CLOCK_SYNC,
                                       self._handler_command_clock_sync)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.MEASURE_0, self._handler_command_measure_0)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.MEASURE_N, self._handler_command_measure_n)
        self._protocol_fsm.add_handler(ProtocolState.COMMAND, ProtocolEvent.TIMED_N, self._handler_command_timed_n)

        # DIRECT ACCESS State
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.ENTER,
                                       self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.EXECUTE_DIRECT,
                                       self._handler_direct_access_execute_direct)
        self._protocol_fsm.add_handler(ProtocolState.DIRECT_ACCESS, ProtocolEvent.STOP_DIRECT,
                                       self._handler_direct_access_stop_direct)

        # ASYNC MEASURE_N State
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_N, ProtocolEvent.ENTER, self._handler_measuring_n_enter)
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_N, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_N, ProtocolEvent.MEASURE_N_ASYNC,
                                       self._handler_measure_n_async)

        # ASYNC TIMED_N State
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_TIMED_N, ProtocolEvent.ENTER,
                                       self._handler_measuring_timed_n_enter)
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_TIMED_N, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.MEASURING_TIMED_N, ProtocolEvent.TIMED_N_ASYNC,
                                       self._handler_timed_n_async)

        # AUTOSAMPLE State
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.ENTER, self._handler_enter)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.AUTOSAMPLE, ProtocolEvent.STOP_AUTOSAMPLE,
                                       self._handler_autosample_stop_autosample)

        # PERIODIC State
        self._protocol_fsm.add_handler(ProtocolState.PERIODIC, ProtocolEvent.ENTER, self._handler_enter)
        self._protocol_fsm.add_handler(ProtocolState.PERIODIC, ProtocolEvent.EXIT, self._handler_generic_exit)
        self._protocol_fsm.add_handler(ProtocolState.PERIODIC, ProtocolEvent.GET, self._handler_get)
        self._protocol_fsm.add_handler(ProtocolState.PERIODIC, ProtocolEvent.STOP_PERIODIC,
                                       self._handler_periodic_stop_periodic)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        # Add build handlers for device commands.
        self._add_build_handler(InstrumentCommands.GET, self._build_get_command)
        self._add_build_handler(InstrumentCommands.SET, self._build_set_command)
        self._add_build_handler(InstrumentCommands.CMD_LINE, self._build_simple_command)
        self._add_build_handler(InstrumentCommands.SLEEP, self._build_simple_command)
        self._add_build_handler(InstrumentCommands.EXIT, self._build_simple_command)
        self._add_build_handler(InstrumentCommands.STATUS, self._build_simple_command)
        self._add_build_handler(InstrumentCommands.MEASURE, self._build_measure_command)
        self._add_build_handler(InstrumentCommands.TIMED, self._build_timed_command)
        self._add_build_handler(InstrumentCommands.SELFTEST, self._build_simple_command)
        self._add_build_handler(InstrumentCommands.SET_CLOCK, self._build_clock_command)
        self._add_build_handler(InstrumentCommands.GET_CAL_FILE, self._build_simple_command)

        # Add response handlers for device commands.
        self._add_response_handler(InstrumentCommands.GET, self._parse_generic_response)
        self._add_response_handler(InstrumentCommands.SET, self._parse_generic_response)
        self._add_response_handler(InstrumentCommands.SET_CLOCK, self._parse_generic_response)
        self._add_response_handler(InstrumentCommands.STATUS, self._parse_generic_response)
        self._add_response_handler(InstrumentCommands.CMD_LINE, self._parse_cmd_line_response)

        # Construct the parameter dictionary containing device parameters,
        # current parameter values, and set formatting functions.
        self._build_param_dict()
        self._build_cmd_dict()

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        self._chunker = StringChunker(Protocol.sieve_function)

        self._direct_commands['Wakeup'] = NEWLINE
        command_dict = InstrumentCommands.dict()
        label_dict = InstrumentCommandNames.dict()
        for key in command_dict:
            label = label_dict.get(key)
            command = command_dict[key]
            builder = self._build_handlers.get(command, None)
            if builder is self._build_simple_command:
                command = builder(command)
                self._direct_commands[label] = command
            elif builder in [self._build_get_command, self._build_set_command, self._build_clock_command,
                             self._build_timed_command, self._build_measure_command]:
                command += ' '


    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        :param raw_data:
        """
        return_list = []

        sieve_match = [SUNA_SAMPLE_REGEX,
                       SUNA_STATUS_REGEX,
                       SUNA_TEST_REGEX]

        for matcher in sieve_match:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def _build_cmd_dict(self):
        """
        Populate the command dictionary with commands
        """
        self._cmd_dict.add(Capability.ACQUIRE_SAMPLE, timeout=ACQUIRE_SAMPLE_TIMEOUT, display_name='Acquire Sample')
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, timeout=ACQUIRE_STATUS_TIMEOUT, display_name='Acquire Status')
        self._cmd_dict.add(Capability.MEASURE_N, timeout=MEASURE_N_TIMEOUT, display_name='Acquire N Light Samples')
        self._cmd_dict.add(Capability.MEASURE_0, timeout=MEASURE_0_TIMEOUT, display_name='Acquire Dark Sample')
        self._cmd_dict.add(Capability.TIMED_N, timeout=TIMED_N_TIMEOUT, display_name='Acquire Light Samples (N seconds)')
        self._cmd_dict.add(Capability.TEST, display_name='Execute Test')
        self._cmd_dict.add(Capability.START_AUTOSAMPLE, display_name='Start Autosample')
        self._cmd_dict.add(Capability.STOP_AUTOSAMPLE, timeout=STOP_AUTOSAMPLE_TIMEOUT, display_name='Stop Autosample')
        self._cmd_dict.add(Capability.START_PERIODIC, display_name='Start Periodic Mode')
        self._cmd_dict.add(Capability.STOP_PERIODIC, timeout=STOP_PERIODIC_TIMEOUT, display_name='Stop Periodic Mode')
        self._cmd_dict.add(Capability.CLOCK_SYNC, timeout=CLOCK_SYNC_TIMEOUT, display_name='Synchronize Clock')
        self._cmd_dict.add(Capability.DISCOVER, timeout=DISCOVER_TIMEOUT, display_name='Discover')

    def _build_param_dict(self):
        """
        Populate the parameter dictionary with parameters.
        For each parameter key, add match string, match lambda function,
        and value formatting function for set commands.
        """

        # DATA ACQUISITION
        self._param_dict.add(Parameter.OPERATION_CONTROL,
                             r'OPERCTRL\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value='Samples',
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Operation Control",
                             range={'Samples': 'Samples', 'Duration': 'Duration'},
                             description='Operation control: (Samples | Duration)')

        self._param_dict.add(Parameter.LIGHT_SAMPLES,
                             r'LGTSMPLS\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=5,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Light Samples",
                             range=(1, 65535),
                             description='Number of light samples: (1 - 65535)')

        self._param_dict.add(Parameter.DARK_SAMPLES,
                             r'DRKSMPLS\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=1,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Dark Samples",
                             range=(1, 65535),
                             description='Number of dark samples: (1 - 65535)')

        self._param_dict.add(Parameter.LIGHT_DURATION,
                             r'LGTDURAT\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=10,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Light Duration",
                             range=(1, 65535),
                             description='How long lamp is on during sample collect: (1 - 65535)',
                             units=Units.SECOND)

        self._param_dict.add(Parameter.DARK_DURATION,
                             r'DRKDURAT\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=5,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Dark Duration",
                             range=(1, 65535),
                             description='How long lamp is off during sample collect: (1 - 65535)',
                             units=Units.SECOND)

        self._param_dict.add(Parameter.POLLED_TIMEOUT,
                             r'POLLTOUT\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=65535,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Polled Timeout",
                             range=(1, 65535),
                             description='Instrument will go to sleep if not polled within time interval: (0 - 65535)',
                             units=Units.SECOND)

        self._param_dict.add(Parameter.SKIP_SLEEP_AT_START,
                             r'SKPSLEEP\s(\S*)',
                             lambda match: True if match.group(1) == InstrumentCommandArgs.ON else False,
                             lambda x: InstrumentCommandArgs.ON if self._is_true(x) else InstrumentCommandArgs.OFF,
                             type=ParameterDictType.BOOL,
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Skip Sleep at Start",
                             range={'True': True, 'False': False},
                             description='Disable sleep at start: (true | false)')

        self._param_dict.add(Parameter.COUNTDOWN,
                             r'COUNTDWN\s(\S*)',
                             lambda match:
                             int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=15,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Countdown",
                             range=(0, 3600),
                             description="How long to wait before starting sampling: (0 - 3600)",
                             units=Units.SECOND)

        self._param_dict.add(Parameter.REF_MIN_AT_LAMP_ON,
                             r'REFLIMIT\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=False,
                             direct_access=False,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             units=Units.COUNTS,
                             display_name="Reference Minute at Lamp-On",
                             range=(0, 65000),
                             description="When switching on the lamp, the reference detector must register at least "
                                         "the specified number of counts: (0 - 65000)")

        self._param_dict.add(Parameter.LAMP_STABIL_TIME,
                             r'STBLTIME\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=5,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Lamp Stability Time",
                             range=(0, 255),
                             description="Time required to stabilize lamp output after lamp has ignited: (0 - 255)",
                             units=Prefixes.DECI + Units.SECOND)

        self._param_dict.add(Parameter.LAMP_SWITCH_OFF_TEMPERATURE,
                             r'LAMPTOFF\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=35,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Lamp Switch-Off Temperature",
                             range=(-274, 32767),
                             description='Temperature at which lamp will turn off.',
                             units=Units.DEGREE_CELSIUS)

        self._param_dict.add(Parameter.SPECTROMETER_INTEG_PERIOD,
                             r'SPINTPER\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=False,
                             direct_access=False,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Spectrometer Integration Period",
                             range=(5, 60000),
                             description="Factory set time to obtain a good signal without saturation: (5 - 60000)",
                             units=Units.MILLISECOND)

        # INPUT / OUTPUT
        self._param_dict.add(Parameter.MESSAGE_LEVEL,
                             r'MSGLEVEL\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value="Info",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Message Level",
                             range={'Error': 'Error', 'Warn': 'Warn', 'Info': 'Info', 'Debug': 'Debug'},
                             description="Level of logging: (Error | Warn | Info | Debug)")

        self._param_dict.add(Parameter.MESSAGE_FILE_SIZE,
                             r'MSGFSIZE\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=0,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Message File Size",
                             range=(0, 65),
                             description="Set size of logging file: (0 - 65)",
                             units=Prefixes.MEGA + Units.BYTE)

        self._param_dict.add(Parameter.DATA_FILE_SIZE,
                             r'DATFSIZE\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=5,
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Data File Size",
                             range=(1, 65),
                             description="Set size of data collection file (1 - 65).",
                             units=Prefixes.MEGA + Units.BYTE)

        self._param_dict.add(Parameter.OUTPUT_FRAME_TYPE,
                             r'OUTFRTYP\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value="Full_ASCII",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Output Frame Type",
                             range={'Full_ASCII': 'Full_ASCII', 'Full_Binary': 'Full_Binary',
                                    'Reduced_Binary': 'Reduced_Binary', 'Concentration': 'Concentration', 'APF': 'APF',
                                    'MBARI': 'MBARI', 'None': 'None'},
                             description="Type: (Full_ASCII | Full_Binary | Reduced_Binary"
                                         " | Concentration | APF | MBARI | None)")

        self._param_dict.add(Parameter.OUTPUT_DARK_FRAME,
                             r'OUTDRKFR\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value="Output",
                             visibility=ParameterDictVisibility.IMMUTABLE,
                             display_name="Output Dark Frame",
                             range={'Output': 'Output', 'Suppress': 'Suppress'},
                             description="Enable dark frame sampling: (Output | Suppress)")

        # DATA PROCESSING
        self._param_dict.add(Parameter.TEMP_COMPENSATION,
                             r'TEMPCOMP\s(\S*)',
                             lambda match: True if match.group(1) == InstrumentCommandArgs.ON else False,
                             lambda x: InstrumentCommandArgs.ON if self._is_true(x) else InstrumentCommandArgs.OFF,
                             type=ParameterDictType.BOOL,
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Temperature Compensation",
                             range={'True': True, 'False': False},
                             description="Enable temperature compensation: (true | false)")

        self._param_dict.add(Parameter.FIT_WAVELENGTH_LOW,
                             r'WFIT_LOW\s(\S*)',
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             startup_param=True,
                             direct_access=True,
                             default_value=217.00,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Fit Wavelength Low",
                             range=(210, 350),
                             description="Low value used for measuring nitrogen concentrations: (210 - 350)",
                             units=Units.NANOMETER)

        self._param_dict.add(Parameter.FIT_WAVELENGTH_HIGH,
                             r'WFIT_HGH\s(\S*)',
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             startup_param=True,
                             direct_access=True,
                             default_value=240.00,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Fit Wavelength High",
                             range=(210, 350),
                             description="High value used for measuring nitrogen concentrations: (210 - 350)",
                             units=Units.NANOMETER)

        self._param_dict.add(Parameter.CONCENTRATIONS_IN_FIT,
                             r'FITCONCS\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=1,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Concentrations to Fit",
                             range=(1, 3),
                             description="Number of concentrations to be used for processing: (1 - 3)")

        self._param_dict.add(Parameter.BASELINE_ORDER,
                             r'BL_ORDER\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=False,
                             direct_access=False,
                             value=1,
                             visibility=ParameterDictVisibility.READ_ONLY,
                             display_name="Baseline Order",
                             range={'Linear': 1, 'Quadratic': 2},
                             description="Function type to use: (Linear | Quadratic")

        self._param_dict.add(Parameter.DARK_CORRECTION_METHOD,
                             r'DRKCORMT\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value="SpecAverage",
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Dark Correction Method",
                             range={'SpecAverage': 'SpecAverage', 'SWAverage': 'SWAverage'},
                             description="Method: (SpecAverage | SWAverage)")

        self._param_dict.add(Parameter.SALINITY_FITTING,
                             r'SALINFIT\s(\S*)',
                             lambda match: True if match.group(1) == InstrumentCommandArgs.ON else False,
                             lambda x: InstrumentCommandArgs.ON if self._is_true(x) else InstrumentCommandArgs.OFF,
                             type=ParameterDictType.BOOL,
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Salinity Fitting",
                             range={'True': True, 'False': False},
                             description="Enable salinity matching: (true | false)")

        self._param_dict.add(Parameter.BROMIDE_TRACING,
                             r'BRMTRACE\s(\S*)',
                             lambda match: True if match.group(1) == InstrumentCommandArgs.ON else False,
                             lambda x: InstrumentCommandArgs.ON if self._is_true(x) else InstrumentCommandArgs.OFF,
                             type=ParameterDictType.BOOL,
                             startup_param=True,
                             direct_access=True,
                             default_value=False,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Bromide Tracing",
                             range={'True': True, 'False': False},
                             description="Enable bromide tracing: (true | false)")

        self._param_dict.add(Parameter.ABSORBANCE_CUTOFF,
                             r'A_CUTOFF\s(\S*)',
                             lambda match: float(match.group(1)),
                             str,
                             type=ParameterDictType.FLOAT,
                             startup_param=True,
                             direct_access=True,
                             default_value=1.3,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Absorbance Cutoff",
                             range=(0.01, 10.0),
                             description="Cutoff value to exclude channel from processing: (0.01 - 10.0)")

        self._param_dict.add(Parameter.INTEG_TIME_ADJUSTMENT,
                             r'INTPRADJ\s(\S*)',
                             lambda match: True if match.group(1) == InstrumentCommandArgs.ON else False,
                             lambda x: InstrumentCommandArgs.ON if self._is_true(x) else InstrumentCommandArgs.OFF,
                             type=ParameterDictType.BOOL,
                             startup_param=True,
                             direct_access=True,
                             default_value=True,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Integration Time Adjustment",
                             range={'True': True, 'False': False},
                             description="Enable integration time adjustment: (true | false)")

        self._param_dict.add(Parameter.INTEG_TIME_FACTOR,
                             r'INTPRFAC\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=1,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Integration Time Factor",
                             range=(1, 20),
                             description="Factor used in integration time adjustment: (1 - 20)",
                             units=Units.SECOND)

        self._param_dict.add(Parameter.INTEG_TIME_STEP,
                             r'INTADSTP\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=20,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Integration Time Step",
                             range=(1, 20),
                             description="Time step used in integration time adjustment: (1 - 20)",
                             units=Units.SECOND)

        self._param_dict.add(Parameter.INTEG_TIME_MAX,
                             r'INTADMAX\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=20,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Integration Time Max",
                             range=(1, 20),
                             description="Maximum integration time factor: (1 - 20)",
                             units=Units.SECOND)

        # PERIODIC PARAMETERS
        self._param_dict.add(Parameter.PERIODIC_INTERVAL,
                             r'PERDIVAL\s(\S*)',
                             lambda match: match.group(1),
                             str,
                             type=ParameterDictType.STRING,
                             startup_param=True,
                             direct_access=True,
                             default_value='1h',
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Periodic Interval",
                             range={'00:01': '1m', '00:02': '2m', '00:05': '5m', '00:06': '6m', '00:10': '10m',
                                    '00:15': '15m', '00:20': '20m', '00:30': '30m', '01:00': '1h', '02:00': '2h',
                                    '03:00': '3h', '04:00': '4h', '06:00': '6h', '08:00': '8h', '12:00': '12h',
                                    '24:00': '24h'},
                             description="Establishes a grid of acquisition times in Periodic Mode (HH:MM)")

        self._param_dict.add(Parameter.PERIODIC_OFFSET,
                             r'PERDOFFS\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=0,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Periodic Offset",
                             range=(0, 86399),
                             description="Measured in seconds. Locates the time grid"
                                         " relative to the start of the day in Periodic Mode (0-86399)",
                             units=Units.SECOND)

        self._param_dict.add(Parameter.PERIODIC_DURATION,
                             r'PERDDURA\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=10,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Periodic Duration",
                             range=(1, 255),
                             description="Number of seconds over which data will be collected in Periodic mode when"
                                         " Operation Control is Duration (1-255)",
                             units=Units.SECOND)

        self._param_dict.add(Parameter.PERIODIC_SAMPLES,
                             r'PERDSMPL\s(\S*)',
                             lambda match: int(match.group(1)),
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=True,
                             default_value=10,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Periodic Samples",
                             range=(1, 255),
                             description="Number of samples collected in Periodic mode when Operation Control is"
                                         " Samples (1-255)")

        # DRIVER PARAMETERS
        self._param_dict.add(Parameter.NUM_LIGHT_SAMPLES,
                             r'donotmatch',
                             int,
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=False,
                             default_value=2,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Number of Light Samples",
                             range=(1, 40),
                             description="Number of light samples taken in polled mode: (1 - 40)")

        self._param_dict.add(Parameter.TIME_LIGHT_SAMPLE,
                             r'donotmatch',
                             int,
                             str,
                             type=ParameterDictType.INT,
                             startup_param=True,
                             direct_access=False,
                             default_value=30,
                             visibility=ParameterDictVisibility.READ_WRITE,
                             display_name="Time to Take Light Sample",
                             range=(0, 30),
                             description="Number of seconds to take light samples in polled mode: (0 - 30)",
                             units=Units.SECOND)

    def _got_chunk(self, chunk, timestamp):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and REGEXes.
        """
        try:
            if self._extract_sample(SUNASampleDataParticle, SUNA_SAMPLE_REGEX, chunk, timestamp):
                return
            if self._extract_sample(SUNAStatusDataParticle, SUNA_STATUS_REGEX, chunk, timestamp):
                return
            if self._extract_sample(SUNATestDataParticle, SUNA_TEST_REGEX, chunk, timestamp):
                return
        except SampleException:
            raise SampleException('Error extracting DataParticle')

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _handler_generic_exit(self, *args, **kwargs):
        """
        Generic exit handler, do nothing
        """
        pass

    def _handler_generic_enter(self, *args, **kwargs):
        """
        Generic enter handler, raise STATE CHANGE
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_enter(self):
        """
        Enter handler for states that can initialize parameters.
        Used for COMMAND, AUTOSAMPLE, and PERIODIC
        """

        if self._init_type != InitializationType.NONE:
            self._update_params()
            self._init_params()
            # Exit will resume sampling if in PERIODIC or AUTOSAMPLE. No effect if COMMAND.
            self._do_cmd_no_resp(InstrumentCommands.EXIT)

        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    ########################################################################
    # Unknown handlers.
    ########################################################################
    def _handler_unknown_discover(self):
        """
        Discover current instrument mode and set state accordingly
        If mode is not polled or continuous then set to polled
        @return next_state, (next_state, result)
        """
        result = []

        self._bring_up_command_line()
        mode = self._do_cmd_resp(InstrumentCommands.GET, Parameter.OPERATION_MODE, response_regex=OK_GET_REGEX)
        log.debug('Upon discover instrument mode is %s', mode)

        state_map = {
            InstrumentCommandArgs.POLLED: ProtocolState.COMMAND,
            InstrumentCommandArgs.CONTINUOUS: ProtocolState.AUTOSAMPLE,
            InstrumentCommandArgs.PERIODIC: ProtocolState.PERIODIC
        }

        next_state = state_map.get(mode)

        if next_state is None:
            # Instrument is in unsupported state. Set to COMMAND state
            log.info('Instrument is in unsupported mode "%s", setting state to %s', mode, ProtocolState.COMMAND)
            return self._change_state(ProtocolState.COMMAND)

        return next_state, (next_state, result)

    ########################################################################
    # Command handlers.
    ########################################################################
    def _handler_command_acquire_sample(self):
        """
        Get a sample from the SUNA
        """
        next_state = None
        timeout = time.time() + DEFAULT_TIMEOUT

        # exit command-line to CMD? prompt (does nothing if already at CMD? prompt)
        self._do_cmd_no_resp(InstrumentCommands.EXIT)
        self._do_cmd_resp(InstrumentCommands.MEASURE, 1, expected_prompt=Prompt.POLLED, timeout=DEFAULT_TIMEOUT)
        particles = self.wait_for_particles([DataParticleType.SUNA_SAMPLE], timeout)
        return next_state, (next_state, particles)

    def _handler_command_acquire_status(self):
        """
        Start acquire status
        """
        next_state = None
        timeout = time.time() + DEFAULT_TIMEOUT

        self._bring_up_command_line()
        status_output = self._do_cmd_resp(InstrumentCommands.STATUS, expected_prompt=[Prompt.OK, Prompt.ERROR])
        self._do_cmd_no_resp(InstrumentCommands.GET_CAL_FILE)

        old_config = self._param_dict.get_config()
        self._param_dict.update(status_output)
        new_config = self._param_dict.get_config()

        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

        particles = self.wait_for_particles([DataParticleType.SUNA_STATUS], timeout)

        return next_state, (next_state, particles)

    def _handler_command_start_direct(self):
        """
        Start direct access
        """
        next_state = ProtocolState.DIRECT_ACCESS
        result = []
        return next_state, (next_state, result)

    def _handler_command_start_autosample(self):
        """
        Start autosampling
        """
        return self._change_state(ProtocolState.AUTOSAMPLE)

    def _handler_command_start_periodic(self):
        """
        Start Periodic
        """
        return self._change_state(ProtocolState.PERIODIC)

    def _handler_command_set(self, params, *args):
        """
        Set parameter
        """
        next_state = None
        result = None
        self._set_params(params, *args)
        return next_state, (next_state, result)

    def _set_params(self, *args, **kwargs):
        """
        Used to set the parameters including when startup config is set by _init_params call
        """

        try:
            params = args[0]

            if params is None or not isinstance(params, dict):
                raise InstrumentParameterException('Params is empty or is not a dictionary')

        except IndexError:
            raise InstrumentParameterException('Set command requires a parameter dict.')

        self._verify_not_readonly(*args, **kwargs)

        old_config = self._param_dict.get_config()
        log.debug("OLD CONFIG: %s", self._param_dict.get_config())

        # Make sure we are in the command line before sending commands
        self._bring_up_command_line()

        # Special case for FIT_WAVELENGTH_HIGH and FIT_WAVELENGTH_LOW:
        # They must be set together
        if Parameter.FIT_WAVELENGTH_HIGH in params or Parameter.FIT_WAVELENGTH_LOW in params:
            current_high = self._param_dict.get(Parameter.FIT_WAVELENGTH_HIGH)
            current_low = self._param_dict.get(Parameter.FIT_WAVELENGTH_LOW)
            new_high = params.get(Parameter.FIT_WAVELENGTH_HIGH, current_high)
            new_low = params.get(Parameter.FIT_WAVELENGTH_LOW, current_low)

            # Format before comparing
            current_high = self._format_value(Parameter.FIT_WAVELENGTH_HIGH, current_high)
            current_low = self._format_value(Parameter.FIT_WAVELENGTH_LOW, current_low)
            new_high = self._format_value(Parameter.FIT_WAVELENGTH_HIGH, new_high)
            new_low = self._format_value(Parameter.FIT_WAVELENGTH_LOW, new_low)

            if current_high != new_high or current_low != new_low:
                value = new_low + ',' + new_high
                self._do_cmd_resp(InstrumentCommands.SET, Parameter.FIT_WAVELENGTH_BOTH, value, timeout=DEFAULT_TIMEOUT,
                                  expected_prompt=[Prompt.OK, Prompt.ERROR])

        # Handle parameters that are for the driver, not the instrument
        if Parameter.NUM_LIGHT_SAMPLES in params:
            val = params[Parameter.NUM_LIGHT_SAMPLES]
            if val >= MIN_LIGHT_SAMPLE or val <= MAX_LIGHT_SAMPLE:
                self._param_dict.set_value(Parameter.NUM_LIGHT_SAMPLES, val)
            else:
                raise InstrumentParameterException('Parameter value is outside constraints!')

        if Parameter.TIME_LIGHT_SAMPLE in params:
            val = params[Parameter.TIME_LIGHT_SAMPLE]
            if val >= MIN_TIME_SAMPLE or val <= MAX_TIME_SAMPLE:
                self._param_dict.set_value(Parameter.TIME_LIGHT_SAMPLE, val)
            else:
                raise InstrumentParameterException('Parameter value is outside constraints!')

        # Handle the rest of the normal instrument parameters
        skip_these = (Parameter.FIT_WAVELENGTH_HIGH, Parameter.FIT_WAVELENGTH_LOW, Parameter.NUM_LIGHT_SAMPLES,
                      Parameter.TIME_LIGHT_SAMPLE)
        for (key, val) in params.iteritems():
            if key in skip_these:
                continue

            log.debug("KEY = %s VALUE = %s", key, val)

            # Format before comparing
            new_val = self._format_value(key, val)
            current_val = self._format_value(key, self._param_dict.get(key))
            if current_val != new_val:
                self._do_cmd_resp(InstrumentCommands.SET, key, new_val, timeout=DEFAULT_TIMEOUT,
                                  expected_prompt=[Prompt.OK, Prompt.ERROR])

        # Collect the current settings from the instrument
        status_output = self._do_cmd_resp(InstrumentCommands.STATUS, expected_prompt=[Prompt.OK, Prompt.ERROR])
        self._param_dict.update(status_output)

        new_config = self._param_dict.get_config()
        log.debug("NEW CONFIG: %s", self._param_dict.get_config())

        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _handler_command_test(self):
        """
        Test the instrument state
        """
        next_state = None
        result = []

        self._bring_up_command_line()
        self._do_cmd_no_resp(InstrumentCommands.SELFTEST)
        return next_state, (next_state, result)

    def _handler_command_clock_sync(self, *args, **kwargs):
        """
        Sync clock close to a second edge
        set clock YYYY/MM/DD hh:mm:ss
        """
        next_state = None
        str_time = get_timestamp_delayed("%Y/%m/%d %H:%M:%S")
        log.debug('syncing clock to: %s', str_time)

        self._bring_up_command_line()
        result = self._do_cmd_resp(InstrumentCommands.SET_CLOCK, str_time, timeout=CLOCK_SYNC_TIMEOUT,
                                   expected_prompt=[Prompt.OK, Prompt.ERROR])

        return next_state, (next_state, [result])

    ########################################################################
    # Direct access handlers.
    ########################################################################
    def _handler_direct_access_enter(self):
        """
        Enter direct access state.
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
        self._sent_cmds = []

    def _handler_direct_access_execute_direct(self, data):
        """
        Send commands from operator directly to the instrument
        """
        next_state = None
        result = [self._do_cmd_direct(data)]

        # add sent command to list for 'echo' filtering in callback
        self._sent_cmds.append(data)

        return next_state, (next_state, result)

    def _handler_direct_access_stop_direct(self):
        """
        Stopping DA, restore the DA parameters to their previous value
        """
        return self._handler_unknown_discover()

    ########################################################################
    # Measurement commands
    ########################################################################
    def _handler_command_measure_n(self):
        """
        Measure N is asynchronous. Transition to MEASURING_N State
        """
        next_state = ProtocolState.MEASURING_N
        result = []
        return next_state, (next_state, result)

    def _handler_measuring_n_enter(self):
        """
        Trigger the MEASURE_N_ASYNC event
        """
        self._async_raise_fsm_event(ProtocolEvent.MEASURE_N_ASYNC)

    def _handler_measure_n_async(self):
        """
        Measure N Light Samples
        """
        next_state = ProtocolState.COMMAND
        result = []

        # exit command-line to CMD? prompt (does nothing if already at CMD? prompt)
        self._do_cmd_no_resp(InstrumentCommands.EXIT)
        self._do_cmd_resp(InstrumentCommands.MEASURE, self._param_dict.get(Parameter.NUM_LIGHT_SAMPLES),
                                   expected_prompt=Prompt.POLLED, timeout=MEASURE_N_CMD_TIMEOUT)

        return next_state, (next_state, result)

    def _handler_command_measure_0(self):
        """
        Measure 0 Dark Sample
        """
        next_state = None

        # exit command-line to CMD? prompt (does nothing if already at CMD? prompt)
        self._do_cmd_no_resp(InstrumentCommands.EXIT)
        result = self._do_cmd_resp(InstrumentCommands.MEASURE, 0, expected_prompt=Prompt.POLLED)

        particles = self.wait_for_particles([DataParticleType.SUNA_DARK_SAMPLE], 0)

        return next_state, (next_state, [particles])

    def _handler_command_timed_n(self):
        """
        Timed N is asynchronous. Transition to MEASURING_TIMED_N State
        """
        next_state = ProtocolState.MEASURING_TIMED_N
        result = []
        return next_state, (next_state, result)

    def _handler_measuring_timed_n_enter(self):
        """
        Trigger the TIMED_N_ASYNC event
        """
        self._async_raise_fsm_event(ProtocolEvent.TIMED_N_ASYNC)

    def _handler_timed_n_async(self):
        """
        Timed Sampling for N time
        """
        next_state = ProtocolState.COMMAND
        result = []

        # exit command-line to CMD? prompt (does nothing if already at CMD? prompt)
        self._do_cmd_no_resp(InstrumentCommands.EXIT)
        self._do_cmd_resp(InstrumentCommands.TIMED, self._param_dict.get(Parameter.TIME_LIGHT_SAMPLE),
                                   expected_prompt=Prompt.POLLED, timeout=TIMED_N_CMD_TIMEOUT)
        return next_state, (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_autosample_stop_autosample(self):
        """
        Exit the autosample state
        """
        return self._change_state(ProtocolState.COMMAND)

    ########################################################################
    # Autosample handlers.
    ########################################################################
    def _handler_periodic_stop_periodic(self):
        """
        Exit the periodic state
        """
        return self._change_state(ProtocolState.COMMAND)

    ########################################################################
    # Build handlers
    ########################################################################
    def _build_clock_command(self, cmd, value):
        """
        Build a command to get the desired argument.

        @param cmd The command being used (Command.CLOCK_SYNC in this case)
        @param value string containing the date/time to set
        @retval Returns string ready for sending to instrument
        """
        return "%s %s%s" % (InstrumentCommands.SET_CLOCK, value, NEWLINE)

    def _build_get_command(self, cmd, param):
        """
        Build a command to get the desired argument.

        @param cmd The command being used (Command.GET in this case)
        @param param The name of the parameter to get
        @retval Returns string ready for sending to instrument
        """
        if not Parameter.has(param):
            raise InstrumentParameterException("%s is not a parameter" % param)
        return "%s %s%s" % (InstrumentCommands.GET, param, NEWLINE)

    def _build_set_command(self, cmd, param, value):
        """
        Build a command to set the desired argument

        @param cmd The command being used (Command.SET in this case)
        @param param The name of the parameter to set
        @value The value to set the parameter to
        @retval Returns string ready for sending to instrument
        """
        if not Parameter.has(param):
            raise InstrumentParameterException("%s is not a parameter" % param)

        if param == Parameter.FIT_WAVELENGTH_HIGH:
            param = Parameter.FIT_WAVELENGTH_BOTH
            value = self._param_dict.format(Parameter.FIT_WAVELENGTH_LOW) + ',' + value
        if param == Parameter.FIT_WAVELENGTH_LOW:
            param = Parameter.FIT_WAVELENGTH_BOTH
            value = value + ',' + self._param_dict.format(Parameter.FIT_WAVELENGTH_HIGH)

        return "%s %s %s%s" % (InstrumentCommands.SET, param, value, NEWLINE)

    def _build_measure_command(self, cmd, samples):
        """
        Build a command to take samples

        @param cmd The command, "Measure"
        @param samples The number of light samples to take
        @retval Returns string ready for sending to instrument
        """
        if samples < 0:
            raise InstrumentParameterException("Sample count cannot be less than 0: (%s)" % samples)
        return "%s %s%s" % (InstrumentCommands.MEASURE, samples, NEWLINE)

    def _build_timed_command(self, cmd, time_amount):
        """
        Build a command to take samples

        @param cmd The command, "Timed"
        @param time_amount The amount of time to sample for
        @retval Returns string ready for sending to instrument
        """
        if time_amount < 0:
            raise InstrumentParameterException("Time to sample cannot be less than 0: " % time_amount)
        return "%s %s%s" % (InstrumentCommands.TIMED, time_amount, NEWLINE)

    ########################################################################
    # Response handlers
    ########################################################################
    def _parse_generic_response(self, response, prompt):
        if prompt == Prompt.ERROR:
            raise InstrumentProtocolException("Error occurred for command: (%r)" % response)
        return response

    def _parse_cmd_line_response(self, response, prompt):
        """
        Parse the response from the instrument for a $ command.

        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The response as is, None is there is no response
        """
        for search_prompt in (Prompt.POLLED, Prompt.COMMAND_LINE):
            start = response.find(search_prompt)
            if start != -1:
                log.debug("_parse_cmd_line_response: response=%r", response[start:start + len(search_prompt)])
                return response[start:start + len(search_prompt)]
        return None

    ########################################################################
    # Helpers
    ########################################################################
    def _update_params(self):
        """
        Update the parameter dictionary by getting new values from the instrument. The response
        is saved to the param dictionary.
        """
        self._bring_up_command_line()
        status_output = self._do_cmd_resp(InstrumentCommands.STATUS, expected_prompt=[Prompt.OK, Prompt.ERROR])
        old_config = self._param_dict.get_config()
        self._param_dict.update(status_output)
        new_config = self._param_dict.get_config()

        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)

    def _send_wakeup(self):
        """
        Send a wakeup to this instrument...one that wont hurt if it is awake
        already.
        """
        self._connection.send(NEWLINE)

    def _change_state(self, next_state):
        """
        Transitions to a new state
        @param next_state:
        @return:
        @raise InstrumentStateException: if unhandled state requested
        """

        mode_map = {
            ProtocolState.COMMAND: InstrumentCommandArgs.POLLED,
            ProtocolState.AUTOSAMPLE: InstrumentCommandArgs.CONTINUOUS,
            ProtocolState.PERIODIC: InstrumentCommandArgs.PERIODIC
        }

        args = mode_map.get(next_state)
        if args is None:
            raise InstrumentStateException('Requested transition to unknown state %s', next_state)

        # Set new state
        self._bring_up_command_line()
        result = self._do_cmd_resp(InstrumentCommands.SET, Parameter.OPERATION_MODE, args,
                                   expected_prompt=[Prompt.OK, Prompt.ERROR])
        self._do_cmd_no_resp(InstrumentCommands.EXIT)

        return next_state, (next_state, [result])

    @staticmethod
    def _is_true(x):
        if isinstance(x, basestring):
            return x.lower() == 'true'

        return x

    def _bring_up_command_line(self):
        """
        Get to the command line, that is SUNA> prompt for entering commands.
        It's tricky to get to the prompt when the instrument is in Periodic
        mode.
        What seems to work best is to send $ character, wait until there's
        some response, wait ~1 second, then send it again, repeating until
        the SUNA> prompt appears.

        @raise InstrumentProtocolException: If prompt is not received within
        STOP_PERIODIC_TIMEOUT seconds
        """
        self._linebuf = ''
        re_newline = re.compile(r'(.*)\n(.*)', re.DOTALL)
        timeout = time.time() + STOP_PERIODIC_TIMEOUT

        while time.time() < timeout:
            self._connection.send(InstrumentCommands.CMD_LINE + NEWLINE)
            try:
                groups = self._get_response(5, response_regex=re_newline)
                line = groups[0]
                if Prompt.COMMAND_LINE in line:
                    return
                # Save any characters after the last newline
                self._linebuf = groups[1]
                # Wait 1 second before sending the next $
                time.sleep(1)

            except InstrumentTimeoutException:
                pass

        raise InstrumentProtocolException('Could not bring up command line')

    def _format_value(self, param, value):
        """
        @param param: Parameter in _param_dict to use for formatting
        @param value: value to be formatted
        @return: A string formatted using _param_dict.format()
        @raise InstrumentParameterException: if the parameter name is invalid.
        """
        try:
            formatted_value = self._param_dict.format(param, value)
        except KeyError:
            raise InstrumentParameterException('Could not format param %s' % param)

        return formatted_value


def create_playback_protocol(callback):
    return Protocol(None, None, callback)
