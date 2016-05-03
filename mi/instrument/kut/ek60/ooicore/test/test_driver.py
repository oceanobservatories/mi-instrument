"""
@package mi.instrument.kut.ek60.ooicore.test.test_driver
@file mi/instrument/kut/ek60/ooicore/driver.py
@author Richard Han
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""
import os
import ftplib

import time
import json
from mock import Mock

from nose.plugins.attrib import attr
from mi.core.log import get_logger
from mi.core.instrument.chunker import StringChunker

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase, ParameterTestConfigKey
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import AgentCapabilityType


from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.instrument.instrument_driver import DriverConfigKey, ResourceAgentState

from mi.instrument.kut.ek60.ooicore.driver import InstrumentDriver, ZPLSCStatusParticleKey
from mi.instrument.kut.ek60.ooicore.driver import DataParticleType
from mi.instrument.kut.ek60.ooicore.driver import Command
from mi.instrument.kut.ek60.ooicore.driver import ProtocolState
from mi.instrument.kut.ek60.ooicore.driver import ProtocolEvent
from mi.instrument.kut.ek60.ooicore.driver import Capability
from mi.instrument.kut.ek60.ooicore.driver import Parameter
from mi.instrument.kut.ek60.ooicore.driver import Protocol
from mi.instrument.kut.ek60.ooicore.driver import Prompt
from mi.instrument.kut.ek60.ooicore.driver import ZPLSCStatusParticle
from mi.instrument.kut.ek60.ooicore.driver import NEWLINE
from mi.instrument.kut.ek60.ooicore.zplsc_b import ZplscBParticleKey

log = get_logger()

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'


###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.kut.ek60.ooicore.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='613CAW',
    instrument_agent_name='kut_ek60_ooicore',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={
        DriverConfigKey.PARAMETERS: {
            Parameter.FTP_IP_ADDRESS: '128.193.64.201',
            Parameter.FTP_PORT: '80',
            Parameter.SCHEDULE: "# Default schedule file" + NEWLINE +
                                "---" + NEWLINE +
                                "file_prefix:    \"DEFAULT\"" + NEWLINE +
                                "file_path:      \"DEFAULT\"" + NEWLINE +
                                "max_file_size:   52428800" + NEWLINE +
                                "intervals: " + NEWLINE +
                                "    -   name: \"default\"" + NEWLINE +
                                "        type: \"constant\"" + NEWLINE +
                                "        start_at:  \"00:00\"" + NEWLINE +
                                "        duration:  \"00:01:30\"" + NEWLINE +
                                "        repeat_every:   \"00:10\"" + NEWLINE +
                                "        stop_repeating_at: \"23:55\"" + NEWLINE +
                                "        interval:   1000" + NEWLINE +
                                "        max_range:  220" + NEWLINE +
                                "        frequency: " + NEWLINE +
                                "          38000: " + NEWLINE +
                                "              mode:   active" + NEWLINE +
                                "              power:  100 " + NEWLINE +
                                "              pulse_length:   256" + NEWLINE +
                                "          120000: " + NEWLINE +
                                "              mode:   active " + NEWLINE +
                                "              power:  100 " + NEWLINE +
                                "              pulse_length:   256" + NEWLINE +
                                "          200000: " + NEWLINE +
                                "              mode:   active " + NEWLINE +
                                "              power:  120 " + NEWLINE +
                                "              pulse_length:   256" + NEWLINE +
                                "...",
        }
    }
)

zplsc_status_particle_01 = [{DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CONNECTED, DataParticleKey.VALUE: 1},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_MODE, DataParticleKey.VALUE: 'active'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_POWER, DataParticleKey.VALUE: 120.0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_PULSE_LENGTH, DataParticleKey.VALUE: 0.000256},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_SAMPLE_INTERVAL, DataParticleKey.VALUE: 6.4e-05},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_MODE, DataParticleKey.VALUE: 'active'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_POWER, DataParticleKey.VALUE: 100.0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_PULSE_LENGTH, DataParticleKey.VALUE: 0.000256},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_SAMPLE_INTERVAL, DataParticleKey.VALUE: 6.4e-05},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_MODE, DataParticleKey.VALUE: 'active'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_POWER, DataParticleKey.VALUE: 100.0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_PULSE_LENGTH, DataParticleKey.VALUE: 0.000256},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_SAMPLE_INTERVAL, DataParticleKey.VALUE: 6.4e-05},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_UTC_TIME, DataParticleKey.VALUE: '2014-07-29 15:49:11.789000'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_EXECUTABLE, DataParticleKey.VALUE: 'c:/users/ooi/desktop/er60.lnk'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FS_ROOT, DataParticleKey.VALUE: 'D:/'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_NEXT_SCHEDULED_INTERVAL, DataParticleKey.VALUE: '2014-07-29 00:00:00.000000'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_HOST, DataParticleKey.VALUE: '157.237.15.100'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_PID, DataParticleKey.VALUE: '0'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_PORT, DataParticleKey.VALUE: 52873},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILENAME, DataParticleKey.VALUE: 'DEFAULT-D20140728-T171009.raw'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILESIZE, DataParticleKey.VALUE: 0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FILE_PATH, DataParticleKey.VALUE: 'D:\\data\\QCT_1'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FILE_PREFIX, DataParticleKey.VALUE: 'OOI'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_MAX_FILE_SIZE, DataParticleKey.VALUE: 52428800},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAMPLE_RANGE, DataParticleKey.VALUE: 220.0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_BOTTOM, DataParticleKey.VALUE: 1},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_INDEX, DataParticleKey.VALUE: 1},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_RAW, DataParticleKey.VALUE: 1},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SCHEDULED_INTERVALS_REMAINING, DataParticleKey.VALUE: 144},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_GPTS_ENABLED, DataParticleKey.VALUE: 0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SCHEDULE_FILENAME, DataParticleKey.VALUE: 'driver_schedule.yaml'}]

zplsc_status_particle_04 = [{DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CONNECTED, DataParticleKey.VALUE: 0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_MODE, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_POWER, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_PULSE_LENGTH, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_SAMPLE_INTERVAL, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_MODE, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_POWER, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_PULSE_LENGTH, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_SAMPLE_INTERVAL, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_MODE, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_POWER, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_PULSE_LENGTH, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_SAMPLE_INTERVAL, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_UTC_TIME, DataParticleKey.VALUE: '2015-03-13 18:06:25.436000'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_EXECUTABLE, DataParticleKey.VALUE: 'c:/users/ooi/desktop/er60.lnk'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FS_ROOT, DataParticleKey.VALUE: 'D:/'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_NEXT_SCHEDULED_INTERVAL, DataParticleKey.VALUE: '2015-03-13 00:00:00.000000'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_HOST, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_PID, DataParticleKey.VALUE: '0'},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_PORT, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILENAME, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILESIZE, DataParticleKey.VALUE: 0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FILE_PATH, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_FILE_PREFIX, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_MAX_FILE_SIZE, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAMPLE_RANGE, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_BOTTOM, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_INDEX, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SAVE_RAW, DataParticleKey.VALUE: None},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SCHEDULED_INTERVALS_REMAINING, DataParticleKey.VALUE: 24},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_GPTS_ENABLED, DataParticleKey.VALUE: 0},
                            {DataParticleKey.VALUE_ID: ZPLSCStatusParticleKey.ZPLSC_SCHEDULE_FILENAME, DataParticleKey.VALUE: 'driver_schedule.yaml'}]

PORT_TIMESTAMP = 3558720820.531179
DRIVER_TIMESTAMP = 3555423722.711772

########################################################################
#                                                                             #
# Common capabilities in the base class                                       #
#                                                                             #
# Instrument specific stuff in the derived class                              #
#                                                                             #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
#                                                                             #
# Qualification tests are driven through the instrument_agent                 #
#                                                                             #
###############################################################################

###############################################################################
#                           DRIVER TEST MIXIN                                 #
#     Defines a set of constants and assert methods used for data particle    #
#     verification                                                            #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.                                      #
###############################################################################


class DriverTestMixinSub(DriverTestMixin):

    """
    Mixin class used for storing data particle constants and common data assertion methods.
    """

    FTP_IP_ADDRESS = "128.193.64.201"
    FTP_PORT = 80
    USER_NAME = "ooi"
    PASSWORD = "994ef22"

    InstrumentDriver = InstrumentDriver
    # Create some short names for the parameter test config

    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    TEST_USER_NAME = "ooi_user"
    TEST_USER_PASSWORD = "ooi_password"

    DEFAULT_SCHEDULE = "# Default schedule file" + NEWLINE + \
                       "---" + NEWLINE + \
                       "file_prefix:    \"DEFAULT\"" + NEWLINE + \
                       "file_path:      \"DEFAULT\"" + NEWLINE + \
                       "max_file_size:   52428800" + NEWLINE + \
                       "intervals: " + NEWLINE + \
                       "    -   name: \"default\"" + NEWLINE + \
                       "        type: \"constant\"" + NEWLINE + \
                       "        start_at:  \"00:00\"" + NEWLINE +  \
                       "        duration:  \"00:01:30\"" + NEWLINE + \
                       "        repeat_every:   \"00:10\"" + NEWLINE + \
                       "        stop_repeating_at: \"23:55\"" + NEWLINE +  \
                       "        interval:   1000" + NEWLINE + \
                       "        max_range:  220" + NEWLINE + \
                       "        frequency: " + NEWLINE + \
                       "          38000: " + NEWLINE + \
                       "              mode:   active" + NEWLINE + \
                       "              power:  100 " + NEWLINE + \
                       "              pulse_length:   256" + NEWLINE + \
                       "          120000: " + NEWLINE + \
                       "              mode:   active " + NEWLINE + \
                       "              power:  100 " + NEWLINE + \
                       "              pulse_length:   256" + NEWLINE + \
                       "          200000: " + NEWLINE + \
                       "              mode:   active " + NEWLINE + \
                       "              power:  120 " + NEWLINE + \
                       "              pulse_length:   256" + NEWLINE + \
                       "..."

    DEFAULT_SCHEDULE_2 = "# Default schedule file" + NEWLINE + \
                         "---" + NEWLINE + \
                         "file_prefix:    \"DEFAULT\"" + NEWLINE + \
                         "file_path:      \"DEFAULT\"" + NEWLINE + \
                         "max_file_size:   1228800" + NEWLINE + \
                         "intervals: " + NEWLINE + \
                         "    -   name: \"default\"" + NEWLINE + \
                         "        type: \"constant\"" + NEWLINE + \
                         "        start_at:  \"00:00\"" + NEWLINE +  \
                         "        duration:  \"00:15:00\"" + NEWLINE + \
                         "        repeat_every:   \"01:00\"" + NEWLINE + \
                         "        stop_repeating_at: \"23:55\"" + NEWLINE +  \
                         "        interval:   1000" + NEWLINE + \
                         "        max_range:  80" + NEWLINE + \
                         "        frequency: " + NEWLINE + \
                         "          38000: " + NEWLINE + \
                         "              mode:   active" + NEWLINE + \
                         "              power:  100 " + NEWLINE + \
                         "              pulse_length:   256" + NEWLINE + \
                         "          120000: " + NEWLINE + \
                         "              mode:   active " + NEWLINE + \
                         "              power:  100 " + NEWLINE + \
                         "              pulse_length:   64" + NEWLINE + \
                         "          200000: " + NEWLINE + \
                         "              mode:   active " + NEWLINE + \
                         "              power:  120 " + NEWLINE + \
                         "              pulse_length:   64" + NEWLINE + \
                         "..."

    TEST_SCHEDULE = "# QCT Example 5 configuration file" + NEWLINE + \
                    "---" + NEWLINE + \
                    "file_prefix:    \"OOI\"" + NEWLINE + \
                    "file_path:      \"DEFAULT\"       #relative to filesystem_root/data" + NEWLINE + \
                    "max_file_size:   52428800       #50MB in bytes:  50 * 1024 * 1024" + NEWLINE + \
                    "" + NEWLINE + \
                    "intervals:" + NEWLINE + \
                    " " + NEWLINE + \
                    "-   name: \"constant_38kHz_passive\"" + NEWLINE + \
                    "   type: \"constant\"" + NEWLINE + \
                    "   start_at:  \"00:00\"" + NEWLINE + \
                    "    duration:  \"00:01:30\"" + NEWLINE + \
                    "    repeat_every:   \"00:05\"" + NEWLINE + \
                    "    stop_repeating_at: \"23:55\"" + NEWLINE + \
                    "    interval:   1000" + NEWLINE + \
                    "   max_range:  150" + NEWLINE + \
                    "   frequency:" + NEWLINE + \
                    "     38000:" + NEWLINE + \
                    "         mode:   passive" + NEWLINE + \
                    "         power:  100" + NEWLINE + \
                    "         pulse_length:   256" + NEWLINE + \
                    "      120000:" + NEWLINE + \
                    "         mode:   active" + NEWLINE + \
                    "         power:  100" + NEWLINE + \
                    "         pulse_length:   64" + NEWLINE + \
                    "     200000:" + NEWLINE + \
                    "         mode:   active" + NEWLINE + \
                    "         power:  120" + NEWLINE + \
                    "         pulse_length:   64" + NEWLINE + \
                    "..."

    INVALID_STATUS = "This is an invalid status; it had better cause an exception."

    VALID_STATUS_03 = '{\"schedule_filename\": \"driver_schedule.yaml\", \"schedule\": {\"max_file_size\": 52428800, \"intervals\": [{\"max_range\": 220, \"start_at\": \"00:00\", \
\"name\": \"default\", \"interval\": 1000, \"frequency\": {\"200000\": {\"bandwidth\": 10635, \"pulse_length\": 256, \"mode\": \"active\", \"power\": 120, \
\"sample_interval\": 64}, \"38000\": {\"bandwidth\": 3675.35, \"pulse_length\": 256, \"mode\": \"active\", \"power\": 100, \"sample_interval\": 64}, \
\"120000\": {\"bandwidth\": 8709.93, \"pulse_length\": 256, \"mode\": \"active\", \"power\": 100, \"sample_interval\": 64}}, \"duration\": \"00:01:30\", \
\"stop_repeating_at\": \"23:55\", \"type\": \"constant\"}], \"file_path\": \"DEFAULT\", \"file_prefix\": \"DEFAULT\"}, \
\"er60_channels\": {\"GPT 200 kHz 00907207b7b1 6-2 OOI38|200\": {\"pulse_length\": 0.000256, \"frequency\": 200000, \"sample_interval\": 6.4e-05, \
\"power\": 120.0, \"mode\": \"active\"}, \"GPT 120 kHz 00907207b7dc 1-1 ES120-7CD\": {\"pulse_length\": 0.000256, \"frequency\": 120000, \"sample_interval\": 6.4e-05, \
\"power\": 100.0, \"mode\": \"active\"}, \"GPT  38 kHz 00907207b7b1 6-1 OOI.38|200\": {\"pulse_length\": 0.000256, \"frequency\": 38000, \"sample_interval\": 6.4e-05, \
\"power\": 100.0, \"mode\": \"active\"}}, \"gpts_enabled\": false, \"er60_status\": {\"executable\": \"c:/users/ooi/desktop/er60.lnk\", \"current_utc_time\": \"2014-07-28 21:38:01.756000\", \
\"current_running_interval\": null, \"pid\": null, \"host\": \"157.237.15.100\", \"scheduled_intervals_remaining\": 144, \"next_scheduled_interval\": \"2014-07-28 00:00:00.000000\", \
\"raw_output": {\"max_file_size\": 52428800, \"sample_range\": 220.0, \"file_prefix\": \"DEFAULT\", \"save_raw\": true, \"current_raw_filesize\": null, \"save_index\": true, \"save_bottom\": true, \
\"current_raw_filename\": \"DEFAULT-D20140728-T171009.raw\", \"file_path\": \"D:\\\\data\\\\DEFAULT\"} \
\"message\": \"ER60.exe successfully terminated:  SUCCESS: Sent termination signal to process with PID 2916, child of PID 2888.\", \"fs_root\": \"D:/\", \"port\": 52873}, \"connected\": false}'

    VALID_STATUS_01 = '{"schedule_filename": "driver_schedule.yaml", "schedule": {"max_file_size": 52428800, "intervals": [{"max_range": 220, "start_at": "00:00", "name": "default", "interval": 1000, "frequency": {"200000": {"bandwidth": 10635, "pulse_length": 256, "mode": "active", "power": 120, "sample_interval": 64}, "38000": {"bandwidth": 3675.35, "pulse_length": 256, "mode": "active", "power": 100, "sample_interval": 64}, "120000": {"bandwidth": 8709.93, "pulse_length": 256, "mode": "active", "power": 100, "sample_interval": 64}}, "duration": "00:01:30", "stop_repeating_at": "23:55", "type": "constant"}], "file_path": "QCT_1", "file_prefix": "OOI"}, "er60_channels": {"GPT 200 kHz 00907207b7b1 6-2 OOI38|200": {"pulse_length": 0.000256, "frequency": 200000, "sample_interval": 6.4e-05, "power": 120.0, "mode": "active"}, "GPT 120 kHz 00907207b7dc 1-1 ES120-7CD": {"pulse_length": 0.000256, "frequency": 120000, "sample_interval": 6.4e-05, "power": 100.0, "mode": "active"}, "GPT  38 kHz 00907207b7b1 6-1 OOI.38|200": {"pulse_length": 0.000256, "frequency": 38000, "sample_interval": 6.4e-05, "power": 100.0, "mode": "active"}}, "gpts_enabled": false, "er60_status": {"executable": "c:/users/ooi/desktop/er60.lnk", "current_utc_time": "2014-07-29 15:49:11.789000", "current_running_interval": null, "pid": null, "host": "157.237.15.100", "scheduled_intervals_remaining": 144, "next_scheduled_interval": "2014-07-29 00:00:00.000000", "raw_output": {"max_file_size": 52428800, "sample_range": 220.0, "file_prefix": "OOI", "save_raw": true, "current_raw_filesize": null, "save_index": true, "save_bottom": true, "current_raw_filename": "DEFAULT-D20140728-T171009.raw", "file_path": "D:\\\\data\\\\QCT_1"}, "message": "ER60.exe successfully terminated:  SUCCESS: Sent termination signal to process with PID 2916, child of PID 2888.", "fs_root": "D:/", "port": 52873}, "connected": true}'
    VALID_STATUS_04 = '{"schedule_filename": "driver_schedule.yaml", "schedule": {"max_file_size": 288, "intervals": [{"max_range": 80, "start_at": "00:00", "name": "default", "interval": 1000, "frequency": {"200000": {"power": 120, "bandwidth": 18760, "mode": "active", "pulse_length": 64, "sample_interval": 16}, "38000": {"power": 100, "bandwidth": 3675.35, "mode": "active", "pulse_length": 256, "sample_interval": 64}, "120000": {"power": 100, "bandwidth": 11800.1, "mode": "active", "pulse_length": 64, "sample_interval": 16}}, "duration": "00:15:00", "stop_repeating_at": "23:55", "type": "constant"}], "file_path": "DEFAULT_FILE_PATH", "file_prefix": "Driver DEFAULT CONFIG_PREFIX"}, "er60_channels": {}, "gpts_enabled": false, "er60_status": {"executable": "c:/users/ooi/desktop/er60.lnk", "current_utc_time": "2015-03-13 18:06:25.436000", "current_running_interval": null, "pid": null, "next_scheduled_interval": "2015-03-13 00:00:00.000000", "scheduled_intervals_remaining": 24, "host": null, "raw_output": {"max_file_size": null, "sample_range": null, "file_prefix": null, "save_raw": null, "current_raw_filesize": null, "save_index": null, "save_bottom": null, "current_raw_filename": null, "file_path": null}, "fs_root": "D:/", "port": null}, "connected": false}'

    VALID_STATUS_02 = \
        "{'connected': True," + NEWLINE + \
        "         'er60_channels': {'GPT  38 kHz 00907207b7b1 6-1 OOI.38|200': {'frequency': 38000," + NEWLINE + \
        "                                                                       'mode': 'active'," + NEWLINE + \
        "                                                                       'power': 100.0," + NEWLINE + \
        "                                                                       'pulse_length': 0.000256," + NEWLINE + \
        "                                                                       'sample_interval': 6.4e-05}," + NEWLINE + \
        "                           'GPT 120 kHz 00907207b7dc 1-1 ES120-7CD': {'frequency': 120000," + NEWLINE + \
        "                                                                      'mode': 'active'," + NEWLINE + \
        "                                                                      'power': 100.0," + NEWLINE + \
        "                                                                      'pulse_length': 6.4e-05," + NEWLINE +  \
        "                                                                      'sample_interval': 1.6e-05}," + NEWLINE + \
        "                           'GPT 200 kHz 00907207b7b1 6-2 OOI38|200': {'frequency': 200000," + NEWLINE + \
        "                                                                      'mode': 'active'," + NEWLINE + \
        "                                                                     'power': 120.0," + NEWLINE + \
        "                                                                      'pulse_length': 6.4e-05," + NEWLINE + \
        "                                                                      'sample_interval': 1.6e-05}}," + NEWLINE + \
        "         'er60_status': {'current_running_interval': null," + NEWLINE + \
        "                         'current_utc_time': '2014-07-09 01:23:39.691000'," + NEWLINE + \
        "                         'executable': 'c:/users/ooi/desktop/er60.lnk'," + NEWLINE + \
        "                         'fs_root': 'D:/'," + NEWLINE + \
        "                         'host': '157.237.15.100'," + NEWLINE + \
        "                         'next_scheduled_interval': null," + NEWLINE + \
        "                         'pid': 1864," + NEWLINE + \
        "                         'port': 56635," + NEWLINE + \
        "                         'raw_output': {'current_raw_filename': 'OOI-D20140707-T214500.raw'," + NEWLINE + \
        "                                        'current_raw_filesize': 0," + NEWLINE + \
        "                                       'file_path': 'D:\\data\\QCT_1'," + NEWLINE + \
        "                                        'file_prefix': 'OOI'," + NEWLINE + \
        "                                        'max_file_size': 52428800," + NEWLINE + \
        "                                        'sample_range': 220.0," + NEWLINE +  \
        "                                        'save_bottom': True," + NEWLINE + \
        "                                        'save_index': True," + NEWLINE + \
        "                                        'save_raw': True}," + NEWLINE + \
        "                         'scheduled_intervals_remaining': 0}," + NEWLINE + \
        "         'gpts_enabled': false," + NEWLINE + \
        "         'schedule': {}," + NEWLINE + \
        "         'schedule_filename': 'qct_configuration_example_1.yaml'}"

    _driver_capabilities = {
        # capabilities defined in the IOS
        Capability.DISCOVER: {STATES: [ProtocolState.UNKNOWN]},
        Capability.START_AUTOSAMPLE: {STATES: [ProtocolState.COMMAND]},
        Capability.STOP_AUTOSAMPLE: {STATES: [ProtocolState.AUTOSAMPLE]},
        Capability.ACQUIRE_STATUS: {STATES: [ProtocolState.COMMAND]}
    }

    ###
    #  Parameter and Type Definitions
    ###

    _driver_parameters = {
        # Parameters defined in the IOS

        Parameter.SCHEDULE: {TYPE: str, READONLY: False, STARTUP: True, DEFAULT: DEFAULT_SCHEDULE, VALUE: DEFAULT_SCHEDULE},
        Parameter.FTP_IP_ADDRESS: {TYPE: str, READONLY: False, STARTUP: True, DEFAULT: FTP_IP_ADDRESS, VALUE: FTP_IP_ADDRESS},
        Parameter.FTP_PORT: {TYPE: str, READONLY: False, STARTUP: True, DEFAULT: FTP_PORT, VALUE: FTP_PORT},
        Parameter.FTP_USERNAME: {TYPE: str, READONLY: False, STARTUP: True, DEFAULT: USER_NAME, VALUE: USER_NAME},
        Parameter.FTP_PASSWORD: {TYPE: str, READONLY: False, STARTUP: True, DEFAULT: PASSWORD, VALUE: PASSWORD},
    }

    _sample_parameters = {
        ZPLSCStatusParticleKey.ZPLSC_CONNECTED: {TYPE: bool, VALUE: True, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_MODE: {TYPE: unicode, VALUE: 'active', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_POWER: {TYPE: float, VALUE: 100.0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_PULSE_LENGTH: {TYPE: float, VALUE: 0.000256, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_120K_SAMPLE_INTERVAL: {TYPE: float, VALUE: 6.4e-05, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_MODE: {TYPE: unicode, VALUE: 'active', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_POWER: {TYPE: float, VALUE: 120.0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_PULSE_LENGTH: {TYPE: float, VALUE: 0.000256, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_200K_SAMPLE_INTERVAL: {TYPE: float, VALUE: 6.4e-05, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_MODE: {TYPE: unicode, VALUE: 'active', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_POWER: {TYPE: float, VALUE: 100.0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_PULSE_LENGTH: {TYPE: float, VALUE: 0.000256, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_ACTIVE_38K_SAMPLE_INTERVAL: {TYPE: float, VALUE: 6.4e-05, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_CURRENT_UTC_TIME: {TYPE: unicode, VALUE: '2014-07-29 15:49:11.789000', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_EXECUTABLE: {TYPE: unicode, VALUE: 'c:/users/ooi/desktop/er60.lnk', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_FS_ROOT: {TYPE: unicode, VALUE: 'D:/', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_NEXT_SCHEDULED_INTERVAL: {TYPE: unicode, VALUE: '2014-07-29 00:00:00.000000', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_HOST: {TYPE: unicode, VALUE: '157.237.15.100', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_PID: {TYPE: int, VALUE: 0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_PORT: {TYPE: int, VALUE: 52873, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILENAME: {TYPE: unicode, VALUE: 'DEFAULT-D20140728-T171009.raw', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_CURRENT_RAW_FILESIZE: {TYPE: int, VALUE: 0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_FILE_PATH: {TYPE: unicode, VALUE: 'D:\\data\\QCT_1', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_FILE_PREFIX: {TYPE: unicode, VALUE: 'OOI', REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_MAX_FILE_SIZE: {TYPE: int, VALUE: 52428800, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SAMPLE_RANGE: {TYPE: float, VALUE:  220.0, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SAVE_BOTTOM: {TYPE: bool, VALUE: True, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SAVE_INDEX: {TYPE: bool, VALUE: True, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SAVE_RAW: {TYPE: bool, VALUE: True, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SCHEDULED_INTERVALS_REMAINING: {TYPE: int, VALUE: 144, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_GPTS_ENABLED: {TYPE: bool, VALUE: False, REQUIRED: True},
        ZPLSCStatusParticleKey.ZPLSC_SCHEDULE_FILENAME: {TYPE: unicode, VALUE: 'driver_schedule.yaml', REQUIRED: True}
    }

    base_path = os.path.dirname(os.path.dirname(__file__))
    resource_dir = os.path.join(base_path, 'resource')
    input_file = os.path.join(resource_dir, 'OOI-D20141212-T152500.raw')
    _test_file_notice = 'downloaded file:' + input_file

    ouptut_file_1 = os.path.join(resource_dir, 'OOI-D20141212-T152500_38k.png')
    ouptut_file_2 = os.path.join(resource_dir, 'OOI-D20141212-T152500_120k.png')
    ouptut_file_3 = os.path.join(resource_dir, 'OOI-D20141212-T152500_200k.png')

    _metadata_dict = {
        ZplscBParticleKey.FILE_TIME: {'type': str, 'value': 2},
        ZplscBParticleKey.ECHOGRAM_PATH: {'type': list, 'value': [ouptut_file_1, ouptut_file_2, ouptut_file_3]},
        ZplscBParticleKey.CHANNEL: {'type': list, 'value': [1, 2, 3]},
        ZplscBParticleKey.TRANSDUCER_DEPTH: {'type': list, 'value': [0.0, 0.0, 0.0]},
        ZplscBParticleKey.FREQUENCY: {'type': list, 'value': [120000.0, 38000.0, 200000.0]},
        ZplscBParticleKey.TRANSMIT_POWER: {'type': list, 'value': [25.0, 100.0, 25.0]},
        ZplscBParticleKey.PULSE_LENGTH: {'type': list, 'value': [0.000256, 0.001024, 0.0000634]},
        ZplscBParticleKey.BANDWIDTH: {'type': list, 'value': [8709.9277, 2425.149685, 10635.04492]},
        ZplscBParticleKey.SAMPLE_INTERVAL: {'type': list, 'value': [0.000064, 0.000256, 0.000064]},
        ZplscBParticleKey.SOUND_VELOCITY: {'type': list, 'value': [1493.8888, 1493.888, 1493.888]},
        ZplscBParticleKey.ABSORPTION_COEF: {'type': list, 'value': [0.03744, 0.00979, 0.052688]},
        ZplscBParticleKey.TEMPERATURE: {'type': list, 'value': [10.0, 10.0, 10.0]}
    }

    def assert_particle_sample(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  ZPLSCStatusParticle status particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(ZPLSCStatusParticleKey, self._sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.RAW)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_status_particle(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  ZPLSCDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(ZPLSCStatusParticleKey, self._sample_parameters)
        self.assert_data_particle_header(data_particle, DataParticleType.ZPLSC_STATUS)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters, verify_values)

    def assert_driver_parameters(self, current_parameters, verify_values=False):
        """
        Verify that all driver parameters are correct and potentially verify values.
        @param current_parameters: driver parameters read from the driver instance
        @param verify_values: should we verify values against definition?
        """
        self.assert_parameters(current_parameters, self._driver_parameters, verify_values)

    def assert_file_data(self, data_particle, verify_values=True):
        """
        Verify ZIPLSC file data particle
        @param data_particle:
        @param verify_values: bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, DataParticleType.METADATA)
        self.assert_data_particle_parameters(data_particle, self._metadata_dict)  # , verify_values


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
#                                                                             #
#   These tests are especially useful for testing parsers and other data      #
#   handling.  The tests generally focus on small segments of code, like a    #
#   single function call, but more complex code using Mock objects.  However  #
#   if you find yourself mocking too much maybe it is better as an            #
#   integration test.                                                         #
#                                                                             #
#   Unit tests do not start up external processes like the port agent or      #
#   driver process.                                                           #
###############################################################################
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, DriverTestMixinSub):

    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilites
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(Command())
        self.assert_enum_has_no_duplicates(ZPLSCStatusParticleKey())

        # Test capabilites for duplicates, then verify that capabilities is a subset of proto events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_driver_schema(self):
        """
        get the driver schema and verify it is configured properly
        """
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_driver_schema(driver, self._driver_parameters, self._driver_capabilities)

    def test_capabilities(self):
        """
        Verify the FSM reports capabilities as expected.  All states defined in this dict must
        also be defined in the protocol FSM.
        """
        capabilities = {
            ProtocolState.COMMAND: ['DRIVER_EVENT_START_AUTOSAMPLE',
                                    'DRIVER_EVENT_GET',
                                    'DRIVER_EVENT_SET',
                                    'DRIVER_EVENT_ACQUIRE_STATUS'],
            ProtocolState.AUTOSAMPLE: ['DRIVER_EVENT_STOP_AUTOSAMPLE',
                                       'DRIVER_EVENT_GET'],
            ProtocolState.UNKNOWN: ['DRIVER_EVENT_DISCOVER']
        }
        driver = self.InstrumentDriver(self._got_data_event_callback)
        self.assert_capabilities(driver, capabilities)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities. Iterate through available capabilities,
        and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(Prompt, NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))

    def test_zplsc_status_sample_format(self):
        """
        Verify driver can get status data out in a reasonable format.
        Parsed is all we care about...raw is tested in the base DataParticle tests.
        """

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: DataParticleType.ZPLSC_STATUS,
            DataParticleKey.PORT_TIMESTAMP: PORT_TIMESTAMP,
            DataParticleKey.DRIVER_TIMESTAMP: DRIVER_TIMESTAMP,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: zplsc_status_particle_01}

        self.maxDiff = None

        data = json.loads(self.VALID_STATUS_01)
        self.compare_parsed_data_particle(ZPLSCStatusParticle, data, expected_particle)

        expected_particle[DataParticleKey.VALUES] = zplsc_status_particle_04
        data = json.loads(self.VALID_STATUS_04)
        self.compare_parsed_data_particle(ZPLSCStatusParticle, data, expected_particle)

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)

        self.assert_chunker_sample(chunker, self._test_file_notice)
        self.assert_chunker_sample_with_noise(chunker, self._test_file_notice)
        self.assert_chunker_fragmented_sample(chunker, self._test_file_notice, 5)
        self.assert_chunker_combined_sample(chunker, self._test_file_notice)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver)

        self.assert_raw_particle_published(driver, True)

        # Start validating data particles

        self.assert_particle_published(driver, self._test_file_notice, self.assert_file_data, True)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, DriverTestMixinSub):
    def setUp(self):
        time.sleep(30)
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_connect(self):
        """
        Test configuring and connecting to the device through the port
        agent. Discover device state.
        """
        self.assert_initialize_driver()

    def test_state_transition(self):
        """
        Tests to see if we can make transition to different states
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_state_change(ProtocolState.COMMAND, 3)

        # Test transition to auto sample
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_AUTOSAMPLE)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 3)

        # Test transition back to command state
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_state_change(ProtocolState.COMMAND, 3)

    def test_parameters(self):
        """
        Test driver parameters and verify their type.  Startup parameters also verify the parameter
        value.  This test confirms that parameters are being read/converted properly and that
        the startup has been applied.
        """
        self.assert_initialize_driver()
        reply = self.driver_client.cmd_dvr('get_resource', Parameter.ALL)
        log.debug("reply = %s", reply)
        self.assert_driver_parameters(reply, True)

    def test_set_params(self):
        """
        Test get driver parameters and verify their initial values.
        """
        self.assert_initialize_driver()

        # verify we can set FTP_IP_ADDRESS param
        self.assert_set(Parameter.FTP_IP_ADDRESS, "128.193.64.111")

        # Need to set the FTP IP address back to the working one so that
        # the file transfer of a schedule file would work.
        self.assert_set(Parameter.FTP_IP_ADDRESS, self.FTP_IP_ADDRESS)

        # verify we can set SCHEDULE param
        self.assert_set(Parameter.SCHEDULE, DriverTestMixinSub.TEST_SCHEDULE)

        self.assert_set(Parameter.FTP_USERNAME, DriverTestMixinSub.TEST_USER_NAME)

        self.assert_set(Parameter.FTP_PASSWORD, DriverTestMixinSub.TEST_USER_PASSWORD)

    def test_acquire_status(self):
        """
        Test acquire status command which generates a status particle
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        # test acquire_status particles
        self.assert_particle_generation(ProtocolEvent.ACQUIRE_STATUS,
                                        DataParticleType.ZPLSC_STATUS,
                                        self.assert_status_particle,
                                        delay=10)

    def ftp_remove_files(self, ftp_session):
        """
        Remove all files in the current directory
        """

        dir_files = ftp_session.nlst()

        log.debug("nlst = %s", dir_files)
        for the_file in dir_files:
            ftp_session.delete(the_file)

    def ftp_login(self):
        host = self.FTP_IP_ADDRESS
        try:
            ftp_session = ftplib.FTP()
            ftp_session.connect(host)
            ftp_session.login(self.USER_NAME, self.PASSWORD, "")
            log.debug("ftp session was created")
            return ftp_session

        except (ftplib.socket.error, ftplib.socket.gaierror), e:
            log.error("ERROR: cannot reach FTP Host %s " % host)
            return

    def test_autosample_on(self):
        """
        Test for turning auto sample data on
        """
        log.debug("Start test_autosample_on: ")

        self.assert_initialize_driver(ProtocolState.COMMAND)

        ftp_session = self.ftp_login()

        res = ftp_session.pwd()
        log.debug(" current working dir  = %s", res)
        ftp_session.cwd("/data/DEFAULT")

        # Remove all existing raw files from The data/DEFAULT directory before starting autosample test
        log.debug(" Remove all files in the DEFAULT directory")
        self.ftp_remove_files(ftp_session)
        ftp_session.quit()

        log.debug("Start autosample")
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_AUTOSAMPLE)

        time.sleep(800)

        log.debug(" Stop Autosample")
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_AUTOSAMPLE)

        ftp_session = self.ftp_login()
        res = ftp_session.pwd()
        log.debug(" current working dir before = %s", res)
        ftp_session.cwd("/data/DEFAULT")

        res = ftp_session.pwd()
        log.debug(" current working dir after = %s", res)

        # Verify that raw files are generated in the /data/DEFAULT directory
        entries = []
        res = ftp_session.dir(entries.append)
        log.debug("Default directory files")
        if not entries:
            log.debug("Default directory is empty")
            self.fail("Autosample Failed: No raw files were generated")

        for entry in entries:
            log.debug("file generated = %s", entry)

        ftp_session.quit()


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase, DriverTestMixinSub):
    def setUp(self):
        time.sleep(50)
        InstrumentDriverQualificationTestCase.setUp(self)

    def test_discover(self):
        """
        over-ridden because the driver will always go to command mode
        during the discover process after a reset.

        """
        # Verify the agent is in command mode
        self.assert_enter_command_mode()

        # Now reset and try to discover.  This will stop the driver and cause it to re-discover which
        # will always go back to command for this instrument
        self.assert_reset()
        self.assert_discover(ResourceAgentState.COMMAND)

    def test_status_particles(self):
        """
        Verify status particle in autosample state and in Command state
        """

        self.assert_enter_command_mode()

        self.assert_particle_polled(Capability.ACQUIRE_STATUS, self.assert_status_particle,
                                    DataParticleType.ZPLSC_STATUS, timeout=10)

    def test_get_set_parameters(self):
        """
        verify that all parameters can be get and set properly
        """
        self.assert_enter_command_mode()

        self.assert_get_parameter(Parameter.FTP_IP_ADDRESS, self.FTP_IP_ADDRESS)
        self.assert_get_parameter(Parameter.SCHEDULE, self.DEFAULT_SCHEDULE)

        self.assert_set_parameter(Parameter.FTP_IP_ADDRESS,  "128.193.68.215")
        self.assert_set_parameter(Parameter.FTP_IP_ADDRESS, self.FTP_IP_ADDRESS)
        self.assert_set_parameter(Parameter.SCHEDULE, self.TEST_SCHEDULE)

        self.assert_set_parameter(Parameter.FTP_USERNAME, self.TEST_USER_NAME)
        self.assert_set_parameter(Parameter.FTP_USERNAME, self.USER_NAME)

        self.assert_set_parameter(Parameter.FTP_PASSWORD, self.TEST_USER_PASSWORD)
        self.assert_set_parameter(Parameter.FTP_PASSWORD, self.PASSWORD)

        self.assert_reset()

    def test_get_capabilities(self):
        """
        @brief Verify that the correct capabilities are returned from get_capabilities
        at various driver/agent states.
        """

        self.assert_enter_command_mode()

        ##################
        #  Command Mode
        ##################

        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.COMMAND),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: [
                ProtocolEvent.GET,
                ProtocolEvent.SET,
                ProtocolEvent.START_AUTOSAMPLE,
                ProtocolEvent.ACQUIRE_STATUS,
            ],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()
        }

        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
            ProtocolEvent.STOP_AUTOSAMPLE,
        ]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()

        #######################
        #  Uninitialized Mode
        #######################
        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.UNINITIALIZED)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = []
        capabilities[AgentCapabilityType.RESOURCE_INTERFACE] = []
        capabilities[AgentCapabilityType.RESOURCE_PARAMETER] = []

        self.assert_reset()
        self.assert_capabilities(capabilities)
