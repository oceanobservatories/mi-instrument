"""
@package mi.instrument.nortek.test.test_driver
@file mi/instrument/nortek/test/test_driver.py
@author Steve Foley
@brief Common test code for Nortek drivers
"""
import base64
from binascii import unhexlify

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.instrument.instrument_protocol import InitializationType
from ooi.logging import log
from mi.idk.unit_test import InstrumentDriverUnitTestCase, ParameterTestConfigKey, InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.core.instrument.chunker import StringChunker
from mi.core.instrument.data_particle import DataParticleKey, DataParticleValue
from mi.core.instrument.instrument_driver import DriverConnectionState, DriverParameter, DriverConfigKey
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility, ParameterDictType
from mi.instrument.nortek.driver import common, EngineeringParameter, ParameterUnits
from mi.instrument.nortek.driver import UserConfigKey
from mi.instrument.nortek.driver import NortekInstrumentProtocol
from mi.instrument.nortek.driver import ScheduledJob
from mi.core.exceptions import InstrumentCommandException, InstrumentParameterException, SampleException
from mi.instrument.nortek.driver import (InstrumentPrompts, Parameter, ProtocolState,
                                         ProtocolEvent, InstrumentCommands, Capability)
from mi.instrument.nortek.user_configuration import UserConfigCompositeKey
import mi.instrument.nortek.particles as particles


__author__ = 'Rachel Manoni, Ronald Ronquillo'
__license__ = 'Apache 2.0'

InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.nortek.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='3DLE2A',
    instrument_agent_name='nortek_driver',
    instrument_agent_packet_config=None,
    driver_startup_config={}
)


VID = DataParticleKey.VALUE_ID
VAL = DataParticleKey.VALUE

hw_config_particle = [
    {VID: particles.NortekHardwareConfigDataParticleKey.SERIAL_NUM, VAL: "VEC 8181      "},
    {VID: particles.NortekHardwareConfigDataParticleKey.RECORDER_INSTALLED, VAL: 0},
    {VID: particles.NortekHardwareConfigDataParticleKey.COMPASS_INSTALLED, VAL: 0},
    {VID: particles.NortekHardwareConfigDataParticleKey.BOARD_FREQUENCY, VAL: 65535},
    {VID: particles.NortekHardwareConfigDataParticleKey.PIC_VERSION, VAL: 0},
    {VID: particles.NortekHardwareConfigDataParticleKey.HW_REVISION, VAL: 4},
    {VID: particles.NortekHardwareConfigDataParticleKey.RECORDER_SIZE, VAL: 144},
    {VID: particles.NortekHardwareConfigDataParticleKey.VELOCITY_RANGE, VAL: 0},
    {VID: particles.NortekHardwareConfigDataParticleKey.FW_VERSION, VAL: "3.36"}]


def hw_config_sample():
    sample_as_hex = "a505180056454320383138312020202020200400ffff00000400900004000000ffff0000ffffffff0000332e3336b0480606"
    return sample_as_hex.decode('hex')


def head_config_sample():
    sample_as_hex = "a50470003700701701005645432034393433000000000000000000000\
000992ac3eaabea0e001925dbda7805830589051cbd0d00822becff1dbf05fc222b4200a00f000\
00000ffff0000ffff0000ffff0000000000000000ffff0000010000000100000000000000fffff\
fff00000000ffff0100000000001900a2f65914c9050301d81b5a2a9d9ffefc35325d007b9e4ff\
f92324c00987e0afd48ff0afd547d2b01cffe3602ff7ffafff7fffaff000000000000000000000\
000000000009f14100e100e10275b0000000000000000000000000000000000000000000300065b0606"
    return sample_as_hex.decode('hex')


head_config_particle = [
    {VID: particles.NortekHeadConfigDataParticleKey.PRESSURE_SENSOR, VAL: 1},
    {VID: particles.NortekHeadConfigDataParticleKey.MAG_SENSOR, VAL: 1},
    {VID: particles.NortekHeadConfigDataParticleKey.TILT_SENSOR, VAL: 1},
    {VID: particles.NortekHeadConfigDataParticleKey.TILT_SENSOR_MOUNT, VAL: 0},
    {VID: particles.NortekHeadConfigDataParticleKey.HEAD_FREQ, VAL: 6000},
    {VID: particles.NortekHeadConfigDataParticleKey.HEAD_TYPE, VAL: 1},
    {VID: particles.NortekHeadConfigDataParticleKey.HEAD_SERIAL, VAL: "VEC 4943"},
    {VID: particles.NortekHeadConfigDataParticleKey.SYSTEM_DATA,
     VAL: base64.b64encode(
         "\x00\x00\x00\x00\x00\x00\x00\x00\x99\x2a\xc3\xea\xab\xea\x0e\x00\x19\x25\xdb\
\xda\x78\x05\x83\x05\x89\x05\x1c\xbd\x0d\x00\x82\x2b\xec\xff\x1d\xbf\x05\xfc\
\x22\x2b\x42\x00\xa0\x0f\x00\x00\x00\x00\xff\xff\x00\x00\xff\xff\x00\x00\xff\
\xff\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x00\x00\x01\x00\x00\x00\x01\x00\
\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\xff\xff\x01\x00\x00\
\x00\x00\x00\x19\x00\xa2\xf6\x59\x14\xc9\x05\x03\x01\xd8\x1b\x5a\x2a\x9d\x9f\
\xfe\xfc\x35\x32\x5d\x00\x7b\x9e\x4f\xff\x92\x32\x4c\x00\x98\x7e\x0a\xfd\x48\
\xff\x0a\xfd\x54\x7d\x2b\x01\xcf\xfe\x36\x02\xff\x7f\xfa\xff\xf7\xff\xfa\xff\
\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9f\x14\x10\
\x0e\x10\x0e\x10\x27"),
     DataParticleKey.BINARY: True},
    {VID: particles.NortekHeadConfigDataParticleKey.NUM_BEAMS, VAL: 3}]


def user_config_sample():
    # Visually break it up a bit (full lines are 30 bytes wide)
    sample_as_hex = """
        a500 0001 0200 1000 0700 2c00 0002 0100 3c00 0300 8200 0000 cc4e 0000 0000
        0200 0100 0100 0700 5802 3439 3433 0000 0100 2642 2812 1209 c0a8 0000 3000
        1141 1400 0100 1400 0400 0000 2035 5e01
        023d 1e3d 393d 533d 6e3d 883d a23d bb3d d43d ed3d 063e 1e3e 363e 4e3e 653e
        7d3e 933e aa3e c03e d63e ec3e 023f 173f 2c3f 413f 553f 693f 7d3f 913f a43f
        b83f ca3f dd3f f03f 0240 1440 2640 3740 4940 5a40 6b40 7c40 8c40 9c40 ac40
        bc40 cc40 db40 ea40 f940 0841 1741 2541 3341 4241 4f41 5d41 6a41 7841 8541
        9241 9e41 ab41 b741 c341 cf41 db41 e741 f241 fd41 0842 1342 1e42 2842 3342
        3d42 4742 5142 5b42 6442 6e42 7742 8042 8942 9142 9a42 a242 aa42 b242 ba42
        3333 3035 2d30 3031 3036 5f30 3030 3031 5f32 3830 3932 3031 3200 0000 0000
        0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
        0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1e00
        5a00 5a00 bc02 3200 0000 0000 0000 0700 0000 0000 0000 0000 0000 0000 0100
        0000 0000 2a00 0000 0200 1400 ea01 1400 ea01 0a00 0500 0000 4000 4000 0200
        0f00 5a00 0000 0100 c800 0000 0000 0f00 ea01 ea01 0000 0000 0000 0000 0000
        0712 0080 0040 0000 0000 0000 8200 0000 0a00 0800 b12b 0000 0000 0200 0600
        0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0aff
        cdff 8b00 e500 ee00 0b00 84ff 3dff a7ff 0606"""

    return sample_as_hex.translate(None, ' \n').decode('hex')


user_config_particle = [{VID: UserConfigKey.TX_LENGTH, VAL: 2},
                        {VID: UserConfigKey.BLANK_DIST, VAL: 16},
                        {VID: UserConfigKey.RX_LENGTH, VAL: 7},
                        {VID: UserConfigKey.TIME_BETWEEN_PINGS, VAL: 44},
                        {VID: UserConfigKey.TIME_BETWEEN_BURSTS, VAL: 512},
                        {VID: UserConfigKey.NUM_PINGS, VAL: 1},
                        {VID: UserConfigKey.AVG_INTERVAL, VAL: 60},
                        {VID: UserConfigKey.NUM_BEAMS, VAL: 3},
                        {VID: UserConfigKey.PROFILE_TYPE, VAL: 1},
                        {VID: UserConfigKey.MODE_TYPE, VAL: 0},
                        {VID: UserConfigKey.POWER_TCM1, VAL: 0},
                        {VID: UserConfigKey.POWER_TCM2, VAL: 0},
                        {VID: UserConfigKey.SYNC_OUT_POSITION, VAL: 1},
                        {VID: UserConfigKey.SAMPLE_ON_SYNC, VAL: 0},
                        {VID: UserConfigKey.START_ON_SYNC, VAL: 0},
                        {VID: UserConfigKey.POWER_PCR1, VAL: 0},
                        {VID: UserConfigKey.POWER_PCR2, VAL: 0},
                        {VID: UserConfigKey.COMPASS_UPDATE_RATE, VAL: 2},
                        {VID: UserConfigKey.COORDINATE_SYSTEM, VAL: 1},
                        {VID: UserConfigKey.NUM_CELLS, VAL: 1},
                        {VID: UserConfigKey.CELL_SIZE, VAL: 7},
                        {VID: UserConfigKey.MEASUREMENT_INTERVAL, VAL: 600},
                        {VID: UserConfigKey.DEPLOYMENT_NAME, VAL: "4943"},
                        {VID: UserConfigKey.WRAP_MODE, VAL: 1},
                        {VID: UserConfigCompositeKey.DEPLOY_START_TIME,
                         VAL: [26, 42, 28, 12, 12, 9]},
                        {VID: UserConfigCompositeKey.DIAG_INTERVAL, VAL: 43200},
                        {VID: UserConfigKey.USE_SPEC_SOUND_SPEED, VAL: 0},
                        {VID: UserConfigKey.DIAG_MODE_ON, VAL: 0},
                        {VID: UserConfigKey.ANALOG_OUTPUT_ON, VAL: 0},
                        {VID: UserConfigKey.OUTPUT_FORMAT, VAL: 0},
                        {VID: UserConfigKey.SCALING, VAL: 1},
                        {VID: UserConfigKey.SERIAL_OUT_ON, VAL: 1},
                        {VID: UserConfigKey.STAGE_ON, VAL: 0},
                        {VID: UserConfigKey.ANALOG_POWER_OUTPUT, VAL: 0},
                        {VID: UserConfigKey.SOUND_SPEED_ADJUST, VAL: 16657},
                        {VID: UserConfigKey.NUM_DIAG_SAMPLES, VAL: 20},
                        {VID: UserConfigKey.NUM_BEAMS_PER_CELL, VAL: 1},
                        {VID: UserConfigKey.NUM_PINGS_DIAG, VAL: 20},
                        {VID: UserConfigKey.USE_DSP_FILTER, VAL: 0},
                        {VID: UserConfigKey.FILTER_DATA_OUTPUT, VAL: 0},
                        {VID: UserConfigKey.ANALOG_INPUT_ADDR, VAL: 0},
                        {VID: UserConfigKey.SW_VER, VAL: 13600},
                        {VID: UserConfigCompositeKey.VELOCITY_ADJ_FACTOR, VAL:
                            "Aj0ePTk9Uz1uPYg9oj27PdQ97T0GPh4+Nj5OPmU+fT6TPqo+wD7WPuw+Aj8XPyw/QT9VP2k/fT+RP6Q/uD/KP90/"
                            "8D8CQBRAJkA3QElAWkBrQHxAjECcQKxAvEDMQNtA6kD5QAhBF0ElQTNBQkFPQV1BakF4QYVBkkGeQatBt0HDQc9B20"
                            "HnQfJB/UEIQhNCHkIoQjNCPUJHQlFCW0JkQm5Cd0KAQolCkUKaQqJCqkKyQrpC"},
                        {VID: UserConfigKey.FILE_COMMENTS,
                         VAL: "3305-00106_00001_28092012"},
                        {VID: UserConfigKey.WAVE_DATA_RATE, VAL: 1},
                        {VID: UserConfigKey.WAVE_CELL_POS, VAL: 1},
                        {VID: UserConfigKey.DYNAMIC_POS_TYPE, VAL: 1},
                        {VID: UserConfigKey.PERCENT_WAVE_CELL_POS, VAL: 32768},
                        {VID: UserConfigKey.WAVE_TX_PULSE, VAL: 16384},
                        {VID: UserConfigKey.FIX_WAVE_BLANK_DIST, VAL: 0},
                        {VID: UserConfigKey.WAVE_CELL_SIZE, VAL: 0},
                        {VID: UserConfigKey.NUM_DIAG_PER_WAVE, VAL: 0},
                        {VID: UserConfigKey.NUM_SAMPLE_PER_BURST, VAL: 10},
                        {VID: UserConfigKey.ANALOG_SCALE_FACTOR, VAL: 11185},
                        {VID: UserConfigKey.CORRELATION_THRS, VAL: 0},
                        {VID: UserConfigKey.TX_PULSE_LEN_2ND, VAL: 2},
                        {VID: UserConfigCompositeKey.FILTER_CONSTANTS,
                         VAL: 'Cv/N/4sA5QDuAAsAhP89/w=='}]


def eng_clock_sample():
    sample_as_hex = "0907021110120606"
    return sample_as_hex.decode('hex')


eng_clock_particle = [{VID: particles.NortekEngClockDataParticleKey.DATE_TIME_ARRAY,
                       VAL: [9, 7, 2, 11, 10, 12]}]


def eng_battery_sample():
    sample_as_hex = "a71f0606"
    return sample_as_hex.decode('hex')


eng_battery_particle = [
    {VID: particles.NortekEngBatteryDataParticleKey.BATTERY_VOLTAGE, VAL: 8103}]


def eng_id_sample():
    sample_as_hex = "41514420313231352020202020200606"
    return sample_as_hex.decode('hex')


eng_id_particle = [{VID: particles.NortekEngIdDataParticleKey.ID, VAL: "AQD 1215"}]


def user_config1():
    # NumberSamplesPerBurst = 20, MeasurementInterval = 500
    # deployment output from the Nortek application
    user_config_values = "A5 00 00 01 02 00 10 00 07 00 2C 00 00 02 01 00 \
                          40 00 03 00 82 00 00 00 CC 4E 00 00 00 00 01 00 \
                          00 00 01 00 07 00 F4 01 00 00 00 00 00 00 00 00 \
                          39 28 17 14 12 12 30 2A 00 00 30 00 11 41 01 00 \
                          01 00 14 00 04 00 00 00 00 00 5E 01 02 3D 1E 3D \
                          39 3D 53 3D 6E 3D 88 3D A2 3D BB 3D D4 3D ED 3D \
                          06 3E 1E 3E 36 3E 4E 3E 65 3E 7D 3E 93 3E AA 3E \
                          C0 3E D6 3E EC 3E 02 3F 17 3F 2C 3F 41 3F 55 3F \
                          69 3F 7D 3F 91 3F A4 3F B8 3F CA 3F DD 3F F0 3F \
                          02 40 14 40 26 40 37 40 49 40 5A 40 6B 40 7C 40 \
                          8C 40 9C 40 AC 40 BC 40 CC 40 DB 40 EA 40 F9 40 \
                          08 41 17 41 25 41 33 41 42 41 4F 41 5D 41 6A 41 \
                          78 41 85 41 92 41 9E 41 AB 41 B7 41 C3 41 CF 41 \
                          DB 41 E7 41 F2 41 FD 41 08 42 13 42 1E 42 28 42 \
                          33 42 3D 42 47 42 51 42 5B 42 64 42 6E 42 77 42 \
                          80 42 89 42 91 42 9A 42 A2 42 AA 42 B2 42 BA 42 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 1E 00 5A 00 5A 00 BC 02 \
                          32 00 00 00 00 00 00 00 07 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 01 00 00 00 00 00 2A 00 00 00 \
                          02 00 14 00 EA 01 14 00 EA 01 0A 00 05 00 00 00 \
                          40 00 40 00 02 00 0F 00 5A 00 00 00 01 00 C8 00 \
                          00 00 00 00 0F 00 EA 01 EA 01 00 00 00 00 00 00 \
                          00 00 00 00 07 12 00 80 00 40 00 00 00 00 00 00 \
                          82 00 00 00 14 00 10 00 B1 2B 00 00 00 00 02 00 \
                          14 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
                          00 00 00 00 00 00 00 00 00 00 00 00 00 00 0A FF \
                          CD FF 8B 00 E5 00 EE 00 0B 00 84 FF 3D FF 5A 78"
    uc = unhexlify(user_config_values.translate(None, ' \n'))
    return uc


def user_config2():
    # NumberSamplesPerBurst = 10, MeasurementInterval = 600
    # instrument user configuration from the OSU instrument itself
    user_config_values = [
        0xa5, 0x00, 0x00, 0x01, 0x02, 0x00, 0x10, 0x00, 0x07, 0x00, 0x2c, 0x00, 0x00, 0x02, 0x01, 0x00,
        0x3c, 0x00, 0x03, 0x00, 0x82, 0x00, 0x00, 0x00, 0xcc, 0x4e, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x01, 0x00, 0x01, 0x00, 0x07, 0x00, 0x58, 0x02, 0x34, 0x39, 0x34, 0x33, 0x00, 0x00, 0x01, 0x00,
        0x26, 0x42, 0x28, 0x12, 0x12, 0x09, 0xc0, 0xa8, 0x00, 0x00, 0x30, 0x00, 0x11, 0x41, 0x14, 0x00,
        0x01, 0x00, 0x14, 0x00, 0x04, 0x00, 0x00, 0x00, 0x20, 0x35, 0x5e, 0x01, 0x02, 0x3d, 0x1e, 0x3d,
        0x39, 0x3d, 0x53, 0x3d, 0x6e, 0x3d, 0x88, 0x3d, 0xa2, 0x3d, 0xbb, 0x3d, 0xd4, 0x3d, 0xed, 0x3d,
        0x06, 0x3e, 0x1e, 0x3e, 0x36, 0x3e, 0x4e, 0x3e, 0x65, 0x3e, 0x7d, 0x3e, 0x93, 0x3e, 0xaa, 0x3e,
        0xc0, 0x3e, 0xd6, 0x3e, 0xec, 0x3e, 0x02, 0x3f, 0x17, 0x3f, 0x2c, 0x3f, 0x41, 0x3f, 0x55, 0x3f,
        0x69, 0x3f, 0x7d, 0x3f, 0x91, 0x3f, 0xa4, 0x3f, 0xb8, 0x3f, 0xca, 0x3f, 0xdd, 0x3f, 0xf0, 0x3f,
        0x02, 0x40, 0x14, 0x40, 0x26, 0x40, 0x37, 0x40, 0x49, 0x40, 0x5a, 0x40, 0x6b, 0x40, 0x7c, 0x40,
        0x8c, 0x40, 0x9c, 0x40, 0xac, 0x40, 0xbc, 0x40, 0xcc, 0x40, 0xdb, 0x40, 0xea, 0x40, 0xf9, 0x40,
        0x08, 0x41, 0x17, 0x41, 0x25, 0x41, 0x33, 0x41, 0x42, 0x41, 0x4f, 0x41, 0x5d, 0x41, 0x6a, 0x41,
        0x78, 0x41, 0x85, 0x41, 0x92, 0x41, 0x9e, 0x41, 0xab, 0x41, 0xb7, 0x41, 0xc3, 0x41, 0xcf, 0x41,
        0xdb, 0x41, 0xe7, 0x41, 0xf2, 0x41, 0xfd, 0x41, 0x08, 0x42, 0x13, 0x42, 0x1e, 0x42, 0x28, 0x42,
        0x33, 0x42, 0x3d, 0x42, 0x47, 0x42, 0x51, 0x42, 0x5b, 0x42, 0x64, 0x42, 0x6e, 0x42, 0x77, 0x42,
        0x80, 0x42, 0x89, 0x42, 0x91, 0x42, 0x9a, 0x42, 0xa2, 0x42, 0xaa, 0x42, 0xb2, 0x42, 0xba, 0x42,
        0x33, 0x33, 0x30, 0x35, 0x2d, 0x30, 0x30, 0x31, 0x30, 0x36, 0x5f, 0x30, 0x30, 0x30, 0x30, 0x31,
        0x5f, 0x32, 0x38, 0x30, 0x39, 0x32, 0x30, 0x31, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x5a, 0x00, 0x5a, 0x00, 0xbc, 0x02,
        0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x14, 0x00, 0xea, 0x01, 0x14, 0x00, 0xea, 0x01, 0x0a, 0x00, 0x05, 0x00, 0x00, 0x00,
        0x40, 0x00, 0x40, 0x00, 0x02, 0x00, 0x0f, 0x00, 0x5a, 0x00, 0x00, 0x00, 0x01, 0x00, 0xc8, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0xea, 0x01, 0xea, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x07, 0x12, 0x00, 0x80, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x82, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x08, 0x00, 0xb1, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0xff,
        0xcd, 0xff, 0x8b, 0x00, 0xe5, 0x00, 0xee, 0x00, 0x0b, 0x00, 0x84, 0xff, 0x3d, 0xff, 0xa7, 0xff]

    user_config = ''
    for value in user_config_values:
        user_config += chr(value)
    return user_config


PORT_TIMESTAMP = 3558720820.531179
DRIVER_TIMESTAMP = 3555423722.711772


def bad_sample():
    sample = 'thisshouldnotworkd'
    return sample


###############################################################################
#                           DRIVER TEST MIXIN        		                  #
#     Defines a set of constants and assert methods used for data particle    #
#     verification 														      #
#                                                                             #
#  In python, mixin classes are classes designed such that they wouldn't be   #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.									  #
###############################################################################
class DriverTestMixinSub(DriverTestMixin):
    """
    Mixin class used for storing data particle constance and common data assertion methods.
    """

    # Create some short names for the parameter test config
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    _battery_voltage_parameter = {
        particles.NortekEngBatteryDataParticleKey.BATTERY_VOLTAGE: {TYPE: int, VALUE: 0, REQUIRED: True}
    }

    _clock_data_parameter = {
        particles.NortekEngClockDataParticleKey.DATE_TIME_ARRAY: {TYPE: list, VALUE: [1, 2, 3, 4, 5, 6], REQUIRED: True},
    }

    _id_parameter = {
        particles.NortekEngIdDataParticleKey.ID: {TYPE: unicode, VALUE: '', REQUIRED: True}
    }

    _driver_parameters = {
        Parameter.TRANSMIT_PULSE_LENGTH: {TYPE: int, VALUE: 2, REQUIRED: True},
        Parameter.BLANKING_DISTANCE: {TYPE: int, VALUE: 16, REQUIRED: True},
        Parameter.RECEIVE_LENGTH: {TYPE: int, VALUE: 7, REQUIRED: True},
        Parameter.TIME_BETWEEN_PINGS: {TYPE: int, VALUE: 44, REQUIRED: True},
        Parameter.TIME_BETWEEN_BURST_SEQUENCES: {TYPE: int, VALUE: 512, REQUIRED: True},
        Parameter.NUMBER_PINGS: {},
        Parameter.AVG_INTERVAL: {},
        Parameter.USER_NUMBER_BEAMS: {},
        Parameter.TIMING_CONTROL_REGISTER: {},
        Parameter.POWER_CONTROL_REGISTER: {},
        Parameter.COMPASS_UPDATE_RATE: {},
        Parameter.COORDINATE_SYSTEM: {},
        Parameter.NUMBER_BINS: {},
        Parameter.BIN_LENGTH: {},
        Parameter.MEASUREMENT_INTERVAL: {},
        Parameter.DEPLOYMENT_NAME: {},
        Parameter.WRAP_MODE: {},
        Parameter.CLOCK_DEPLOY: {},
        Parameter.DIAGNOSTIC_INTERVAL: {},
        Parameter.MODE: {},
        Parameter.ADJUSTMENT_SOUND_SPEED: {},
        Parameter.NUMBER_SAMPLES_DIAGNOSTIC: {},
        Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC: {},
        Parameter.NUMBER_PINGS_DIAGNOSTIC: {},
        Parameter.MODE_TEST: {},
        Parameter.ANALOG_INPUT_ADDR: {},
        Parameter.SW_VERSION: {},
        Parameter.VELOCITY_ADJ_TABLE: {},
        Parameter.COMMENTS: {},
        Parameter.WAVE_MEASUREMENT_MODE: {},
        # Parameter.DYN_PERCENTAGE_POSITION: {},
        # Parameter.WAVE_TRANSMIT_PULSE: {},
        # Parameter.WAVE_BLANKING_DISTANCE: {},
        # Parameter.WAVE_CELL_SIZE: {},
        # Parameter.NUMBER_DIAG_SAMPLES: {},
        Parameter.NUMBER_SAMPLES_PER_BURST: {},
        Parameter.ANALOG_OUTPUT_SCALE: {},
        Parameter.CORRELATION_THRESHOLD: {},
        Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG: {},
        # Parameter.QUAL_CONSTANTS: {},
        EngineeringParameter.CLOCK_SYNC_INTERVAL: {},
        EngineeringParameter.ACQUIRE_STATUS_INTERVAL: {},
    }

    _user_config_parameters = {
        UserConfigKey.TX_LENGTH: {TYPE: int, VALUE: 2, REQUIRED: True},
        UserConfigKey.BLANK_DIST: {TYPE: int, VALUE: 16, REQUIRED: True},
        UserConfigKey.RX_LENGTH: {TYPE: int, VALUE: 7, REQUIRED: True},
        UserConfigKey.TIME_BETWEEN_PINGS: {TYPE: int, VALUE: 44, REQUIRED: True},
        UserConfigKey.TIME_BETWEEN_BURSTS: {TYPE: int, VALUE: 512, REQUIRED: True},
        UserConfigKey.NUM_PINGS: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.AVG_INTERVAL: {TYPE: int, VALUE: 64, REQUIRED: True},
        UserConfigKey.NUM_BEAMS: {TYPE: int, VALUE: 3, REQUIRED: True},
        UserConfigKey.PROFILE_TYPE: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.MODE_TYPE: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigCompositeKey.TCR: {TYPE: int, VALUE: 130, REQUIRED: False},
        UserConfigCompositeKey.PCR: {TYPE: int, VALUE: 0, REQUIRED: False},
        UserConfigKey.POWER_TCM1: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.POWER_TCM2: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.SYNC_OUT_POSITION: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.SAMPLE_ON_SYNC: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.START_ON_SYNC: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.POWER_PCR1: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.POWER_PCR2: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.COMPASS_UPDATE_RATE: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.COORDINATE_SYSTEM: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.NUM_CELLS: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.CELL_SIZE: {TYPE: int, VALUE: 7, REQUIRED: True},
        UserConfigKey.MEASUREMENT_INTERVAL: {TYPE: int, VALUE: 500, REQUIRED: True},
        UserConfigKey.DEPLOYMENT_NAME: {TYPE: unicode, VALUE: "", REQUIRED: True},
        UserConfigKey.WRAP_MODE: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigCompositeKey.DEPLOY_START_TIME: {TYPE: list, VALUE: [39, 28, 17, 14, 12, 12], REQUIRED: True},
        UserConfigCompositeKey.DIAG_INTERVAL: {TYPE: int, VALUE: 10800, REQUIRED: True},
        UserConfigCompositeKey.MODE: {TYPE: int, VALUE: 48, REQUIRED: False},
        UserConfigKey.USE_SPEC_SOUND_SPEED: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.DIAG_MODE_ON: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.ANALOG_OUTPUT_ON: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.OUTPUT_FORMAT: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.SCALING: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.SERIAL_OUT_ON: {TYPE: bool, VALUE: True, REQUIRED: True},
        UserConfigKey.STAGE_ON: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.ANALOG_POWER_OUTPUT: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.SOUND_SPEED_ADJUST: {TYPE: int, VALUE: 16657, REQUIRED: True},
        UserConfigKey.NUM_DIAG_SAMPLES: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.NUM_BEAMS_PER_CELL: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.NUM_PINGS_DIAG: {TYPE: int, VALUE: 20, REQUIRED: True},
        UserConfigCompositeKey.MODE_TEST: {TYPE: int, VALUE: 4, REQUIRED: False},
        UserConfigKey.USE_DSP_FILTER: {TYPE: bool, VALUE: False, REQUIRED: True},
        UserConfigKey.FILTER_DATA_OUTPUT: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.ANALOG_INPUT_ADDR: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.SW_VER: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigCompositeKey.VELOCITY_ADJ_FACTOR: {TYPE: unicode, VALUE: '', REQUIRED: True},
        UserConfigKey.FILE_COMMENTS: {TYPE: unicode, VALUE: '', REQUIRED: True},
        UserConfigCompositeKey.WAVE_MODE: {TYPE: int, VALUE: 4615, REQUIRED: False},
        UserConfigKey.WAVE_DATA_RATE: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.WAVE_CELL_POS: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.DYNAMIC_POS_TYPE: {TYPE: int, VALUE: 1, REQUIRED: True},
        UserConfigKey.PERCENT_WAVE_CELL_POS: {TYPE: int, VALUE: 32768, REQUIRED: True},
        UserConfigKey.WAVE_TX_PULSE: {TYPE: int, VALUE: 16384, REQUIRED: True},
        UserConfigKey.FIX_WAVE_BLANK_DIST: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.WAVE_CELL_SIZE: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.NUM_DIAG_PER_WAVE: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.NUM_SAMPLE_PER_BURST: {TYPE: int, VALUE: 20, REQUIRED: True},
        UserConfigKey.ANALOG_SCALE_FACTOR: {TYPE: int, VALUE: 11185, REQUIRED: True},
        UserConfigKey.CORRELATION_THRS: {TYPE: int, VALUE: 0, REQUIRED: True},
        UserConfigKey.TX_PULSE_LEN_2ND: {TYPE: int, VALUE: 2, REQUIRED: True},
        UserConfigCompositeKey.FILTER_CONSTANTS: {TYPE: unicode, VALUE: 'Cv/N/4sA5QDuAAsAhP89/w==', REQUIRED: True},
        UserConfigKey.CHECKSUM: {TYPE: int, VALUE: 0, REQUIRED: False}
    }

    _head_config_parameter = {
        particles.NortekHeadConfigDataParticleKey.PRESSURE_SENSOR: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.MAG_SENSOR: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.TILT_SENSOR: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.TILT_SENSOR_MOUNT: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.HEAD_FREQ: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.HEAD_TYPE: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.HEAD_SERIAL: {TYPE: unicode, VALUE: '', REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.SYSTEM_DATA: {TYPE: unicode, VALUE: '', REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.NUM_BEAMS: {TYPE: int, VALUE: 3, REQUIRED: True},
        particles.NortekHeadConfigDataParticleKey.CONFIG: {TYPE: int, VALUE: 0, REQUIRED: False},
        UserConfigKey.CHECKSUM: {TYPE: int, VALUE: 0, REQUIRED: False}
    }

    _hardware_config_parameter = {
        particles.NortekHardwareConfigDataParticleKey.SERIAL_NUM: {TYPE: unicode, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.RECORDER_INSTALLED: {TYPE: bool, VALUE: False, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.COMPASS_INSTALLED: {TYPE: bool, VALUE: True, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.BOARD_FREQUENCY: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.PIC_VERSION: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.HW_REVISION: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.RECORDER_SIZE: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.VELOCITY_RANGE: {TYPE: int, VALUE: 0, REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.FW_VERSION: {TYPE: unicode, VALUE: '', REQUIRED: True},
        particles.NortekHardwareConfigDataParticleKey.STATUS: {TYPE: int, VALUE: 0, REQUIRED: False},
        particles.NortekHardwareConfigDataParticleKey.CONFIG: {TYPE: unicode, VALUE: 0, REQUIRED: False},
        particles.NortekHardwareConfigDataParticleKey.CHECKSUM: {TYPE: int, VALUE: 0, REQUIRED: False}
    }

    _capabilities = {
        ProtocolState.UNKNOWN: [ProtocolEvent.DISCOVER,
                                ProtocolEvent.READ_MODE],

        ProtocolState.COMMAND: [ProtocolEvent.GET,
                                ProtocolEvent.SET,
                                ProtocolEvent.START_DIRECT,
                                ProtocolEvent.START_AUTOSAMPLE,
                                ProtocolEvent.CLOCK_SYNC,
                                ProtocolEvent.ACQUIRE_SAMPLE,
                                ProtocolEvent.ACQUIRE_STATUS,
                                ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                ProtocolEvent.SCHEDULED_ACQUIRE_STATUS],

        ProtocolState.AUTOSAMPLE: [ProtocolEvent.STOP_AUTOSAMPLE,
                                   ProtocolEvent.SCHEDULED_CLOCK_SYNC,
                                   ProtocolEvent.SCHEDULED_ACQUIRE_STATUS,
                                   ProtocolEvent.READ_MODE,
                                   ProtocolEvent.GET],

        ProtocolState.DIRECT_ACCESS: [ProtocolEvent.STOP_DIRECT,
                                      ProtocolEvent.EXECUTE_DIRECT,
                                      ProtocolEvent.READ_MODE],
        ProtocolState.ACQUIRING_SAMPLE: [ProtocolEvent.GET_SAMPLE]
    }

    def assert_particle_battery(self, data_particle, verify_values=False):
        """
        Verify [flortd]_sample particle
        @param data_particle [FlortDSample]_ParticleKey data particle
        @param verify_values bool, should we verify parameter values
        """

        self.assert_data_particle_keys(particles.NortekEngBatteryDataParticleKey, self._battery_voltage_parameter)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.BATTERY)
        self.assert_data_particle_parameters(data_particle, self._battery_voltage_parameter, verify_values)

    def assert_particle_clock(self, data_particle, verify_values=False):
        """
        Verify [flortd]_sample particle
        @param data_particle [FlortDSample]_ParticleKey data particle
        @param verify_values bool, should we verify parameter values
        """

        self.assert_data_particle_keys(particles.NortekEngClockDataParticleKey, self._clock_data_parameter)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.CLOCK)
        self.assert_data_particle_parameters(data_particle, self._clock_data_parameter, verify_values)

    def assert_particle_hardware(self, data_particle, verify_values=False):
        """
        Verify [flortd]_sample particle
        @param data_particle [FlortDSample]_ParticleKey data particle
        @param verify_values bool, should we verify parameter values
        """

        self.assert_data_particle_keys(particles.NortekHardwareConfigDataParticleKey, self._hardware_config_parameter)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.HARDWARE_CONFIG)
        self.assert_data_particle_parameters(data_particle, self._hardware_config_parameter, verify_values)

    def assert_particle_head(self, data_particle, verify_values=False):
        """
        Verify [nortek]_sample particle
        @param data_particle [nortek]_ParticleKey data particle
        @param verify_values bool, should we verify parameter values
        """

        self.assert_data_particle_keys(particles.NortekHeadConfigDataParticleKey, self._head_config_parameter)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.HEAD_CONFIG)
        self.assert_data_particle_parameters(data_particle, self._head_config_parameter, verify_values)

    def assert_particle_user(self, data_particle, verify_values=False):
        """
        Verify [nortek]_sample particle
        @param data_particle  [nortek]_ParticleKey data particle
        @param verify_values  bool, should we verify parameter values
        """

        self.assert_data_particle_keys(UserConfigKey, self._user_config_parameters)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.USER_CONFIG)
        self.assert_data_particle_parameters(data_particle, self._user_config_parameters, verify_values)

    def assert_particle_id(self, data_particle, verify_values=False):
        """
        Verify [nortek]_sample particle
        @param data_particle  [nortek]_ParticleKey data particle
        @param verify_values  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(particles.NortekEngIdDataParticleKey, self._id_parameter)
        self.assert_data_particle_header(data_particle, particles.NortekDataParticleType.ID_STRING)
        self.assert_data_particle_parameters(data_particle, self._id_parameter, verify_values)


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
###############################################################################
@attr('UNIT', group='mi')
class NortekUnitTest(InstrumentDriverUnitTestCase, DriverTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    def test_base_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the capabilities
        """
        self.assert_enum_has_no_duplicates(particles.NortekDataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(InstrumentCommands())
        self.assert_enum_has_no_duplicates(InstrumentPrompts())

        # Test capabilities for duplicates, them verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_base_driver_protocol_filter_capabilities(self):
        """
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock(spec="PortAgentClient")
        protocol = NortekInstrumentProtocol(InstrumentPrompts, common.NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))

    def test_core_chunker(self):
        """
        Verify the chunker can parse each sample type
        1. complete data structure
        2. fragmented data structure
        3. combined data structure
        4. data structure with noise
        """
        chunker = StringChunker(NortekInstrumentProtocol.sieve_function)

        # test complete data structures
        self.assert_chunker_sample(chunker, hw_config_sample())
        self.assert_chunker_sample(chunker, head_config_sample())
        self.assert_chunker_sample(chunker, user_config_sample())

        # test fragmented data structures
        self.assert_chunker_fragmented_sample(chunker, hw_config_sample())
        self.assert_chunker_fragmented_sample(chunker, head_config_sample())
        self.assert_chunker_fragmented_sample(chunker, user_config_sample())

        # test combined data structures
        self.assert_chunker_combined_sample(chunker, hw_config_sample())
        self.assert_chunker_combined_sample(chunker, head_config_sample())
        self.assert_chunker_combined_sample(chunker, user_config_sample())

        # test data structures with noise
        self.assert_chunker_sample_with_noise(chunker, hw_config_sample())
        self.assert_chunker_sample_with_noise(chunker, head_config_sample())
        self.assert_chunker_sample_with_noise(chunker, user_config_sample())

    def test_core_corrupt_data_structures(self):
        """
        Verify when generating the particle, if the particle is corrupt, it will not generate
        """
        particle = particles.NortekHardwareConfigDataParticle(bad_sample(), port_timestamp=PORT_TIMESTAMP)
        with self.assertRaises(SampleException):
            particle.generate()

        particle = particles.NortekHeadConfigDataParticle(bad_sample(), port_timestamp=PORT_TIMESTAMP)
        with self.assertRaises(SampleException):
            particle.generate()

        particle = particles.NortekUserConfigDataParticle(bad_sample(), port_timestamp=PORT_TIMESTAMP)
        with self.assertRaises(SampleException):
            particle.generate()

    def test_hw_config_sample_format(self):
        """
        Verify driver can get hardware config sample data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.HARDWARE_CONFIG,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: hw_config_particle}

        self.compare_parsed_data_particle(particles.NortekHardwareConfigDataParticle,
                                          hw_config_sample(), expected_particle)

    def test_head_config_sample_format(self):
        """
        Verify driver can get hardware config sample data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.HEAD_CONFIG,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: head_config_particle}

        self.compare_parsed_data_particle(particles.NortekHeadConfigDataParticle, head_config_sample(), expected_particle)

    def test_user_config_sample_format(self):
        """
        Verify driver can get user config sample data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.USER_CONFIG,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: user_config_particle}

        self.compare_parsed_data_particle(particles.NortekUserConfigDataParticle,
                                          user_config_sample(),
                                          expected_particle)

    def test_eng_clock_sample_format(self):
        """
        Verify driver can get clock sample engineering data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.CLOCK,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: eng_clock_particle}

        self.compare_parsed_data_particle(particles.NortekEngClockDataParticle,
                                          eng_clock_sample(),
                                          expected_particle)

    def test_eng_battery_sample_format(self):
        """
        Verify driver can get battery sample engineering data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.BATTERY,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: eng_battery_particle}

        self.compare_parsed_data_particle(particles.NortekEngBatteryDataParticle,
                                          eng_battery_sample(),
                                          expected_particle)

    def test_eng_id_sample_format(self):
        """
        Verify driver can get id sample engineering data out in a
        reasonable format. Parsed is all we care about...raw is tested in the
        base DataParticle tests.
        """
        port_timestamp = PORT_TIMESTAMP
        driver_timestamp = DRIVER_TIMESTAMP

        # construct the expected particle
        expected_particle = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.STREAM_NAME: particles.NortekDataParticleType.ID_STRING,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp,
            DataParticleKey.PREFERRED_TIMESTAMP: DataParticleKey.PORT_TIMESTAMP,
            DataParticleKey.QUALITY_FLAG: DataParticleValue.OK,
            DataParticleKey.VALUES: eng_id_particle}

        self.compare_parsed_data_particle(particles.NortekEngIdDataParticle,
                                          eng_id_sample(),
                                          expected_particle)

    def test_scheduled_clock_sync_acquire_status(self):
        """
        Verify the scheduled clock sync and acquire status is added to the protocol
        Verify if there is no scheduling, nothing is added to the protocol
        """
        mock_callback = Mock(spec="PortAgentClient")
        protocol = NortekInstrumentProtocol(InstrumentPrompts, common.NEWLINE, mock_callback)
        protocol._init_type = InitializationType.NONE

        # Verify there is nothing scheduled
        protocol._handler_autosample_enter()
        self.assertEqual(protocol._scheduler_callback.get(ScheduledJob.CLOCK_SYNC), None)
        self.assertEqual(protocol._scheduler_callback.get(ScheduledJob.ACQUIRE_STATUS), None)

        protocol._param_dict.add(EngineeringParameter.CLOCK_SYNC_INTERVAL,
                                 common.INTERVAL_TIME_REGEX,
                                 lambda match: match.group(1),
                                 str,
                                 type=ParameterDictType.STRING,
                                 visibility=ParameterDictVisibility.IMMUTABLE,
                                 display_name="Clock Sync Interval",
                                 description='Interval for synchronizing the clock',
                                 units=ParameterUnits.TIME_INTERVAL,
                                 default_value='00:00:10',
                                 startup_param=True)
        protocol._param_dict.add(EngineeringParameter.ACQUIRE_STATUS_INTERVAL,
                                 common.INTERVAL_TIME_REGEX,
                                 lambda match: match.group(1),
                                 str,
                                 type=ParameterDictType.STRING,
                                 visibility=ParameterDictVisibility.IMMUTABLE,
                                 display_name="Acquire Status Interval",
                                 description='Interval for gathering status particles',
                                 units=ParameterUnits.TIME_INTERVAL,
                                 default_value='00:00:02',
                                 startup_param=True)

        # set the values of the dictionary using set_default
        protocol._param_dict.set_value(EngineeringParameter.ACQUIRE_STATUS_INTERVAL,
                                       protocol._param_dict.get_default_value(
                                           EngineeringParameter.ACQUIRE_STATUS_INTERVAL))
        protocol._param_dict.set_value(EngineeringParameter.CLOCK_SYNC_INTERVAL,
                                       protocol._param_dict.get_default_value(EngineeringParameter.CLOCK_SYNC_INTERVAL))
        protocol._handler_autosample_enter()

        # Verify there is scheduled events
        self.assertTrue(protocol._scheduler_callback.get(ScheduledJob.CLOCK_SYNC))
        self.assertTrue(protocol._scheduler_callback.get(ScheduledJob.ACQUIRE_STATUS))


###############################################################################
#                            INTEGRATION TESTS                                #
# Device specific integration tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('INT', group='mi')
class NortekIntTest(InstrumentDriverIntegrationTestCase, DriverTestMixinSub):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_set_init_params(self):
        """
        Verify the instrument will set the init params from a config file
        This verifies setting all the parameters.
        """
        self.assert_initialize_driver()

        self.driver_client.cmd_dvr('set_init_params',
                                   {DriverConfigKey.PARAMETERS:
                                        {DriverParameter.ALL:
                                             base64.b64encode(user_config1())}})

        values_after = self.driver_client.cmd_dvr("get_resource", Parameter.ALL)
        log.debug("VALUES_AFTER = %s", values_after)

        self.assertEquals(values_after[Parameter.TRANSMIT_PULSE_LENGTH],
                          self._user_config_parameters.get(UserConfigKey.TX_LENGTH)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.BLANKING_DISTANCE],
                          self._user_config_parameters.get(UserConfigKey.BLANK_DIST)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.RECEIVE_LENGTH],
                          self._user_config_parameters.get(UserConfigKey.RX_LENGTH)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.TIME_BETWEEN_PINGS],
                          self._user_config_parameters.get(UserConfigKey.TIME_BETWEEN_PINGS)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.TIME_BETWEEN_BURST_SEQUENCES],
                          self._user_config_parameters.get(UserConfigKey.TIME_BETWEEN_BURSTS)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.AVG_INTERVAL],
                          self._user_config_parameters.get(UserConfigKey.AVG_INTERVAL)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.USER_NUMBER_BEAMS],
                          self._user_config_parameters.get(UserConfigKey.NUM_BEAMS)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.TIMING_CONTROL_REGISTER],
                          self._user_config_parameters.get(UserConfigCompositeKey.TCR)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.POWER_CONTROL_REGISTER],
                          self._user_config_parameters.get(UserConfigCompositeKey.PCR)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.COMPASS_UPDATE_RATE],
                          self._user_config_parameters.get(UserConfigKey.COMPASS_UPDATE_RATE)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.COORDINATE_SYSTEM],
                          self._user_config_parameters.get(UserConfigKey.COORDINATE_SYSTEM)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.NUMBER_BINS],
                          self._user_config_parameters.get(UserConfigKey.NUM_CELLS)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.BIN_LENGTH],
                          self._user_config_parameters.get(UserConfigKey.CELL_SIZE)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.MEASUREMENT_INTERVAL],
                          self._user_config_parameters.get(UserConfigKey.MEASUREMENT_INTERVAL)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.DEPLOYMENT_NAME],
                          self._user_config_parameters.get(UserConfigKey.DEPLOYMENT_NAME)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.WRAP_MODE],
                          self._user_config_parameters.get(UserConfigKey.WRAP_MODE)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.CLOCK_DEPLOY],
                          self._user_config_parameters.get(UserConfigCompositeKey.DEPLOY_START_TIME)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.DIAGNOSTIC_INTERVAL],
                          self._user_config_parameters.get(UserConfigCompositeKey.DIAG_INTERVAL)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.MODE],
                          self._user_config_parameters.get(UserConfigCompositeKey.MODE)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.ADJUSTMENT_SOUND_SPEED],
                          self._user_config_parameters.get(UserConfigKey.SOUND_SPEED_ADJUST)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.NUMBER_SAMPLES_DIAGNOSTIC],
                          self._user_config_parameters.get(UserConfigKey.NUM_DIAG_SAMPLES)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.NUMBER_BEAMS_CELL_DIAGNOSTIC],
                          self._user_config_parameters.get(UserConfigKey.NUM_BEAMS_PER_CELL)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.NUMBER_PINGS_DIAGNOSTIC],
                          self._user_config_parameters.get(UserConfigKey.NUM_PINGS_DIAG)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.MODE_TEST],
                          self._user_config_parameters.get(UserConfigCompositeKey.MODE_TEST)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.ANALOG_INPUT_ADDR],
                          self._user_config_parameters.get(UserConfigKey.ANALOG_INPUT_ADDR)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.SW_VERSION],
                          self._user_config_parameters.get(UserConfigKey.SW_VER)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.COMMENTS],
                          self._user_config_parameters.get(UserConfigKey.FILE_COMMENTS)[ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.WAVE_MEASUREMENT_MODE],
                          self._user_config_parameters.get(UserConfigCompositeKey.WAVE_MODE)[ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.DYN_PERCENTAGE_POSITION],
        #                   self._user_config_parameters.get(UserConfigKey.PERCENT_WAVE_CELL_POS)[
        #                       ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.WAVE_TRANSMIT_PULSE],
        #                   self._user_config_parameters.get(UserConfigKey.WAVE_TX_PULSE)[ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.WAVE_BLANKING_DISTANCE],
        #                   self._user_config_parameters.get(UserConfigKey.FIX_WAVE_BLANK_DIST)[
        #                       ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.WAVE_CELL_SIZE],
        #                   self._user_config_parameters.get(UserConfigKey.WAVE_CELL_SIZE)[ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.NUMBER_DIAG_SAMPLES],
        #                   self._user_config_parameters.get(UserConfigKey.NUM_DIAG_PER_WAVE)[
        #                       ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.NUMBER_SAMPLES_PER_BURST],
                          self._user_config_parameters.get(UserConfigKey.NUM_SAMPLE_PER_BURST)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.ANALOG_OUTPUT_SCALE],
                          self._user_config_parameters.get(UserConfigKey.ANALOG_SCALE_FACTOR)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.CORRELATION_THRESHOLD],
                          self._user_config_parameters.get(UserConfigKey.CORRELATION_THRS)[
                              ParameterTestConfigKey.VALUE])
        self.assertEquals(values_after[Parameter.TRANSMIT_PULSE_LENGTH_SECOND_LAG],
                          self._user_config_parameters.get(UserConfigKey.TX_PULSE_LEN_2ND)[
                              ParameterTestConfigKey.VALUE])
        # self.assertEquals(values_after[Parameter.QUAL_CONSTANTS],
        #                   self._user_config_parameters.get(UserConfigCompositeKey.FILTER_CONSTANTS)[
        #                       ParameterTestConfigKey.VALUE])

    def test_instrument_clock_sync(self):
        """
        Verify the driver can sync the clock
        """
        self.assert_initialize_driver()
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.CLOCK_SYNC)

    def test_command_acquire_status(self):
        """
        Test acquire status command and events.

        1. initialize the instrument to COMMAND state
        2. command the instrument to ACQUIRE STATUS (BV, RC, GH, GP, GC, ID)
        3. verify the particle coming in
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        # test acquire status
        self.assert_driver_command(ProtocolEvent.ACQUIRE_STATUS, delay=1)
        # ID
        self.assert_async_particle_generation(particles.NortekDataParticleType.ID_STRING, self.assert_particle_id)
        # BV
        self.assert_async_particle_generation(particles.NortekDataParticleType.BATTERY, self.assert_particle_battery)
        # RC
        self.assert_async_particle_generation(particles.NortekDataParticleType.CLOCK, self.assert_particle_clock)
        # GP
        self.assert_async_particle_generation(particles.NortekDataParticleType.HARDWARE_CONFIG, self.assert_particle_hardware)
        # GH
        self.assert_async_particle_generation(particles.NortekDataParticleType.HEAD_CONFIG, self.assert_particle_head)
        # GC
        self.assert_async_particle_generation(particles.NortekDataParticleType.USER_CONFIG, self.assert_particle_user)

    def test_direct_access(self):
        """
        Verify the driver can enter/exit the direct access state
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)
        self.assert_state_change(ProtocolState.COMMAND, 5)
        log.debug('in command mode')
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_DIRECT)
        self.assert_state_change(ProtocolState.DIRECT_ACCESS, 5)
        log.debug('in direct access')

        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_DIRECT)
        self.assert_state_change(ProtocolState.COMMAND, 5)
        log.debug('leaving direct access')

    def test_errors(self):
        """
        Verify response to erroneous commands and setting bad parameters.
        """
        self.assert_initialize_driver(ProtocolState.COMMAND)

        # Assert an invalid command
        self.assert_driver_command_exception('ima_bad_command', exception_class=InstrumentCommandException)

        # Assert for a known command, invalid state.
        self.assert_driver_command_exception(ProtocolEvent.STOP_AUTOSAMPLE, exception_class=InstrumentCommandException)

        # Assert set fails with a bad parameter (not ALL or a list).
        self.assert_set_exception('I am a bogus param.', exception_class=InstrumentParameterException)

        # Assert set fails with bad parameter and bad value
        self.assert_set_exception('I am a bogus param.', value='bogus value',
                                  exception_class=InstrumentParameterException)

        # put driver in disconnected state.
        self.driver_client.cmd_dvr('disconnect')

        # Assert for a known command, invalid state.
        self.assert_driver_command_exception(ProtocolEvent.CLOCK_SYNC, exception_class=InstrumentCommandException)

        # Test that the driver is in state disconnected.
        self.assert_state_change(DriverConnectionState.DISCONNECTED, timeout=common.TIMEOUT)

        # Setup the protocol state machine and the connection to port agent.
        self.driver_client.cmd_dvr('initialize')

        # Test that the driver is in state unconfigured.
        self.assert_state_change(DriverConnectionState.UNCONFIGURED, timeout=common.TIMEOUT)

        # Assert we forgot the comms parameter.
        self.assert_driver_command_exception('configure', exception_class=InstrumentParameterException)

        # Configure driver and transition to disconnected.
        self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test that the driver is in state disconnected.
        self.assert_state_change(DriverConnectionState.DISCONNECTED, timeout=common.TIMEOUT)
