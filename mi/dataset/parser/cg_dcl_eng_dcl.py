#!/usr/bin/env python

"""
@package mi.dataset.parser
@file mi/dataset/parser/cg_dcl_eng_dcl.py
@author Mark Worden
@brief Parser for the cg_dcl_eng_dcl dataset parser
"""

__author__ = 'mworden'
__license__ = 'Apache 2.0'

import re

from mi.core.log import log
from mi.core.exceptions import ConfigurationException, SampleEncodingException
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, INT_REGEX, \
    TIME_HR_MIN_SEC_REGEX, FLOAT_REGEX, ASCII_HEX_CHAR_REGEX, DCL_TIMESTAMP_REGEX

import mi.dataset.parser.utilities


# TODO - move to mi.core.common
def build_regex_string(groups, separator='\s+'):
    """
    Create a regular expression string from a list of group name and pattern tuples.
    :param groups:  list of tuples (group name, pattern) - if group name is None, no group is created
    :param separator:  (optional) separator between items - default: whitespace
    :return:  regular expression string
    """
    regex_list = []
    for group, string in groups:
        if group:
            regex_list.append('(?P<' + group + '>' + string + ')')
        else:
            regex_list.append(string)

    return separator.join(regex_list)


class ParticleClassTypes(BaseEnum):
    """
    Enum for the cg_dcl_eng_dcl particle class types.
    """

    MSG_COUNTS_PARTICLE_CLASS = 'msg_counts_particle_class'
    CPU_UPTIME_PARTICLE_CLASS = 'cpu_uptime_particle_class'
    ERROR_PARTICLE_CLASS = 'error_particle_class'
    GPS_PARTICLE_CLASS = 'gps_particle_class'
    PPS_PARTICLE_CLASS = 'pps_particle_class'
    SUPERV_PARTICLE_CLASS = 'superv_particle_class'
    DLOG_MGR_PARTICLE_CLASS = 'dlog_mgr_particle_class'
    DLOG_STATUS_PARTICLE_CLASS = 'dlog_status_particle_class'
    STATUS_PARTICLE_CLASS = 'status_particle_class'
    DLOG_AARM_PARTICLE_CLASS = 'dlog_aarm_particle_class'


class ParticleType(BaseEnum):
    """
    Enum for the cg_dcl_eng_dcl particle stream types.
    """

    MSG_COUNTS = 'cg_dcl_eng_dcl_msg_counts'
    MSG_COUNTS_RECOVERED = 'cg_dcl_eng_dcl_msg_counts_recovered'
    CPU_UPTIME = 'cg_dcl_eng_dcl_cpu_uptime'
    CPU_UPTIME_RECOVERED = 'cg_dcl_eng_dcl_cpu_uptime_recovered'
    ERROR = 'cg_dcl_eng_dcl_error'
    ERROR_RECOVERED = 'cg_dcl_eng_dcl_error_recovered'
    GPS = 'cg_dcl_eng_dcl_gps'
    GPS_RECOVERED = 'cg_dcl_eng_dcl_gps_recovered'
    PPS = 'cg_dcl_eng_dcl_pps'
    PPS_RECOVERED = 'cg_dcl_eng_dcl_pps_recovered'
    SUPERV = 'cg_dcl_eng_dcl_superv'
    SUPERV_RECOVERED = 'cg_dcl_eng_dcl_superv_recovered'
    DLOG_MGR = 'cg_dcl_eng_dcl_dlog_mgr'
    MGR_RECOVERED = 'cg_dcl_eng_dcl_dlog_mgr_recovered'
    DLOG_STATUS = 'cg_dcl_eng_dcl_dlog_status'
    DLOG_STATUS_RECOVERED = 'cg_dcl_eng_dcl_dlog_status_recovered'
    STATUS = 'cg_dcl_eng_dcl_status'
    STATUS_RECOVERED = 'cg_dcl_eng_dcl_status_recovered'
    DLOG_AARM = 'cg_dcl_eng_dcl_dlog_aarm'
    DLOG_AARM_RECOVERED = 'cg_dcl_eng_dcl_dlog_aarm_recovered'


class ParticleKey(BaseEnum):
    """
    Enum for the cg_dcl_eng_dcl particle parameters.
    """

    HEADER_TIMESTAMP = 'header_timestamp'
    GPS_COUNTS = 'gps_counts'
    NTP_COUNTS = 'ntp_counts'
    PPS_COUNTS = 'pps_counts'
    SUPERV_COUNTS = 'superv_counts'
    DLOG_MGR_COUNTS = 'dlog_mgr_counts'
    UPTIME_STRING = 'uptime_string'
    LOAD_VAL_1 = 'load_val_1'
    LOAD_VAL_2 = 'load_val_2'
    LOAD_VAL_3 = 'load_val_3'
    MEM_FREE = 'mem_free'
    NUM_PROCESSES = 'num_processes'
    LOG_TYPE = 'log_type'
    MESSAGE_TEXT = 'message_text'
    MESSAGE_SENT_TIMESTAMP = 'message_sent_timestamp'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    GPS_SPEED = 'gps_speed'
    GPS_TRUE_COURSE = 'gps_true_course'
    GPS_QUALITY = 'gps_quality'
    GPS_NUM_SATELLITES = 'gps_num_satellites'
    GPS_HDOP = 'gps_hdop'
    GPS_ALTITUDE = 'gps_altitude'
    DATE_OF_FIX = 'date_of_fix'
    TIME_OF_FIX = 'time_of_fix'
    LATITUDE_ALT_FORMAT = 'latitude_alt_format'
    LONGITUDE_ALT_FORMAT = 'longitude_alt_format'
    NMEA_LOCK = 'nmea_lock'
    DELTA = 'delta'
    DELTA_MIN = 'delta_min'
    DELTA_MAX = 'delta_max'
    BAD_PULSES = 'bad_pulses'
    BOARD_TYPE = 'board_type'
    VMAIN_BACKPLANE_BUS_VOLTAGE = 'vmain_backplane_bus_voltage'
    IMAIN_CURRENT = 'imain_current'
    ERROR_VMAIN_OUT_TOLERANCE = 'error_vmain_out_tolerance'
    ERROR_IMAIN_OUT_TOLERANCE = 'error_imain_out_tolerance'
    ERROR_DCL_ISO_SWGF_LIM_EXCEEDED = 'error_dcl_iso_swgf_lim_exceeded'
    ERROR_DCL_RTN_SWFG_LIM_EXCEEDED = 'error_dcl_rtn_swfg_lim_exceeded'
    ERROR_VMAIN_SWGF_LIM_EXCEEDED = 'error_vmain_swgf_lim_exceeded'
    ERROR_GMAIN_SWGF_LIM_EXCEEDED = 'error_gmain_swgf_lim_exceeded'
    ERROR_SENSOR_ISO_SWGF_LIM_EXCEEDED = 'error_sensor_iso_swgf_lim_exceeded'
    ERROR_SNSR_COM_SWGF_LIM_EXCEEDED = 'error_snsr_com_swgf_lim_exceeded'
    ERROR_LEAK_DETECT_C1_LIM_EXCEEDED = 'error_leak_detect_c1_lim_exceeded'
    ERROR_LEAK_DETECT_C2_LIM_EXCEEDED = 'error_leak_detect_c2_lim_exceeded'
    ERROR_CHANNEL_OVERCURRENT_FAULT = 'error_channel_overcurrent_fault'
    ERROR_CHANNEL_1_NOT_RESPONDING = 'error_channel_1_not_responding'
    ERROR_CHANNEL_2_NOT_RESPONDING = 'error_channel_2_not_responding'
    ERROR_CHANNEL_3_NOT_RESPONDING = 'error_channel_3_not_responding'
    ERROR_CHANNEL_4_NOT_RESPONDING = 'error_channel_4_not_responding'
    ERROR_CHANNEL_5_NOT_RESPONDING = 'error_channel_5_not_responding'
    ERROR_CHANNEL_6_NOT_RESPONDING = 'error_channel_6_not_responding'
    ERROR_CHANNEL_7_NOT_RESPONDING = 'error_channel_7_not_responding'
    ERROR_CHANNEL_8_NOT_RESPONDING = 'error_channel_8_not_responding'
    ERROR_I2C_ERROR = 'error_i2c_error'
    ERROR_UART_ERROR = 'error_uart_error'
    ERROR_BROWN_OUT_RESET = 'error_brown_out_reset'
    BMP085_TEMP = 'bmp085_temp'
    SHT25_TEMP = 'sht25_temp'
    MURATA_12V_TEMP = 'murata_12v_temp'
    MURATA_24V_TEMP = 'murata_24v_temp'
    VICOR_12V_BCM_TEMP = 'vicor_12v_bcm_temp'
    SHT25_HUMIDITY = 'sht25_humidity'
    BMP085_PRESSURE = 'bmp085_pressure'
    ACTIVE_SWGF_CHANNELS = 'active_swgf_channels'
    SWGF_C1_MAX_LEAKAGE = 'swgf_c1_max_leakage'
    SWGF_C2_MAX_LEAKAGE = 'swgf_c2_max_leakage'
    SWGF_C3_MAX_LEAKAGE = 'swgf_c3_max_leakage'
    ACTIVE_LEAK_DETECT_CHANNELS = 'active_leak_detect_channels'
    LEAK_DETECT_C1_V = 'leak_detect_c1_v'
    LEAK_DETECT_C2_V = 'leak_detect_c2_v'
    CHANNEL_STATE = 'channel_state'
    CHANNEL_V = 'channel_v'
    CHANNEL_I = 'channel_i'
    CHANNEL_ERROR_STATUS = 'channel_error_status'
    PWR_BOARD_MODE = 'pwr_board_mode'
    DPB_MODE = 'dpb_mode'
    DPB_VOLTAGE_MODE = 'dpb_voltage_mode'
    VMAIN_DPB_IN = 'vmain_dpb_in'
    IMAIN_DPB_IN = 'imain_dpb_in'
    OUT_12V_V = 'out_12v_v'
    OUT_12V_I = 'out_12v_i'
    OUT_24V_V = 'out_24v_v'
    OUT_24V_I = 'out_24v_i'
    DATALOGGER_TIMESTAMP = 'datalogger_timestamp'
    DLOG_MGR_ACT = 'dlog_mgr_act'
    DLOG_MGR_STR = 'dlog_mgr_str'
    DLOG_MGR_HLT = 'dlog_mgr_hlt'
    DLOG_MGR_FLD = 'dlog_mgr_fld'
    DLOG_MGR_MAP = 'dlog_mgr_map'
    INSTRUMENT_IDENTIFIER = 'instrument_identifier'
    DATALOGGER_STATE = 'datalogger_state'
    BYTES_SENT = 'bytes_sent'
    BYTES_RECEIVED = 'bytes_received'
    BYTES_LOGGED = 'bytes_logged'
    GOOD_RECORDS = 'good_records'
    BAD_RECORDS = 'bad_records'
    BAD_BYTES = 'bad_bytes'
    TIME_RECEIVED_LAST_DATA = 'time_received_last_data'
    TIME_LAST_COMMUNICATED = 'time_last_communicated'
    MESSAGE_SENT_TYPE = 'message_sent_type'
    SYNC_TYPE = 'sync_type'
    NTP_OFFSET = 'ntp_offset'
    NTP_JITTER = 'ntp_jitter'
    ACCELERATION_X = 'acceleration_x'
    ACCELERATION_Y = 'acceleration_y'
    ACCELERATION_Z = 'acceleration_z'
    ANGULAR_RATE_X = 'angular_rate_x'
    ANGULAR_RATE_Y = 'angular_rate_y'
    ANGULAR_RATE_Z = 'angular_rate_z'
    MAGNETOMETER_X = 'magnetometer_x'
    MAGNETOMETER_Y = 'magnetometer_y'
    MAGNETOMETER_Z = 'magnetometer_z'
    TIC_COUNTER = 'tic_counter'


"""
Regex definitions to match channel parameters
"""
CHANNEL_STATE_REGEX = r'channel_\d_state'
CHANNEL_V_REGEX = r'channel_\d_v'
CHANNEL_I_REGEX = r'channel_\d_i'
CHANNEL_ERROR_STATUS_REGEX = r'channel_\d_error_status'

"""
Channel parameters used in encoding rules and the superv regex
"""
CHANNEL_1_STATE = 'channel_1_state'
CHANNEL_1_V = 'channel_1_v'
CHANNEL_1_I = 'channel_1_i'
CHANNEL_1_ERROR_STATUS = 'channel_1_error_status'
CHANNEL_2_STATE = 'channel_2_state'
CHANNEL_2_V = 'channel_2_v'
CHANNEL_2_I = 'channel_2_i'
CHANNEL_2_ERROR_STATUS = 'channel_2_error_status'
CHANNEL_3_STATE = 'channel_3_state'
CHANNEL_3_V = 'channel_3_v'
CHANNEL_3_I = 'channel_3_i'
CHANNEL_3_ERROR_STATUS = 'channel_3_error_status'
CHANNEL_4_STATE = 'channel_4_state'
CHANNEL_4_V = 'channel_4_v'
CHANNEL_4_I = 'channel_4_i'
CHANNEL_4_ERROR_STATUS = 'channel_4_error_status'
CHANNEL_5_STATE = 'channel_5_state'
CHANNEL_5_V = 'channel_5_v'
CHANNEL_5_I = 'channel_5_i'
CHANNEL_5_ERROR_STATUS = 'channel_5_error_status'
CHANNEL_6_STATE = 'channel_6_state'
CHANNEL_6_V = 'channel_6_v'
CHANNEL_6_I = 'channel_6_i'
CHANNEL_6_ERROR_STATUS = 'channel_6_error_status'
CHANNEL_7_STATE = 'channel_7_state'
CHANNEL_7_V = 'channel_7_v'
CHANNEL_7_I = 'channel_7_i'
CHANNEL_7_ERROR_STATUS = 'channel_7_error_status'
CHANNEL_8_STATE = 'channel_8_state'
CHANNEL_8_V = 'channel_8_v'
CHANNEL_8_I = 'channel_8_i'
CHANNEL_8_ERROR_STATUS = 'channel_8_error_status'


#####################
# Encoding Functions
#####################
def bool_string_to_int(value):
    """
    Convert a boolean string to corresponding integer value
    :param value:  e.g. 'TRUE'
    :return:  1 if string is 'true', 0 otherwise
    :throws:  AttributeError if value is not type of string
    """
    return 1 if value.lower() == 'true' else 0


"""
Encoding rules used in _build_parsed_value methods used build the particles.
"""
ENCODING_RULES_DICT = {
    ParticleKey.HEADER_TIMESTAMP: str,
    ParticleKey.GPS_COUNTS: int,
    ParticleKey.NTP_COUNTS: int,
    ParticleKey.PPS_COUNTS: int,
    ParticleKey.SUPERV_COUNTS: int,
    ParticleKey.DLOG_MGR_COUNTS: int,
    ParticleKey.UPTIME_STRING: str,
    ParticleKey.LOAD_VAL_1: float,
    ParticleKey.LOAD_VAL_2: float,
    ParticleKey.LOAD_VAL_3: float,
    ParticleKey.MEM_FREE: int,
    ParticleKey.NUM_PROCESSES: int,
    ParticleKey.LOG_TYPE: str,
    ParticleKey.MESSAGE_TEXT: str,
    ParticleKey.MESSAGE_SENT_TIMESTAMP: str,
    ParticleKey.LATITUDE: float,
    ParticleKey.LONGITUDE: float,
    ParticleKey.GPS_SPEED: float,
    ParticleKey.GPS_TRUE_COURSE: float,
    ParticleKey.GPS_QUALITY: int,
    ParticleKey.GPS_NUM_SATELLITES: int,
    ParticleKey.GPS_HDOP: float,
    ParticleKey.GPS_ALTITUDE: float,
    ParticleKey.DATE_OF_FIX: str,
    ParticleKey.TIME_OF_FIX: str,
    ParticleKey.LATITUDE_ALT_FORMAT: str,
    ParticleKey.LONGITUDE_ALT_FORMAT: str,
    ParticleKey.NMEA_LOCK: bool_string_to_int,
    ParticleKey.DELTA: int,
    ParticleKey.DELTA_MIN: int,
    ParticleKey.DELTA_MAX: int,
    ParticleKey.BAD_PULSES: int,
    ParticleKey.BOARD_TYPE: str,
    ParticleKey.VMAIN_BACKPLANE_BUS_VOLTAGE: float,
    ParticleKey.IMAIN_CURRENT: float,
    ParticleKey.ERROR_VMAIN_OUT_TOLERANCE: int,
    ParticleKey.ERROR_IMAIN_OUT_TOLERANCE: int,
    ParticleKey.ERROR_DCL_ISO_SWGF_LIM_EXCEEDED: int,
    ParticleKey.ERROR_DCL_RTN_SWFG_LIM_EXCEEDED: int,
    ParticleKey.ERROR_VMAIN_SWGF_LIM_EXCEEDED: int,
    ParticleKey.ERROR_GMAIN_SWGF_LIM_EXCEEDED: int,
    ParticleKey.ERROR_SENSOR_ISO_SWGF_LIM_EXCEEDED: int,
    ParticleKey.ERROR_SNSR_COM_SWGF_LIM_EXCEEDED: int,
    ParticleKey.ERROR_LEAK_DETECT_C1_LIM_EXCEEDED: int,
    ParticleKey.ERROR_LEAK_DETECT_C2_LIM_EXCEEDED: int,
    ParticleKey.ERROR_CHANNEL_OVERCURRENT_FAULT: int,
    ParticleKey.ERROR_CHANNEL_1_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_2_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_3_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_4_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_5_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_6_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_7_NOT_RESPONDING: int,
    ParticleKey.ERROR_CHANNEL_8_NOT_RESPONDING: int,
    ParticleKey.ERROR_I2C_ERROR: int,
    ParticleKey.ERROR_UART_ERROR: int,
    ParticleKey.ERROR_BROWN_OUT_RESET: int,
    ParticleKey.BMP085_TEMP: float,
    ParticleKey.SHT25_TEMP: float,
    ParticleKey.MURATA_12V_TEMP: float,
    ParticleKey.MURATA_24V_TEMP: float,
    ParticleKey.VICOR_12V_BCM_TEMP: float,
    ParticleKey.SHT25_HUMIDITY: float,
    ParticleKey.BMP085_PRESSURE: float,
    ParticleKey.ACTIVE_SWGF_CHANNELS: int,
    ParticleKey.SWGF_C1_MAX_LEAKAGE: float,
    ParticleKey.SWGF_C2_MAX_LEAKAGE: float,
    ParticleKey.SWGF_C3_MAX_LEAKAGE: float,
    ParticleKey.ACTIVE_LEAK_DETECT_CHANNELS: int,
    ParticleKey.LEAK_DETECT_C1_V: int,
    ParticleKey.LEAK_DETECT_C2_V: int,
    CHANNEL_1_STATE: int,
    CHANNEL_1_V: float,
    CHANNEL_1_I: float,
    CHANNEL_1_ERROR_STATUS: int,
    CHANNEL_2_STATE: int,
    CHANNEL_2_V: float,
    CHANNEL_2_I: float,
    CHANNEL_2_ERROR_STATUS: int,
    CHANNEL_3_STATE: int,
    CHANNEL_3_V: float,
    CHANNEL_3_I: float,
    CHANNEL_3_ERROR_STATUS: int,
    CHANNEL_4_STATE: int,
    CHANNEL_4_V: float,
    CHANNEL_4_I: float,
    CHANNEL_4_ERROR_STATUS: int,
    CHANNEL_5_STATE: int,
    CHANNEL_5_V: float,
    CHANNEL_5_I: float,
    CHANNEL_5_ERROR_STATUS: int,
    CHANNEL_6_STATE: int,
    CHANNEL_6_V: float,
    CHANNEL_6_I: float,
    CHANNEL_6_ERROR_STATUS: int,
    CHANNEL_7_STATE: int,
    CHANNEL_7_V: float,
    CHANNEL_7_I: float,
    CHANNEL_7_ERROR_STATUS: int,
    CHANNEL_8_STATE: int,
    CHANNEL_8_V: float,
    CHANNEL_8_I: float,
    CHANNEL_8_ERROR_STATUS: int,
    ParticleKey.PWR_BOARD_MODE: int,
    ParticleKey.DPB_MODE: int,
    ParticleKey.DPB_VOLTAGE_MODE: int,
    ParticleKey.VMAIN_DPB_IN: float,
    ParticleKey.IMAIN_DPB_IN: float,
    ParticleKey.OUT_12V_V: float,
    ParticleKey.OUT_12V_I: float,
    ParticleKey.OUT_24V_V: float,
    ParticleKey.OUT_24V_I: float,
    ParticleKey.DATALOGGER_TIMESTAMP: str,
    ParticleKey.DLOG_MGR_ACT: int,
    ParticleKey.DLOG_MGR_STR: int,
    ParticleKey.DLOG_MGR_HLT: int,
    ParticleKey.DLOG_MGR_FLD: int,
    ParticleKey.DLOG_MGR_MAP: str,
    ParticleKey.INSTRUMENT_IDENTIFIER: str,
    ParticleKey.DATALOGGER_STATE: str,
    ParticleKey.BYTES_SENT: int,
    ParticleKey.BYTES_RECEIVED: int,
    ParticleKey.BYTES_LOGGED: int,
    ParticleKey.GOOD_RECORDS: int,
    ParticleKey.BAD_RECORDS: int,
    ParticleKey.BAD_BYTES: int,
    ParticleKey.TIME_RECEIVED_LAST_DATA: int,
    ParticleKey.TIME_LAST_COMMUNICATED: int,
    ParticleKey.MESSAGE_SENT_TYPE: str,
    ParticleKey.SYNC_TYPE: str,
    ParticleKey.NTP_OFFSET: float,
    ParticleKey.NTP_JITTER: float,
    ParticleKey.ACCELERATION_X: float,
    ParticleKey.ACCELERATION_Y: float,
    ParticleKey.ACCELERATION_Z: float,
    ParticleKey.ANGULAR_RATE_X: float,
    ParticleKey.ANGULAR_RATE_Y: float,
    ParticleKey.ANGULAR_RATE_Z: float,
    ParticleKey.MAGNETOMETER_X: float,
    ParticleKey.MAGNETOMETER_Y: float,
    ParticleKey.MAGNETOMETER_Z: float,
    ParticleKey.TIC_COUNTER: float,
}

# Verify ranges for the following fields
KEY_RANGE = {
    ParticleKey.LATITUDE: (-90, 90),
    ParticleKey.LONGITUDE: (-360, 360),
}

# A base regex used to match all cg_dcl_eng_dcl MSG records
MSG_LOG_TYPE_BASE_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                          DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                          ')\s+MSG'

# A regex used to match various MSG type records
MSG_LOG_TYPE_REGEX = MSG_LOG_TYPE_BASE_REGEX + '\s+D_STATUS\s+(STATUS|CPU\s+Uptime):'
ERR_ALM_WNG_LOG_TYPE_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                             DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                             ')\s+(?P<' + ParticleKey.LOG_TYPE + \
                             '>(?:ERR|ALM|WNG))'

# A base regex used to DAT records - only used to match which type to parse, but doesn't use a group for it, durp
_old_DAT_LOG_TYPE_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+DAT'

DAT_LOG_TYPE_REGEX = build_regex_string([
    (ParticleKey.HEADER_TIMESTAMP, DATE_YYYY_MM_DD_REGEX),
    (None, TIME_HR_MIN_SEC_MSEC_REGEX),
    (None, 'DAT'),
    ('type', '\w+'),
])

# Regex for particles CG_DCL_ENG_DCL_MSG_COUNTS_TELEMETERED and CG_DCL_ENG_DCL_MSG_COUNTS_RECOVERED
# Example file record:
# 2013/12/20 00:05:39.802 MSG D_STATUS STATUS: GPS=600, NTP=0, PPS=30, SUPERV=147, DLOG_MGR=9
MSG_COUNTS_REGEX = MSG_LOG_TYPE_BASE_REGEX + \
                   '\s+D_STATUS\s+STATUS:\s+GPS=(?P<' + \
                   ParticleKey.GPS_COUNTS + \
                   '>' + INT_REGEX + '),\s+NTP=(?P<' + \
                   ParticleKey.NTP_COUNTS + \
                   '>' + INT_REGEX + '),\s+PPS=(?P<' + \
                   ParticleKey.PPS_COUNTS + \
                   '>' + INT_REGEX + '),\s+SUPERV=(?P<' + \
                   ParticleKey.SUPERV_COUNTS + \
                   '>' + INT_REGEX + '),\s+DLOG_MGR=(?P<' + \
                   ParticleKey.DLOG_MGR_COUNTS + \
                   '>' + INT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_CPU_UPTIME_TELEMETERED and CG_DCL_ENG_DCL_CPU_UPTIME_RECOVERED
# Example file record:
# 2013/12/20 00:10:40.248 MSG D_STATUS CPU Uptime:  28 days 09:05:18  Load: 0.02 0.23 0.23  Free: 23056k  Nproc: 81
CPU_UPTIME_REGEX = MSG_LOG_TYPE_BASE_REGEX + \
                   '\s+D_STATUS\s+CPU\s+Uptime:\s+(?P<' + \
                   ParticleKey.UPTIME_STRING + \
                   '>' + INT_REGEX + '\s+days\s+' + TIME_HR_MIN_SEC_REGEX + \
                   ')\s+Load:\s+(?P<' + ParticleKey.LOAD_VAL_1 + \
                   '>' + FLOAT_REGEX + ')\s+(?P<' + \
                   ParticleKey.LOAD_VAL_2 + \
                   '>' + FLOAT_REGEX + ')\s+(?P<' + \
                   ParticleKey.LOAD_VAL_3 + \
                   '>' + FLOAT_REGEX + ')\s+Free:\s+(?P<' + \
                   ParticleKey.MEM_FREE + \
                   '>' + INT_REGEX + ')k\s+Nproc:\s+(?P<' + \
                   ParticleKey.NUM_PROCESSES + \
                   '>' + INT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_ERROR_TELEMETERED and CG_DCL_ENG_DCL_ERROR_RECOVERED
# Example file records:
# 2013/12/20 00:16:28.642 ERR DLOGP7 Command [(NULL)] Received no recognized response
# 2013/12/20 01:33:45.515 ALM D_GPS Warning 4 3, BAD GPS CHECKSUM
ERROR_REGEX = ERR_ALM_WNG_LOG_TYPE_REGEX + \
              '\s+(?P<' + ParticleKey.MESSAGE_TEXT + \
              '>.*)' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_GPS_TELEMETERED and CG_DCL_ENG_DCL_GPS_RECOVERED
# Example file record:
# 2013/12/20 01:30:45.503 DAT D_GPS 2013/12/20 01:30:44.848 GPS 40.136760 -70.769495 3.40 070.90 2 9 0.80 0.80 201213
# 013044 4008.2056 N 07046.1697 W
GPS_REGEX_TUPLE = [
    (ParticleKey.HEADER_TIMESTAMP, DCL_TIMESTAMP_REGEX),  # 2013/12/20 01:30:45.503
    (None, 'DAT'),
    (None, 'D_GPS'),
    (ParticleKey.MESSAGE_SENT_TIMESTAMP, '\S+\s+\S+'),  # 2013/12/20 01:30:44.848
    (None, 'GPS'),
    (ParticleKey.LATITUDE, '\S+'),  # 40.136760
    (ParticleKey.LONGITUDE, '\S+'),  # -70.769495
    (ParticleKey.GPS_SPEED, '\S+'),  # 3.40
    (ParticleKey.GPS_TRUE_COURSE, '\S+'),  # 070.90
    (ParticleKey.GPS_QUALITY, '\S+'),  # 2
    (ParticleKey.GPS_NUM_SATELLITES, '\S+'),  # 9
    (ParticleKey.GPS_HDOP, '\S+'),  # 0.80
    (ParticleKey.GPS_ALTITUDE, '\S+'),  # 0.80
    (ParticleKey.DATE_OF_FIX, '\S+'),  # 201213
    (ParticleKey.TIME_OF_FIX, '\S+'),  # 013044
    (ParticleKey.LATITUDE_ALT_FORMAT, '\S+\s[NS]'),  # 4008.2056 N
    (ParticleKey.LONGITUDE_ALT_FORMAT, '\S+\s[WE]$'),  # 07046.1697 W
]

GPS_REGEX = build_regex_string(GPS_REGEX_TUPLE)

# Regex for particles CG_DCL_ENG_DCL_PPS_TELEMETERED and CG_DCL_ENG_DCL_PPS_RECOVERED
# Example file record:
# 2013/12/20 01:30:45.504 DAT D_PPS D_PPS: NMEA_Lock: TRUE  Delta: 0999996 DeltaMin: +0000000 DeltaMax: -0000023
# BadPulses: 0000 TS: 2013/12/20 01:30:42.000
PPS_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
            DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
            ')\s+DAT\s+D_PPS\s+D_PPS:\s+NMEA_Lock:\s+(?P<' + \
            ParticleKey.NMEA_LOCK + \
            '>(?:TRUE|FALSE|True|False|true|false))\s+Delta:\s+(?P<' + \
            ParticleKey.DELTA + '>' + INT_REGEX + \
            ')\s+DeltaMin:\s+(?P<' + ParticleKey.DELTA_MIN + \
            '>' + INT_REGEX + ')\s+DeltaMax:\s+(?P<' + \
            ParticleKey.DELTA_MAX + '>' + INT_REGEX + \
            ')\s+BadPulses:\s+(?P<' + ParticleKey.BAD_PULSES + \
            '>' + INT_REGEX + ')\s+TS:\s+(?P<' + \
            ParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
            DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + ')' + \
            END_OF_LINE_REGEX

ERROR_BITS_PARAM = 'error_bits'

# Regex for particles CG_DCL_ENG_DCL_SUPERV_TELEMETERED and CG_DCL_ENG_DCL_SUPERV_RECOVERED
# Example file record:
# 2013/12/20 01:20:44.908 DAT SUPERV dcl: 24.0 412.0 00001070 t 22.2 19.9 25.3 25.3 45.9 h 23.8 p 14.5 gf 7 -5.2
# -531.9 -595.2 ld 3 1250 1233 p1 0 0.0 0.0 0 p2 0 0.0 0.0 0 p3 0 0.0 0.0 0 p4 1 11.9 4.9 0 p5 0 0.0 0.0 0 p6 1
# 11.9 224.8 0 p7 0 0.0 0.0 0 p8 1 12.0 39.1 1 hb 0 0 0 wake 0 wtc 0 wpc 0 pwr 2 2 3 23.9 400.1 12.0 280.1 0.0 0.0 3b93
SUPERV_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
               DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
               ')\s+DAT\s+SUPERV\s+(?P<' + \
               ParticleKey.BOARD_TYPE + \
               '>.*):\s+(?P<' + \
               ParticleKey.VMAIN_BACKPLANE_BUS_VOLTAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.IMAIN_CURRENT + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ERROR_BITS_PARAM + \
               '>' + ASCII_HEX_CHAR_REGEX + '{8})\s+t\s+(?P<' + \
               ParticleKey.BMP085_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.SHT25_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.MURATA_12V_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.MURATA_24V_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.VICOR_12V_BCM_TEMP + \
               '>' + FLOAT_REGEX + ')\s+h\s+(?P<' + \
               ParticleKey.SHT25_HUMIDITY + \
               '>' + FLOAT_REGEX + ')\s+p\s+(?P<' + \
               ParticleKey.BMP085_PRESSURE + \
               '>' + FLOAT_REGEX + ')\s+gf\s+(?P<' + \
               ParticleKey.ACTIVE_SWGF_CHANNELS + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.SWGF_C1_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.SWGF_C2_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.SWGF_C3_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+ld\s+(?P<' + \
               ParticleKey.ACTIVE_LEAK_DETECT_CHANNELS + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.LEAK_DETECT_C1_V + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.LEAK_DETECT_C2_V + \
               '>' + INT_REGEX + ')\s+p1\s+(?P<' + \
               CHANNEL_1_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_1_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_1_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_1_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p2\s+(?P<' + \
               CHANNEL_2_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_2_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_2_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_2_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p3\s+(?P<' + \
               CHANNEL_3_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_3_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_3_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_3_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p4\s+(?P<' + \
               CHANNEL_4_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_4_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_4_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_4_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p5\s+(?P<' + \
               CHANNEL_5_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_5_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_5_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_5_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p6\s+(?P<' + \
               CHANNEL_6_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_6_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_6_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_6_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p7\s+(?P<' + \
               CHANNEL_7_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_7_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_7_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_7_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+p8\s+(?P<' + \
               CHANNEL_8_STATE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CHANNEL_8_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_8_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CHANNEL_8_ERROR_STATUS + \
               '>' + INT_REGEX + ')\s+.*\s+pwr\s+(?P<' + \
               ParticleKey.PWR_BOARD_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.DPB_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.DPB_VOLTAGE_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               ParticleKey.VMAIN_DPB_IN + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.IMAIN_DPB_IN + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.OUT_12V_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.OUT_12V_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.OUT_24V_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ParticleKey.OUT_24V_I + \
               '>' + FLOAT_REGEX + ')\s+.*' + \
               END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_DLOG_MGR_TELEMETERED and CG_DCL_ENG_DCL_DLOG_MGR_RECOVERED
# Example file record:
# 2013/12/20 18:57:10.822 DAT DLOG_MGR dmgrstatus: 2013/12/20 18:56:41.177 act:4 str:4 hlt:0 fld:0 map:000D0DDD
DLOG_MGR_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                 DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                 ')\s+DAT\s+DLOG_MGR\s+dmgrstatus:\s+(?P<' + \
                 ParticleKey.DATALOGGER_TIMESTAMP + '>' + \
                 DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                 ')\s+act:(?P<' + ParticleKey.DLOG_MGR_ACT + \
                 '>\d)\s+str:(?P<' + ParticleKey.DLOG_MGR_STR + \
                 '>\d)\s+hlt:(?P<' + ParticleKey.DLOG_MGR_HLT + \
                 '>\d)\s+fld:(?P<' + ParticleKey.DLOG_MGR_FLD + \
                 '>\d)\s+map:(?P<' + ParticleKey.DLOG_MGR_MAP + \
                 '>.*)' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_DLOG_STATUS_TELEMETERED and CG_DCL_ENG_DCL_DLOG_STATUS_RECOVERED
# Example file record:
# 2014/09/15 00:54:26.910 DAT DLOGP5 istatus: 2014/09/15 00:54:16.477 spkir1 IDLE tx: 598 rx: 1692514 log: 2399666
# good: 27765 bad: 64 bb: 0 ld: 1410740284 lc: 14107
DLOG_STATUS_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                    DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                    ')\s+DAT\s+DLOGP\d\s+istatus:\s+(?P<' + \
                    ParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
                    DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                    ')\s+(?P<' + ParticleKey.INSTRUMENT_IDENTIFIER + \
                    '>\w+)\s+(?P<' + ParticleKey.DATALOGGER_STATE + \
                    '>\w+)\s+tx:\s+(?P<' + ParticleKey.BYTES_SENT + \
                    '>\d+)\s+rx:\s+(?P<' + ParticleKey.BYTES_RECEIVED + \
                    '>\d+)\s+log:\s+(?P<' + ParticleKey.BYTES_LOGGED + \
                    '>\d+)\s+good:\s+(?P<' + ParticleKey.GOOD_RECORDS + \
                    '>\d+)\s+bad:\s+(?P<' + ParticleKey.BAD_RECORDS + \
                    '>\d+)\s+bb:\s+(?P<' + ParticleKey.BAD_BYTES + \
                    '>\d+)\s+ld:\s+(?P<' + ParticleKey.TIME_RECEIVED_LAST_DATA + \
                    '>\d+)(\s+lc:\s+(?P<' + ParticleKey.TIME_LAST_COMMUNICATED + \
                    '>\d+))?' + END_OF_LINE_REGEX

ERROR_BIT_NOT_USED = 'ERROR_BIT_NOT_USED'

ERROR_BITS = [
    ParticleKey.ERROR_VMAIN_OUT_TOLERANCE,
    ParticleKey.ERROR_IMAIN_OUT_TOLERANCE,
    ParticleKey.ERROR_DCL_ISO_SWGF_LIM_EXCEEDED,
    ParticleKey.ERROR_DCL_RTN_SWFG_LIM_EXCEEDED,
    ParticleKey.ERROR_VMAIN_SWGF_LIM_EXCEEDED,
    ParticleKey.ERROR_GMAIN_SWGF_LIM_EXCEEDED,
    ParticleKey.ERROR_SENSOR_ISO_SWGF_LIM_EXCEEDED,
    ParticleKey.ERROR_SNSR_COM_SWGF_LIM_EXCEEDED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ParticleKey.ERROR_LEAK_DETECT_C1_LIM_EXCEEDED,
    ParticleKey.ERROR_LEAK_DETECT_C2_LIM_EXCEEDED,
    ParticleKey.ERROR_CHANNEL_OVERCURRENT_FAULT,
    ParticleKey.ERROR_CHANNEL_1_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_2_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_3_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_4_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_5_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_6_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_7_NOT_RESPONDING,
    ParticleKey.ERROR_CHANNEL_8_NOT_RESPONDING,
    ParticleKey.ERROR_I2C_ERROR,
    ParticleKey.ERROR_UART_ERROR,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ParticleKey.ERROR_BROWN_OUT_RESET,
]

# Regex for particles CG_DCL_ENG_DCL_STATUS_TELEMETERED and CG_DCL_ENG_DCL_STATUS_RECOVERED
# Example file record:
# 2014/09/15 00:04:20.260 DAT D_STATUS NTP: 2014/09/15 00:04:20.031 *SHM(1) .PPS. -0.202 0.067
D_STATUS_NTP_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+DAT\s+D_STATUS\s+(?P<' + \
                     ParticleKey.MESSAGE_SENT_TYPE + \
                     '>\w+):\s+(?P<' + \
                     ParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+(?P<' + ParticleKey.SYNC_TYPE + '>' + \
                     '.*)\s+((?P<' + ParticleKey.NTP_OFFSET + \
                     '>' + FLOAT_REGEX + ')\s+(?P<' + \
                     ParticleKey.NTP_JITTER + '>' + FLOAT_REGEX + \
                     '))?' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_SEA_STATE_TELEMETERED and CG_DCL_ENG_DCL_SEA_STATE_RECOVERED
# Example file record:
# 2014/09/15 22:22:50.917 DAT DLOGP1 3DM CB_AARM ax: -0.089718 ay: -0.073573 az: -0.864567 rx: -0.126140 ry: 0.090486
# rz: 0.114195 mx: 0.062305 my: -0.140416 mz: 0.498844 t: 1194.34
DLOG_AARM_REGEX = r'(?P<' + ParticleKey.HEADER_TIMESTAMP + '>' + \
                  DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                  ')\s+DAT\s+DLOGP\d\s+3DM\s+CB_AARM\s+ax:\s+(?P<' + \
                  ParticleKey.ACCELERATION_X + '>' + FLOAT_REGEX + \
                  ')\s+ay:\s+(?P<' + ParticleKey.ACCELERATION_Y + \
                  '>' + FLOAT_REGEX + ')\s+az:\s+(?P<' + ParticleKey.ACCELERATION_Z + \
                  '>' + FLOAT_REGEX + ')\s+rx:\s+(?P<' + ParticleKey.ANGULAR_RATE_X + \
                  '>' + FLOAT_REGEX + ')\s+ry:\s+(?P<' + ParticleKey.ANGULAR_RATE_Y + \
                  '>' + FLOAT_REGEX + ')\s+rz:\s+(?P<' + ParticleKey.ANGULAR_RATE_Z + \
                  '>' + FLOAT_REGEX + ')\s+mx:\s+(?P<' + ParticleKey.MAGNETOMETER_X + \
                  '>' + FLOAT_REGEX + ')\s+my:\s+(?P<' + ParticleKey.MAGNETOMETER_Y + \
                  '>' + FLOAT_REGEX + ')\s+mz:\s+(?P<' + ParticleKey.MAGNETOMETER_Z + \
                  '>' + FLOAT_REGEX + ')\s+t:\s+(?P<' + \
                  ParticleKey.TIC_COUNTER + '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for records to ignore
IGNORE_REGEX = r'(' + END_OF_LINE_REGEX + '|' + DATE_YYYY_MM_DD_REGEX + '\s+' + \
               TIME_HR_MIN_SEC_MSEC_REGEX + '\s+MSG\s+D_CTL\s+.*' + END_OF_LINE_REGEX + '?)'


class CgDclEngDclDataParticle(DataParticle):
    """
    Base data particle for cg_dcl_eng_dcl.
    """

    def _encode_value(self, key, data=None, encoding=None, value_range=None):
        """
        Wrapper for base class _encode_value - auto-populates based on key

        :param key:  key name for the particle field
        :param data:  raw string to be encoded
        :param encoding:  encoding function
        :param value_range:  (optional) range tuple to specify required inclusive range
        :return:
        """
        # auto-populate any missing parameters
        if data is None:
            data = self.raw_data[key]
        encoding = encoding if encoding else ENCODING_RULES_DICT.get(key, str)
        value_range = value_range if value_range else KEY_RANGE.get(key, None)

        # DataParticle is a dict, not an object, so we can't use super()
        return DataParticle._encode_value(self, key, data, encoding, value_range=value_range)

    def _build_parsed_values(self):
        """
        This is the default implementation which many cg_dcl_eng_dcl particles can
        take advantage of.
        """
        result = []

        for key in self.raw_data.keys():

            if key == ParticleKey.HEADER_TIMESTAMP:
                # DCL controller timestamp  is the port_timestamp
                self.set_port_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ParticleKey.MESSAGE_SENT_TIMESTAMP:
                # instrument timestamp  is the internal_timestamp
                self.set_internal_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            else:
                result.append(self._encode_value(key))

        return result


class CgDclEngDclMsgCountsRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.MSG_COUNTS_RECOVERED


class CgDclEngDclMsgCountsTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.MSG_COUNTS


class CgDclEngDclCpuUptimeRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.CPU_UPTIME_RECOVERED


class CgDclEngDclCpuUptimeTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.CPU_UPTIME


class CgDclEngDclErrorRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.ERROR_RECOVERED


class CgDclEngDclErrorTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.ERROR


class CgDclEngDclGpsRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.GPS_RECOVERED


class CgDclEngDclGpsTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.GPS


class CgDclEngDclPpsRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.PPS_RECOVERED


class CgDclEngDclPpsTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.PPS


class CgDclEngDclSupervDataParticle(CgDclEngDclDataParticle):
    """
    Class for building a CgDclEngDclSupervDataParticle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """
        result = []

        channel_states = []
        channel_vs = []
        channel_is = []
        channel_error_statuses = []

        # IMPORTANT: The keys must be sorted in order for the list of channel data to
        # be correctly ordered.
        for key in sorted(self.raw_data.keys()):

            if key == ParticleKey.HEADER_TIMESTAMP:
                # DCL controller timestamp  is the port_timestamp
                self.set_port_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ParticleKey.MESSAGE_SENT_TIMESTAMP:
                # instrument timestamp  is the internal_timestamp
                self.set_internal_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ERROR_BITS_PARAM:

                error_bits = format(int(self.raw_data[key], 16), '016b')

                for bit_field, bit in zip(ERROR_BITS, reversed(error_bits)):

                    if bit_field != ERROR_BIT_NOT_USED:
                        result.append(self._encode_value(bit_field, bit))

            elif re.match(CHANNEL_STATE_REGEX, key):

                channel_states.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_V_REGEX, key):

                channel_vs.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_I_REGEX, key):

                channel_is.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_ERROR_STATUS_REGEX, key):

                channel_error_statuses.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            else:
                result.append(self._encode_value(key))

        result.append(self._encode_value(ParticleKey.CHANNEL_STATE, channel_states, list))
        result.append(self._encode_value(ParticleKey.CHANNEL_V, channel_vs, list))
        result.append(self._encode_value(ParticleKey.CHANNEL_I, channel_is, list))
        result.append(self._encode_value(ParticleKey.CHANNEL_ERROR_STATUS, channel_error_statuses, list))

        return result


class CgDclEngDclSupervRecoveredDataParticle(CgDclEngDclSupervDataParticle):
    _data_particle_type = ParticleType.SUPERV_RECOVERED


class CgDclEngDclSupervTelemeteredDataParticle(CgDclEngDclSupervDataParticle):
    _data_particle_type = ParticleType.SUPERV


class CgDclEngDclDlogMgrRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.MGR_RECOVERED


class CgDclEngDclDlogMgrTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.DLOG_MGR


class CgDclEngDclDlogStatusDataParticle(CgDclEngDclDataParticle):
    """
    Class for building a CgDclEngDclDlogStatusDataParticle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """
        result = []

        log.trace("dlog status raw_data: %s", self.raw_data)

        for key in self.raw_data.keys():

            if key == ParticleKey.HEADER_TIMESTAMP:
                # DCL controller timestamp  is the port_timestamp
                self.set_port_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ParticleKey.MESSAGE_SENT_TIMESTAMP:
                # instrument timestamp  is the internal_timestamp
                self.set_internal_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ParticleKey.TIME_LAST_COMMUNICATED and self.raw_data[key] is None:

                result.append({
                    DataParticleKey.VALUE_ID: key,
                    DataParticleKey.VALUE: None
                })

            else:
                result.append(self._encode_value(key))

        return result


class CgDclEngDclDlogStatusRecoveredDataParticle(CgDclEngDclDlogStatusDataParticle):
    _data_particle_type = ParticleType.DLOG_STATUS_RECOVERED


class CgDclEngDclDlogStatusTelemeteredDataParticle(CgDclEngDclDlogStatusDataParticle):
    _data_particle_type = ParticleType.DLOG_STATUS


class CgDclEngDclStatusDataParticle(CgDclEngDclDataParticle):
    """
    Class for building a CgDclEngDclStatusDataParticle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        result = []

        log.trace("status raw_data: %s", self.raw_data)

        for key in self.raw_data.keys():

            if key == ParticleKey.HEADER_TIMESTAMP:
                # DCL controller timestamp  is the port_timestamp
                self.set_port_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key == ParticleKey.MESSAGE_SENT_TIMESTAMP:
                # instrument timestamp  is the internal_timestamp
                self.set_internal_timestamp(mi.dataset.parser.utilities.dcl_time_to_ntp(self.raw_data[key]))

            elif key in [ParticleKey.NTP_OFFSET, ParticleKey.NTP_JITTER] and self.raw_data[key] is None:

                result.append({
                    DataParticleKey.VALUE_ID: key,
                    DataParticleKey.VALUE: None
                })

            else:

                result.append(self._encode_value(key))

        return result


class CgDclEngDclStatusRecoveredDataParticle(CgDclEngDclStatusDataParticle):
    _data_particle_type = ParticleType.STATUS_RECOVERED


class CgDclEngDclStatusTelemeteredDataParticle(CgDclEngDclStatusDataParticle):
    _data_particle_type = ParticleType.STATUS


class CgDclEngDclDlogAarmRecoveredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.DLOG_AARM_RECOVERED


class CgDclEngDclDlogAarmTelemeteredDataParticle(CgDclEngDclDataParticle):
    _data_particle_type = ParticleType.DLOG_AARM


class CgDclEngDclParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        # no sieve function since we are not using the chunker here
        super(CgDclEngDclParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback=None)

        try:
            particle_classes_dict = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT]
            self._msg_counts_particle_class = particle_classes_dict[
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS]
            self._cpu_uptime_particle_class = particle_classes_dict[
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS]
            self._error_particle_class = particle_classes_dict[
                ParticleClassTypes.ERROR_PARTICLE_CLASS]
            self._gps_particle_class = particle_classes_dict[
                ParticleClassTypes.GPS_PARTICLE_CLASS]
            self._pps_particle_class = particle_classes_dict[
                ParticleClassTypes.PPS_PARTICLE_CLASS]
            self._superv_particle_class = particle_classes_dict[
                ParticleClassTypes.SUPERV_PARTICLE_CLASS]
            self._dlog_mgr_particle_class = particle_classes_dict[
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS]
            self._dlog_status_particle_class = particle_classes_dict[
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS]
            self._status_particle_class = particle_classes_dict[
                ParticleClassTypes.STATUS_PARTICLE_CLASS]
            self._dlog_aarm_particle_class = particle_classes_dict[
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS]

            self._particle_classes = {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS: self._msg_counts_particle_class,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS: self._cpu_uptime_particle_class,
                ParticleClassTypes.ERROR_PARTICLE_CLASS: self._error_particle_class,
                ParticleClassTypes.GPS_PARTICLE_CLASS: self._gps_particle_class,
                ParticleClassTypes.PPS_PARTICLE_CLASS: self._pps_particle_class,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS: self._superv_particle_class,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS: self._dlog_mgr_particle_class,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS: self._dlog_status_particle_class,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS: self._dlog_aarm_particle_class,
                ParticleClassTypes.STATUS_PARTICLE_CLASS: self._status_particle_class,
            }
            self._particle_regex = {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS: MSG_COUNTS_REGEX,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS: CPU_UPTIME_REGEX,
                ParticleClassTypes.ERROR_PARTICLE_CLASS: ERROR_REGEX,
                ParticleClassTypes.GPS_PARTICLE_CLASS: GPS_REGEX,
                ParticleClassTypes.PPS_PARTICLE_CLASS: PPS_REGEX,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS: SUPERV_REGEX,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS: DLOG_MGR_REGEX,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS: DLOG_STATUS_REGEX,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS: DLOG_AARM_REGEX,
                ParticleClassTypes.STATUS_PARTICLE_CLASS: D_STATUS_NTP_REGEX,
            }

        except (KeyError, AttributeError):
            message = "Invalid cg_dcl_eng_dcl configuration parameters."
            log.error("Error: %s", message)
            raise ConfigurationException(message)

    def _extract_sample(self, particle_class, regex, raw_data, port_timestamp=None, internal_timestamp=None,
                        preferred_ts=DataParticleKey.PORT_TIMESTAMP):
        return super(CgDclEngDclParser, self)._extract_sample(
            particle_class=particle_class, regex=regex, raw_data=raw_data, port_timestamp=port_timestamp,
            internal_timestamp=internal_timestamp, preferred_ts=preferred_ts)

    def parse_file(self):
        """
        This method will parse a cg_Dcl_eng_Dcl input file and collect the
        particles.
        """
        for line in self._stream_handle:
            particle_class = None

            log.trace("Line: %s", line)
            fields = line.split()
            if len(fields) < 6:
                continue

            l1, l2, l3, l4 = fields[2:6]

            # Identify the particle type
            if l1 == 'MSG':
                if l2 == 'D_STATUS':
                    if l3 == 'STATUS:':  # e.g. 2013/12/20 00:05:39.802 MSG D_STATUS STATUS: ...
                        particle_class = ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS
                    elif l3 == 'CPU':  # 2013/12/20 00:10:40.248 MSG D_STATUS CPU ...
                        particle_class = ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS
                elif l2 == 'D_CTL':
                    continue  # ignore
            elif l1 in ['ERR', 'ALM', 'WNG']:  # 2013/12/20 01:33:45.515 ALM ...
                particle_class = ParticleClassTypes.ERROR_PARTICLE_CLASS
            elif l1 == 'DAT':
                if l2 == 'D_GPS':  # 2013/12/20 01:30:45.503 DAT D_GPS ...
                    particle_class = ParticleClassTypes.GPS_PARTICLE_CLASS
                elif l2 == 'D_PPS':  # 2013/12/20 01:30:45.504 DAT D_PPS D_PPS: ...
                    particle_class = ParticleClassTypes.PPS_PARTICLE_CLASS
                elif l2 == 'SUPERV':  # 2013/12/20 01:20:44.908 DAT SUPERV ...
                    particle_class = ParticleClassTypes.SUPERV_PARTICLE_CLASS
                elif l2 == 'DLOG_MGR':  # 2013/12/20 18:57:10.822 DAT DLOG_MGR ...
                    particle_class = ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS
                elif 'DLOGP' in l2:
                    if l3 == 'istatus:':  # 2014/09/15 00:54:26.910 DAT DLOGP5 istatus: ...
                        particle_class = ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS
                    elif l4 == 'CB_AARM':  # 2014/09/15 22:22:50.917 DAT DLOGP1 3DM CB_AARM ...
                        particle_class = ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS
                elif l3 == 'NTP:':  # 2014/09/15 00:04:20.260 DAT D_STATUS NTP: ...
                    particle_class = ParticleClassTypes.STATUS_PARTICLE_CLASS

            # Extract the particle
            if particle_class:
                regex_match = re.match(self._particle_regex[particle_class], line)
                if not regex_match:
                    log.error('failed to match expected particle regex: %s not in %s',
                              self._particle_regex[particle_class], line)
                    continue
                gdict = regex_match.groupdict()

                try:
                    sample = self._extract_sample(self._particle_classes[particle_class], None, gdict)
                    if sample:
                        self._record_buffer.append(sample)
                    else:
                        log.error('failed to extract sample from line: %r', line)
                except Exception as e:
                    log.exception('exception (%r) extracting sample from line: %r', e, line)

            else:
                log.debug("Non-match .. ignoring line: %r", line)

        # Set an indication that the file was fully parsed
        self._file_parsed = True
