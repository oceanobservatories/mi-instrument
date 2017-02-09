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

from mi.core.log import get_logger
log = get_logger()
from mi.core.exceptions import ConfigurationException, UnexpectedDataException
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, INT_REGEX, \
    TIME_HR_MIN_SEC_REGEX, FLOAT_REGEX, ASCII_HEX_CHAR_REGEX

from mi.dataset.parser import utilities


class CgDclEngDclParticleClassTypes(BaseEnum):
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


class CgDclEngDclDataParticleType(BaseEnum):
    """
    Enum for the cg_dcl_eng_dcl particle stream types.
    """

    CG_DCL_ENG_DCL_MSG_COUNTS_TELEMETERED = 'cg_dcl_eng_dcl_msg_counts'
    CG_DCL_ENG_DCL_MSG_COUNTS_RECOVERED = 'cg_dcl_eng_dcl_msg_counts_recovered'
    CG_DCL_ENG_DCL_CPU_UPTIME_TELEMETERED = 'cg_dcl_eng_dcl_cpu_uptime'
    CG_DCL_ENG_DCL_CPU_UPTIME_RECOVERED = 'cg_dcl_eng_dcl_cpu_uptime_recovered'
    CG_DCL_ENG_DCL_ERROR_TELEMETERED = 'cg_dcl_eng_dcl_error'
    CG_DCL_ENG_DCL_ERROR_RECOVERED = 'cg_dcl_eng_dcl_error_recovered'
    CG_DCL_ENG_DCL_GPS_TELEMETERED = 'cg_dcl_eng_dcl_gps'
    CG_DCL_ENG_DCL_GPS_RECOVERED = 'cg_dcl_eng_dcl_gps_recovered'
    CG_DCL_ENG_DCL_PPS_TELEMETERED = 'cg_dcl_eng_dcl_pps'
    CG_DCL_ENG_DCL_PPS_RECOVERED = 'cg_dcl_eng_dcl_pps_recovered'
    CG_DCL_ENG_DCL_SUPERV_TELEMETERED = 'cg_dcl_eng_dcl_superv'
    CG_DCL_ENG_DCL_SUPERV_RECOVERED = 'cg_dcl_eng_dcl_superv_recovered'
    CG_DCL_ENG_DCL_DLOG_MGR_TELEMETERED = 'cg_dcl_eng_dcl_dlog_mgr'
    CG_DCL_ENG_DCL_DLOG_MGR_RECOVERED = 'cg_dcl_eng_dcl_dlog_mgr_recovered'
    CG_DCL_ENG_DCL_DLOG_STATUS_TELEMETERED = 'cg_dcl_eng_dcl_dlog_status'
    CG_DCL_ENG_DCL_DLOG_STATUS_RECOVERED = 'cg_dcl_eng_dcl_dlog_status_recovered'
    CG_DCL_ENG_DCL_STATUS_TELEMETERED = 'cg_dcl_eng_dcl_status'
    CG_DCL_ENG_DCL_STATUS_RECOVERED = 'cg_dcl_eng_dcl_status_recovered'
    CG_DCL_ENG_DCL_DLOG_AARM_TELEMETERED = 'cg_dcl_eng_dcl_dlog_aarm'
    CG_DCL_ENG_DCL_DLOG_AARM_RECOVERED = 'cg_dcl_eng_dcl_dlog_aarm_recovered'


class CgDclEngDclParserDataParticleKey(BaseEnum):
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


"""
Encoding rules used in _build_parsed_value methods used build the particles.
"""
ENCODING_RULES_DICT = {
    CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP: str,
    CgDclEngDclParserDataParticleKey.GPS_COUNTS: int,
    CgDclEngDclParserDataParticleKey.NTP_COUNTS: int,
    CgDclEngDclParserDataParticleKey.PPS_COUNTS: int,
    CgDclEngDclParserDataParticleKey.SUPERV_COUNTS: int,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_COUNTS: int,
    CgDclEngDclParserDataParticleKey.UPTIME_STRING: str,
    CgDclEngDclParserDataParticleKey.LOAD_VAL_1: float,
    CgDclEngDclParserDataParticleKey.LOAD_VAL_2: float,
    CgDclEngDclParserDataParticleKey.LOAD_VAL_3: float,
    CgDclEngDclParserDataParticleKey.MEM_FREE: int,
    CgDclEngDclParserDataParticleKey.NUM_PROCESSES: int,
    CgDclEngDclParserDataParticleKey.LOG_TYPE: str,
    CgDclEngDclParserDataParticleKey.MESSAGE_TEXT: str,
    CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TIMESTAMP: str,
    CgDclEngDclParserDataParticleKey.LATITUDE: float,
    CgDclEngDclParserDataParticleKey.LONGITUDE: float,
    CgDclEngDclParserDataParticleKey.GPS_SPEED: float,
    CgDclEngDclParserDataParticleKey.GPS_TRUE_COURSE: float,
    CgDclEngDclParserDataParticleKey.GPS_QUALITY: int,
    CgDclEngDclParserDataParticleKey.GPS_NUM_SATELLITES: int,
    CgDclEngDclParserDataParticleKey.GPS_HDOP: float,
    CgDclEngDclParserDataParticleKey.GPS_ALTITUDE: float,
    CgDclEngDclParserDataParticleKey.DATE_OF_FIX: str,
    CgDclEngDclParserDataParticleKey.TIME_OF_FIX: str,
    CgDclEngDclParserDataParticleKey.LATITUDE_ALT_FORMAT: str,
    CgDclEngDclParserDataParticleKey.LONGITUDE_ALT_FORMAT: str,
    CgDclEngDclParserDataParticleKey.NMEA_LOCK: int,
    CgDclEngDclParserDataParticleKey.DELTA: int,
    CgDclEngDclParserDataParticleKey.DELTA_MIN: int,
    CgDclEngDclParserDataParticleKey.DELTA_MAX: int,
    CgDclEngDclParserDataParticleKey.BAD_PULSES: int,
    CgDclEngDclParserDataParticleKey.BOARD_TYPE: str,
    CgDclEngDclParserDataParticleKey.VMAIN_BACKPLANE_BUS_VOLTAGE: float,
    CgDclEngDclParserDataParticleKey.IMAIN_CURRENT: float,
    CgDclEngDclParserDataParticleKey.ERROR_VMAIN_OUT_TOLERANCE: int,
    CgDclEngDclParserDataParticleKey.ERROR_IMAIN_OUT_TOLERANCE: int,
    CgDclEngDclParserDataParticleKey.ERROR_DCL_ISO_SWGF_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_DCL_RTN_SWFG_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_VMAIN_SWGF_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_GMAIN_SWGF_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_SENSOR_ISO_SWGF_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_SNSR_COM_SWGF_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_LEAK_DETECT_C1_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_LEAK_DETECT_C2_LIM_EXCEEDED: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_OVERCURRENT_FAULT: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_1_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_2_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_3_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_4_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_5_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_6_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_7_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_8_NOT_RESPONDING: int,
    CgDclEngDclParserDataParticleKey.ERROR_I2C_ERROR: int,
    CgDclEngDclParserDataParticleKey.ERROR_UART_ERROR: int,
    CgDclEngDclParserDataParticleKey.ERROR_BROWN_OUT_RESET: int,
    CgDclEngDclParserDataParticleKey.BMP085_TEMP: float,
    CgDclEngDclParserDataParticleKey.SHT25_TEMP: float,
    CgDclEngDclParserDataParticleKey.MURATA_12V_TEMP: float,
    CgDclEngDclParserDataParticleKey.MURATA_24V_TEMP: float,
    CgDclEngDclParserDataParticleKey.VICOR_12V_BCM_TEMP: float,
    CgDclEngDclParserDataParticleKey.SHT25_HUMIDITY: float,
    CgDclEngDclParserDataParticleKey.BMP085_PRESSURE: float,
    CgDclEngDclParserDataParticleKey.ACTIVE_SWGF_CHANNELS: int,
    CgDclEngDclParserDataParticleKey.SWGF_C1_MAX_LEAKAGE: float,
    CgDclEngDclParserDataParticleKey.SWGF_C2_MAX_LEAKAGE: float,
    CgDclEngDclParserDataParticleKey.SWGF_C3_MAX_LEAKAGE: float,
    CgDclEngDclParserDataParticleKey.ACTIVE_LEAK_DETECT_CHANNELS: int,
    CgDclEngDclParserDataParticleKey.LEAK_DETECT_C1_V: int,
    CgDclEngDclParserDataParticleKey.LEAK_DETECT_C2_V: int,
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
    CgDclEngDclParserDataParticleKey.PWR_BOARD_MODE: int,
    CgDclEngDclParserDataParticleKey.DPB_MODE: int,
    CgDclEngDclParserDataParticleKey.DPB_VOLTAGE_MODE: int,
    CgDclEngDclParserDataParticleKey.VMAIN_DPB_IN: float,
    CgDclEngDclParserDataParticleKey.IMAIN_DPB_IN: float,
    CgDclEngDclParserDataParticleKey.OUT_12V_V: float,
    CgDclEngDclParserDataParticleKey.OUT_12V_I: float,
    CgDclEngDclParserDataParticleKey.OUT_24V_V: float,
    CgDclEngDclParserDataParticleKey.OUT_24V_I: float,
    CgDclEngDclParserDataParticleKey.DATALOGGER_TIMESTAMP: str,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_ACT: int,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_STR: int,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_HLT: int,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_FLD: int,
    CgDclEngDclParserDataParticleKey.DLOG_MGR_MAP: str,
    CgDclEngDclParserDataParticleKey.INSTRUMENT_IDENTIFIER: str,
    CgDclEngDclParserDataParticleKey.DATALOGGER_STATE: str,
    CgDclEngDclParserDataParticleKey.BYTES_SENT: int,
    CgDclEngDclParserDataParticleKey.BYTES_RECEIVED: int,
    CgDclEngDclParserDataParticleKey.BYTES_LOGGED: int,
    CgDclEngDclParserDataParticleKey.GOOD_RECORDS: int,
    CgDclEngDclParserDataParticleKey.BAD_RECORDS: int,
    CgDclEngDclParserDataParticleKey.BAD_BYTES: int,
    CgDclEngDclParserDataParticleKey.TIME_RECEIVED_LAST_DATA: int,
    CgDclEngDclParserDataParticleKey.TIME_LAST_COMMUNICATED: int,
    CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TYPE: str,
    CgDclEngDclParserDataParticleKey.SYNC_TYPE: str,
    CgDclEngDclParserDataParticleKey.NTP_OFFSET: float,
    CgDclEngDclParserDataParticleKey.NTP_JITTER: float,
    CgDclEngDclParserDataParticleKey.ACCELERATION_X: float,
    CgDclEngDclParserDataParticleKey.ACCELERATION_Y: float,
    CgDclEngDclParserDataParticleKey.ACCELERATION_Z: float,
    CgDclEngDclParserDataParticleKey.ANGULAR_RATE_X: float,
    CgDclEngDclParserDataParticleKey.ANGULAR_RATE_Y: float,
    CgDclEngDclParserDataParticleKey.ANGULAR_RATE_Z: float,
    CgDclEngDclParserDataParticleKey.MAGNETOMETER_X: float,
    CgDclEngDclParserDataParticleKey.MAGNETOMETER_Y: float,
    CgDclEngDclParserDataParticleKey.MAGNETOMETER_Z: float,
    CgDclEngDclParserDataParticleKey.TIC_COUNTER: float,
}

# A base regex used to match all cg_dcl_eng_dcl MSG records
MSG_LOG_TYPE_BASE_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                          DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                          ')\s+MSG'

# A regex used to match various MSG type records
MSG_LOG_TYPE_REGEX = MSG_LOG_TYPE_BASE_REGEX + '\s+D_STATUS\s+(STATUS|CPU\s+Uptime):'
ERR_ALM_WNG_LOG_TYPE_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                             DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                             ')\s+(?P<' + CgDclEngDclParserDataParticleKey.LOG_TYPE + \
                             '>(?:ERR|ALM|WNG))'

# A base regex used to DAT records
DAT_LOG_TYPE_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+DAT'


# Regex for particles CG_DCL_ENG_DCL_MSG_COUNTS_TELEMETERED and CG_DCL_ENG_DCL_MSG_COUNTS_RECOVERED
# Example file record:
# 2013/12/20 00:05:39.802 MSG D_STATUS STATUS: GPS=600, NTP=0, PPS=30, SUPERV=147, DLOG_MGR=9
MSG_COUNTS_REGEX = MSG_LOG_TYPE_BASE_REGEX + \
                   '\s+D_STATUS\s+STATUS:\s+GPS=(?P<' + \
                   CgDclEngDclParserDataParticleKey.GPS_COUNTS + \
                   '>' + INT_REGEX + '),\s+NTP=(?P<' + \
                   CgDclEngDclParserDataParticleKey.NTP_COUNTS + \
                   '>' + INT_REGEX + '),\s+PPS=(?P<' + \
                   CgDclEngDclParserDataParticleKey.PPS_COUNTS + \
                   '>' + INT_REGEX + '),\s+SUPERV=(?P<' + \
                   CgDclEngDclParserDataParticleKey.SUPERV_COUNTS + \
                   '>' + INT_REGEX + '),\s+DLOG_MGR=(?P<' + \
                   CgDclEngDclParserDataParticleKey.DLOG_MGR_COUNTS + \
                   '>' + INT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_CPU_UPTIME_TELEMETERED and CG_DCL_ENG_DCL_CPU_UPTIME_RECOVERED
# Example file record:
# 2013/12/20 00:10:40.248 MSG D_STATUS CPU Uptime:  28 days 09:05:18  Load: 0.02 0.23 0.23  Free: 23056k  Nproc: 81
CPU_UPTIME_REGEX = MSG_LOG_TYPE_BASE_REGEX + \
                   '\s+D_STATUS\s+CPU\s+Uptime:\s+(?P<' + \
                   CgDclEngDclParserDataParticleKey.UPTIME_STRING + \
                   '>' + INT_REGEX + '\s+days\s+' + TIME_HR_MIN_SEC_REGEX + \
                   ')\s+Load:\s+(?P<' + CgDclEngDclParserDataParticleKey.LOAD_VAL_1 + \
                   '>' + FLOAT_REGEX + ')\s+(?P<' + \
                   CgDclEngDclParserDataParticleKey.LOAD_VAL_2 + \
                   '>' + FLOAT_REGEX + ')\s+(?P<' + \
                   CgDclEngDclParserDataParticleKey.LOAD_VAL_3 + \
                   '>' + FLOAT_REGEX + ')\s+Free:\s+(?P<' + \
                   CgDclEngDclParserDataParticleKey.MEM_FREE + \
                   '>' + INT_REGEX + ')k\s+Nproc:\s+(?P<' + \
                   CgDclEngDclParserDataParticleKey.NUM_PROCESSES + \
                   '>' + INT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_ERROR_TELEMETERED and CG_DCL_ENG_DCL_ERROR_RECOVERED
# Example file records:
# 2013/12/20 00:16:28.642 ERR DLOGP7 Command [(NULL)] Received no recognized response
# 2013/12/20 01:33:45.515 ALM D_GPS Warning 4 3, BAD GPS CHECKSUM
ERROR_REGEX = ERR_ALM_WNG_LOG_TYPE_REGEX +  \
              '\s+(?P<' + CgDclEngDclParserDataParticleKey.MESSAGE_TEXT + \
              '>.*)' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_GPS_TELEMETERED and CG_DCL_ENG_DCL_GPS_RECOVERED
# Example file record:
# 2013/12/20 01:30:45.503 DAT D_GPS 2013/12/20 01:30:44.848 GPS 40.136760 -70.769495 3.40 070.90 2 9 0.80 0.80 201213
# 013044 4008.2056 N 07046.1697 W
GPS_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
            DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
            ')\s+DAT\s+D_GPS\s+(?P<' + CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TIMESTAMP + \
            '>' + DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
            ')\s+GPS\s+(?P<' + CgDclEngDclParserDataParticleKey.LATITUDE + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.LONGITUDE + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_SPEED + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_TRUE_COURSE + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_QUALITY + \
            '>' + INT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_NUM_SATELLITES + \
            '>' + INT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_HDOP + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.GPS_ALTITUDE + \
            '>' + FLOAT_REGEX + ')\s+(?P<' + CgDclEngDclParserDataParticleKey.DATE_OF_FIX + \
            '>\d{6})\s+(?P<' + CgDclEngDclParserDataParticleKey.TIME_OF_FIX + \
            '>\d{6})\s+(?P<' + CgDclEngDclParserDataParticleKey.LATITUDE_ALT_FORMAT + \
            '>' + FLOAT_REGEX + '\s(?:N|S|W|E))\s+(?P<' + \
            CgDclEngDclParserDataParticleKey.LONGITUDE_ALT_FORMAT + \
            '>' + FLOAT_REGEX + '\s(?:N|S|W|E))' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_PPS_TELEMETERED and CG_DCL_ENG_DCL_PPS_RECOVERED
# Example file record:
# 2013/12/20 01:30:45.504 DAT D_PPS D_PPS: NMEA_Lock: TRUE  Delta: 0999996 DeltaMin: +0000000 DeltaMax: -0000023
# BadPulses: 0000 TS: 2013/12/20 01:30:42.000
PPS_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
            DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
            ')\s+DAT\s+D_PPS\s+D_PPS:\s+NMEA_Lock:\s+(?P<' + \
            CgDclEngDclParserDataParticleKey.NMEA_LOCK + \
            '>(?:TRUE|FALSE|True|False|true|false))\s+Delta:\s+(?P<' + \
            CgDclEngDclParserDataParticleKey.DELTA + '>' + INT_REGEX + \
            ')\s+DeltaMin:\s+(?P<' + CgDclEngDclParserDataParticleKey.DELTA_MIN + \
            '>' + INT_REGEX + ')\s+DeltaMax:\s+(?P<' + \
            CgDclEngDclParserDataParticleKey.DELTA_MAX + '>' + INT_REGEX + \
            ')\s+BadPulses:\s+(?P<' + CgDclEngDclParserDataParticleKey.BAD_PULSES + \
            '>' + INT_REGEX + ')\s+TS:\s+(?P<' + \
            CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
            DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + ')' + \
            END_OF_LINE_REGEX

ERROR_BITS_PARAM = 'error_bits'

# Regex for particles CG_DCL_ENG_DCL_SUPERV_TELEMETERED and CG_DCL_ENG_DCL_SUPERV_RECOVERED
# Example file record:
# 2013/12/20 01:20:44.908 DAT SUPERV dcl: 24.0 412.0 00001070 t 22.2 19.9 25.3 25.3 45.9 h 23.8 p 14.5 gf 7 -5.2
# -531.9 -595.2 ld 3 1250 1233 p1 0 0.0 0.0 0 p2 0 0.0 0.0 0 p3 0 0.0 0.0 0 p4 1 11.9 4.9 0 p5 0 0.0 0.0 0 p6 1
# 11.9 224.8 0 p7 0 0.0 0.0 0 p8 1 12.0 39.1 1 hb 0 0 0 wake 0 wtc 0 wpc 0 pwr 2 2 3 23.9 400.1 12.0 280.1 0.0 0.0 3b93
SUPERV_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
               DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
               ')\s+DAT\s+SUPERV\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.BOARD_TYPE + \
               '>.*):\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.VMAIN_BACKPLANE_BUS_VOLTAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.IMAIN_CURRENT + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               ERROR_BITS_PARAM + \
               '>' + ASCII_HEX_CHAR_REGEX + '{8})\s+t\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.BMP085_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.SHT25_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.MURATA_12V_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.MURATA_24V_TEMP + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.VICOR_12V_BCM_TEMP + \
               '>' + FLOAT_REGEX + ')\s+h\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.SHT25_HUMIDITY + \
               '>' + FLOAT_REGEX + ')\s+p\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.BMP085_PRESSURE + \
               '>' + FLOAT_REGEX + ')\s+gf\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.ACTIVE_SWGF_CHANNELS + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.SWGF_C1_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.SWGF_C2_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.SWGF_C3_MAX_LEAKAGE + \
               '>' + FLOAT_REGEX + ')\s+ld\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.ACTIVE_LEAK_DETECT_CHANNELS + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.LEAK_DETECT_C1_V + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.LEAK_DETECT_C2_V + \
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
               CgDclEngDclParserDataParticleKey.PWR_BOARD_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.DPB_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.DPB_VOLTAGE_MODE + \
               '>' + INT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.VMAIN_DPB_IN + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.IMAIN_DPB_IN + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.OUT_12V_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.OUT_12V_I + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.OUT_24V_V + \
               '>' + FLOAT_REGEX + ')\s+(?P<' + \
               CgDclEngDclParserDataParticleKey.OUT_24V_I + \
               '>' + FLOAT_REGEX + ')\s+.*' + \
               END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_DLOG_MGR_TELEMETERED and CG_DCL_ENG_DCL_DLOG_MGR_RECOVERED
# Example file record:
# 2013/12/20 18:57:10.822 DAT DLOG_MGR dmgrstatus: 2013/12/20 18:56:41.177 act:4 str:4 hlt:0 fld:0 map:000D0DDD
DLOG_MGR_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                 DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                 ')\s+DAT\s+DLOG_MGR\s+dmgrstatus:\s+(?P<' + \
                 CgDclEngDclParserDataParticleKey.DATALOGGER_TIMESTAMP + '>' + \
                 DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                 ')\s+act:(?P<' + CgDclEngDclParserDataParticleKey.DLOG_MGR_ACT + \
                 '>\d)\s+str:(?P<' + CgDclEngDclParserDataParticleKey.DLOG_MGR_STR + \
                 '>\d)\s+hlt:(?P<' + CgDclEngDclParserDataParticleKey.DLOG_MGR_HLT + \
                 '>\d)\s+fld:(?P<' + CgDclEngDclParserDataParticleKey.DLOG_MGR_FLD + \
                 '>\d)\s+map:(?P<' + CgDclEngDclParserDataParticleKey.DLOG_MGR_MAP + \
                 '>.*)' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_DLOG_STATUS_TELEMETERED and CG_DCL_ENG_DCL_DLOG_STATUS_RECOVERED
# Example file record:
# 2013/12/20 18:57:10.822 DAT DLOG_MGR dmgrstatus: 2013/12/20 18:56:41.177 act:4 str:4 hlt:0 fld:0 map:000D0DDD
DLOG_STATUS_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                    DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                    ')\s+DAT\s+DLOGP\d\s+istatus:\s+(?P<' + \
                    CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
                    DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                    ')\s+(?P<' + CgDclEngDclParserDataParticleKey.INSTRUMENT_IDENTIFIER + \
                    '>\w+)\s+(?P<' + CgDclEngDclParserDataParticleKey.DATALOGGER_STATE + \
                    '>\w+)\s+tx:\s+(?P<' + CgDclEngDclParserDataParticleKey.BYTES_SENT + \
                    '>\d+)\s+rx:\s+(?P<' + CgDclEngDclParserDataParticleKey.BYTES_RECEIVED + \
                    '>\d+)\s+log:\s+(?P<' + CgDclEngDclParserDataParticleKey.BYTES_LOGGED + \
                    '>\d+)\s+good:\s+(?P<' + CgDclEngDclParserDataParticleKey.GOOD_RECORDS + \
                    '>\d+)\s+bad:\s+(?P<' + CgDclEngDclParserDataParticleKey.BAD_RECORDS + \
                    '>\d+)\s+bb:\s+(?P<' + CgDclEngDclParserDataParticleKey.BAD_BYTES + \
                    '>\d+)\s+ld:\s+(?P<' + CgDclEngDclParserDataParticleKey.TIME_RECEIVED_LAST_DATA + \
                    '>\d+)(\s+lc:\s+(?P<' + CgDclEngDclParserDataParticleKey.TIME_LAST_COMMUNICATED + \
                    '>\d+))?' + END_OF_LINE_REGEX

ERROR_BIT_NOT_USED = 'ERROR_BIT_NOT_USED'

ERROR_BITS = [
    CgDclEngDclParserDataParticleKey.ERROR_VMAIN_OUT_TOLERANCE,
    CgDclEngDclParserDataParticleKey.ERROR_IMAIN_OUT_TOLERANCE,
    CgDclEngDclParserDataParticleKey.ERROR_DCL_ISO_SWGF_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_DCL_RTN_SWFG_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_VMAIN_SWGF_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_GMAIN_SWGF_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_SENSOR_ISO_SWGF_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_SNSR_COM_SWGF_LIM_EXCEEDED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    CgDclEngDclParserDataParticleKey.ERROR_LEAK_DETECT_C1_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_LEAK_DETECT_C2_LIM_EXCEEDED,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_OVERCURRENT_FAULT,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_1_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_2_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_3_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_4_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_5_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_6_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_7_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_CHANNEL_8_NOT_RESPONDING,
    CgDclEngDclParserDataParticleKey.ERROR_I2C_ERROR,
    CgDclEngDclParserDataParticleKey.ERROR_UART_ERROR,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    ERROR_BIT_NOT_USED,
    CgDclEngDclParserDataParticleKey.ERROR_BROWN_OUT_RESET,
]

# Regex for particles CG_DCL_ENG_DCL_STATUS_TELEMETERED and CG_DCL_ENG_DCL_STATUS_RECOVERED
# Example file record:
# 2014/09/15 00:04:20.260 DAT D_STATUS NTP: 2014/09/15 00:04:20.031 *SHM(1) .PPS. -0.202 0.067
D_STATUS_NTP_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+DAT\s+D_STATUS\s+(?P<' + \
                     CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TYPE + \
                     '>\w+):\s+(?P<' + \
                     CgDclEngDclParserDataParticleKey.MESSAGE_SENT_TIMESTAMP + '>' + \
                     DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                     ')\s+(?P<' + CgDclEngDclParserDataParticleKey.SYNC_TYPE + '>' + \
                     '.*)\s+((?P<' + CgDclEngDclParserDataParticleKey.NTP_OFFSET + \
                     '>' + FLOAT_REGEX + ')\s+(?P<' + \
                     CgDclEngDclParserDataParticleKey.NTP_JITTER + '>' + FLOAT_REGEX + \
                     '))?' + END_OF_LINE_REGEX

# Regex for particles CG_DCL_ENG_DCL_SEA_STATE_TELEMETERED and CG_DCL_ENG_DCL_SEA_STATE_RECOVERED
# Example file record:
# 2014/09/15 22:22:50.917 DAT DLOGP1 3DM CB_AARM ax: -0.089718 ay: -0.073573 az: -0.864567 rx: -0.126140 ry: 0.090486 rz: 0.114195 mx: 0.062305 my: -0.140416 mz: 0.498844 t: 1194.34
DLOG_AARM_REGEX = r'(?P<' + CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP + '>' + \
                  DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + \
                  ')\s+DAT\s+DLOGP\d\s+3DM\s+CB_AARM\s+ax:\s+(?P<' + \
                  CgDclEngDclParserDataParticleKey.ACCELERATION_X + '>' + FLOAT_REGEX + \
                  ')\s+ay:\s+(?P<' + CgDclEngDclParserDataParticleKey.ACCELERATION_Y + \
                  '>' + FLOAT_REGEX + ')\s+az:\s+(?P<' + CgDclEngDclParserDataParticleKey.ACCELERATION_Z + \
                  '>' + FLOAT_REGEX + ')\s+rx:\s+(?P<' + CgDclEngDclParserDataParticleKey.ANGULAR_RATE_X + \
                  '>' + FLOAT_REGEX + ')\s+ry:\s+(?P<' + CgDclEngDclParserDataParticleKey.ANGULAR_RATE_Y + \
                  '>' + FLOAT_REGEX + ')\s+rz:\s+(?P<' + CgDclEngDclParserDataParticleKey.ANGULAR_RATE_Z + \
                  '>' + FLOAT_REGEX + ')\s+mx:\s+(?P<' + CgDclEngDclParserDataParticleKey.MAGNETOMETER_X + \
                  '>' + FLOAT_REGEX + ')\s+my:\s+(?P<' + CgDclEngDclParserDataParticleKey.MAGNETOMETER_Y + \
                  '>' + FLOAT_REGEX + ')\s+mz:\s+(?P<' + CgDclEngDclParserDataParticleKey.MAGNETOMETER_Z + \
                  '>' + FLOAT_REGEX + ')\s+t:\s+(?P<' + \
                  CgDclEngDclParserDataParticleKey.TIC_COUNTER + '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Regex for records to ignore
IGNORE_REGEX = r'(' + END_OF_LINE_REGEX + '|' + DATE_YYYY_MM_DD_REGEX + '\s+' + \
               TIME_HR_MIN_SEC_MSEC_REGEX + '\s+MSG\s+D_CTL\s+.*' + END_OF_LINE_REGEX + '?)'


class CgDclEngDclDataParticle(DataParticle):
    """
    Base data particle for cg_dcl_eng_dcl.
    """

    def _build_parsed_values(self):
        """
        This is the default implementation which many cg_dcl_eng_dcl particles can
        take advantage of.
        """
        result = []

        for key in self.raw_data.keys():

            if key == CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP:

                self.set_internal_timestamp(
                    utilities.dcl_controller_timestamp_to_ntp_time(self.raw_data[key]))

            result.append(self._encode_value(key, self.raw_data[key], ENCODING_RULES_DICT[key]))

        return result


class CgDclEngDclMsgCountsRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_MSG_COUNTS_RECOVERED


class CgDclEngDclMsgCountsTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_MSG_COUNTS_TELEMETERED


class CgDclEngDclCpuUptimeRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_CPU_UPTIME_RECOVERED


class CgDclEngDclCpuUptimeTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_CPU_UPTIME_TELEMETERED


class CgDclEngDclErrorRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_ERROR_RECOVERED


class CgDclEngDclErrorTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_ERROR_TELEMETERED


class CgDclEngDclGpsRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_GPS_RECOVERED


class CgDclEngDclGpsTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_GPS_TELEMETERED


class CgDclEngDclPpsDataParticle(CgDclEngDclDataParticle):
    """
    Class for building a CgDclEngDclPpsDataParticle
    """

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        """
        result = []

        for key in self.raw_data.keys():

            if key == CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP:

                self.set_internal_timestamp(
                    utilities.dcl_controller_timestamp_to_ntp_time(self.raw_data[key]))

            if key == CgDclEngDclParserDataParticleKey.NMEA_LOCK:

                if self.raw_data[key] in ['TRUE', 'True', 'true']:
                    value = 1
                else:
                    value = 0

            else:

                value = self.raw_data[key]

            result.append(self._encode_value(key, value, ENCODING_RULES_DICT[key]))

        return result


class CgDclEngDclPpsRecoveredDataParticle(CgDclEngDclPpsDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_PPS_RECOVERED


class CgDclEngDclPpsTelemeteredDataParticle(CgDclEngDclPpsDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_PPS_TELEMETERED


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

            if key == CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP:

                self.set_internal_timestamp(
                    utilities.dcl_controller_timestamp_to_ntp_time(self.raw_data[key]))

            if key == ERROR_BITS_PARAM:

                error_bits  = format(int(self.raw_data[key], 16), '016b')

                for bit_field, bit in zip(ERROR_BITS, reversed(error_bits)):

                    if bit_field != ERROR_BIT_NOT_USED:
                        result.append(self._encode_value(bit_field, bit, ENCODING_RULES_DICT[bit_field]))

            elif re.match(CHANNEL_STATE_REGEX, key):

                channel_states.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_V_REGEX, key):

                channel_vs.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_I_REGEX, key):

                channel_is.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            elif re.match(CHANNEL_ERROR_STATUS_REGEX, key):

                channel_error_statuses.append(ENCODING_RULES_DICT[key](self.raw_data[key]))
            else:

                result.append(self._encode_value(key, self.raw_data[key], ENCODING_RULES_DICT[key]))

        result.append(self._encode_value(CgDclEngDclParserDataParticleKey.CHANNEL_STATE,
                                         channel_states, list))
        result.append(self._encode_value(CgDclEngDclParserDataParticleKey.CHANNEL_V,
                                         channel_vs, list))
        result.append(self._encode_value(CgDclEngDclParserDataParticleKey.CHANNEL_I,
                                         channel_is, list))
        result.append(self._encode_value(CgDclEngDclParserDataParticleKey.CHANNEL_ERROR_STATUS,
                                         channel_error_statuses, list))

        return result


class CgDclEngDclSupervRecoveredDataParticle(CgDclEngDclSupervDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_SUPERV_RECOVERED


class CgDclEngDclSupervTelemeteredDataParticle(CgDclEngDclSupervDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_SUPERV_TELEMETERED


class CgDclEngDclDlogMgrRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_MGR_RECOVERED


class CgDclEngDclDlogMgrTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_MGR_TELEMETERED


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

            if key == CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP:

                self.set_internal_timestamp(
                    utilities.dcl_controller_timestamp_to_ntp_time(self.raw_data[key]))

            if key == CgDclEngDclParserDataParticleKey.TIME_LAST_COMMUNICATED and \
                            self.raw_data[key] is None:

                result.append({
                    DataParticleKey.VALUE_ID: key,
                    DataParticleKey.VALUE: None
                })

            else:

                result.append(self._encode_value(key, self.raw_data[key], ENCODING_RULES_DICT[key]))

        return result


class CgDclEngDclDlogStatusRecoveredDataParticle(CgDclEngDclDlogStatusDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_STATUS_RECOVERED


class CgDclEngDclDlogStatusTelemeteredDataParticle(CgDclEngDclDlogStatusDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_STATUS_TELEMETERED


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

            if key == CgDclEngDclParserDataParticleKey.HEADER_TIMESTAMP:

                self.set_internal_timestamp(
                    utilities.dcl_controller_timestamp_to_ntp_time(self.raw_data[key]))

            if key in [CgDclEngDclParserDataParticleKey.NTP_OFFSET, CgDclEngDclParserDataParticleKey.NTP_JITTER] and \
                            self.raw_data[key] is None:

                result.append({
                    DataParticleKey.VALUE_ID: key,
                    DataParticleKey.VALUE: None
                })

            else:

                result.append(self._encode_value(key, self.raw_data[key], ENCODING_RULES_DICT[key]))

        return result


class CgDclEngDclStatusRecoveredDataParticle(CgDclEngDclStatusDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_STATUS_RECOVERED


class CgDclEngDclStatusTelemeteredDataParticle(CgDclEngDclStatusDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_STATUS_TELEMETERED


class CgDclEngDclDlogAarmRecoveredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_AARM_RECOVERED


class CgDclEngDclDlogAarmTelemeteredDataParticle(CgDclEngDclDataParticle):

    _data_particle_type = CgDclEngDclDataParticleType.CG_DCL_ENG_DCL_DLOG_AARM_TELEMETERED


class CgDclEngDclParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        # no sieve function since we are not using the chunker here
        super(CgDclEngDclParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback)


        try:
            particle_classes_dict = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT]
            self._msg_counts_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS]
            self._cpu_uptime_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS]
            self._error_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.ERROR_PARTICLE_CLASS]
            self._gps_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.GPS_PARTICLE_CLASS]
            self._pps_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.PPS_PARTICLE_CLASS]
            self._superv_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.SUPERV_PARTICLE_CLASS]
            self._dlog_mgr_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.DLOG_MGR_PARTICLE_CLASS]
            self._dlog_status_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS]
            self._status_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.STATUS_PARTICLE_CLASS]
            self._dlog_aarm_particle_class = particle_classes_dict[
                CgDclEngDclParticleClassTypes.DLOG_AARM_PARTICLE_CLASS]

        except (KeyError, AttributeError):
            message = "Invalid cg_dcl_eng_dcl configuration parameters."
            log.error("Error: %s", message)
            raise ConfigurationException(message)

    def parse_file(self):
        """
        This method will parse a cg_Dcl_eng_Dcl input file and collect the
        particles.
        """

        # Read the first line in the file
        line = self._stream_handle.readline()

        # While a new line in the file exists
        while line:

            sample = None

            log.trace("Line: %s", line)

            # Let's first see if we have a MSG log message
            if re.match(MSG_LOG_TYPE_REGEX, line):

                msg_counts_match = re.match(MSG_COUNTS_REGEX, line)
                cpu_uptime_match = re.match(CPU_UPTIME_REGEX, line)

                if msg_counts_match is not None:
                    log.trace("msg_counts_match: %s", msg_counts_match.groupdict())

                    sample = self._extract_sample(self._msg_counts_particle_class,
                                                  None,
                                                  msg_counts_match.groupdict(),
                                                  None)

                elif cpu_uptime_match is not None:
                    log.trace("cpu_uptime_match: %s", cpu_uptime_match.groupdict())

                    sample = self._extract_sample(self._cpu_uptime_particle_class,
                                                  None,
                                                  cpu_uptime_match.groupdict(),
                                                  None)

                else:
                    message = "Invalid MSG Log record, Line: " + line
                    log.error(message)
                    self._exception_callback(UnexpectedDataException(message))

            elif re.match(ERR_ALM_WNG_LOG_TYPE_REGEX, line):

                error_match = re.match(ERROR_REGEX, line)

                if error_match is not None:
                    log.trace("error_match: %s", error_match.groupdict())

                    sample = self._extract_sample(self._error_particle_class,
                                                  None,
                                                  error_match.groupdict(),
                                                  None)
                else:
                    message = "Invalid ERR, ALM or WNG Log record, Line: " + line
                    log.error(message)
                    self._exception_callback(UnexpectedDataException(message))

            elif re.match(DAT_LOG_TYPE_REGEX, line):

                gps_match = re.match(GPS_REGEX, line)
                pps_match =  re.match(PPS_REGEX, line)
                superv_match =  re.match(SUPERV_REGEX, line)
                dlog_mgr_match = re.match(DLOG_MGR_REGEX, line)
                dlog_status_match = re.match(DLOG_STATUS_REGEX, line)
                d_status_ntp_match = re.match(D_STATUS_NTP_REGEX, line)
                dlog_aarm_match = re.match(DLOG_AARM_REGEX, line)

                if gps_match is not None:
                    log.trace("gps_match: %s", gps_match.groupdict())

                    sample = self._extract_sample(self._gps_particle_class,
                                                  None,
                                                  gps_match.groupdict(),
                                                  None)

                elif pps_match is not None:
                    log.trace("pps_match: %s", pps_match.groupdict())

                    sample = self._extract_sample(self._pps_particle_class,
                                                  None,
                                                  pps_match.groupdict(),
                                                  None)

                elif superv_match is not None:
                    log.trace("superv_match: %s", superv_match.groupdict())

                    sample = self._extract_sample(self._superv_particle_class,
                                                  None,
                                                  superv_match.groupdict(),
                                                  None)

                elif dlog_mgr_match is not None:
                    log.trace("dlog_mgr_match: %s", dlog_mgr_match.groupdict())

                    sample = self._extract_sample(self._dlog_mgr_particle_class,
                                                  None,
                                                  dlog_mgr_match.groupdict(),
                                                  None)

                elif dlog_status_match is not None:
                    log.trace("dlog_status_match: %s", dlog_status_match.groupdict())

                    sample = self._extract_sample(self._dlog_status_particle_class,
                                                  None,
                                                  dlog_status_match.groupdict(),
                                                  None)

                elif d_status_ntp_match is not None:
                    log.trace("d_status_ntp_match: %s", d_status_ntp_match.groupdict())

                    sample = self._extract_sample(self._status_particle_class,
                                                  None,
                                                  d_status_ntp_match.groupdict(),
                                                  None)

                elif dlog_aarm_match is not None:
                    log.trace("sea_state_match: %s", dlog_aarm_match.groupdict())

                    sample = self._extract_sample(self._dlog_aarm_particle_class,
                                                  None,
                                                  dlog_aarm_match.groupdict(),
                                                  None)

                else:
                    message = "Invalid DAT Log record, Line: " + line
                    log.error(message)
                    self._exception_callback(UnexpectedDataException(message))

            else:
                log.debug("Non-match .. ignoring line: %r", line)

            if sample:
                self._record_buffer.append(sample)

            # Read the next line in the file
            line = self._stream_handle.readline()

        # Set an indication that the file was fully parsed
        self._file_parsed = True


