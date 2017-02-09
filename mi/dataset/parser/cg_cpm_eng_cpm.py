#!/usr/bin/env python

"""
@package mi.dataset.parser.cg_cpm_eng_cpm
@file mi/dataset/parser/cg_cpm_eng_cpm.py
@author Mark Worden
@brief Parser for the cg_cpm_eng_cpm dataset parser
"""

import copy
import re

from mi.core.log import get_logger
from mi.core.exceptions import SampleException, UnexpectedDataException
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, \
    DataParticleValue
from mi.dataset.dataset_parser import SimpleParser, DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    DATE_YYYY_MM_DD_REGEX, TIME_HR_MIN_SEC_MSEC_REGEX, FLOAT_REGEX, \
    TIME_HR_MIN_SEC_REGEX, INT_REGEX, ASCII_HEX_CHAR_REGEX

log = get_logger()

__author__ = 'mworden'
__license__ = 'Apache 2.0'


class CgCpmEngCpmDataParticleType(BaseEnum):
    CG_CPM_ENG_CPM_TELEMETERED = 'cg_cpm_eng_cpm'
    CG_CPM_ENG_CPM_RECOVERED = 'cg_cpm_eng_cpm_recovered'


class CgCpmEngCpmParserDataParticleKey(BaseEnum):
    CG_ENG_PLATFORM_TIME = 'cg_eng_platform_time'
    CG_ENG_PLATFORM_UTIME = 'cg_eng_platform_utime'
    CG_ENG_ALARM_SYS = 'cg_eng_alarm_sys'
    CG_ENG_ALARM_TS = 'cg_eng_alarm_ts'
    CG_ENG_ALARM_SEVERITY = 'cg_eng_alarm_severity'
    CG_ENG_ALARM_AT = 'cg_eng_alarm_at'
    CG_ENG_ALARM_PC = 'cg_eng_alarm_pc'
    CG_ENG_ALARM_ERR = 'cg_eng_alarm_err'
    CG_ENG_MSG_CNTS_C_GPS = 'cg_eng_msg_cnts_c_gps'
    CG_ENG_MSG_CNTS_C_NTP = 'cg_eng_msg_cnts_c_ntp'
    CG_ENG_MSG_CNTS_C_PPS = 'cg_eng_msg_cnts_c_pps'
    CG_ENG_MSG_CNTS_C_POWER_SYS = 'cg_eng_msg_cnts_c_power_sys'
    CG_ENG_MSG_CNTS_C_SUPERV = 'cg_eng_msg_cnts_c_superv'
    CG_ENG_MSG_CNTS_C_TELEM = 'cg_eng_msg_cnts_c_telem'
    CG_ENG_ERR_C_GPS = 'cg_eng_err_c_gps'
    CG_ENG_ERR_C_PPS = 'cg_eng_err_c_pps'
    CG_ENG_ERR_C_CTL = 'cg_eng_err_c_ctl'
    CG_ENG_ERR_C_STATUS = 'cg_eng_err_c_status'
    CG_ENG_ERR_SUPERV = 'cg_eng_err_superv'
    CG_ENG_ERR_C_POWER_SYS = 'cg_eng_err_c_power_sys'
    CG_ENG_ERR_C_TELEM_SYS = 'cg_eng_err_c_telem_sys'
    CG_ENG_ERR_C_IRID = 'cg_eng_err_c_irid'
    CG_ENG_ERR_C_IMM = 'cg_eng_err_c_imm'
    CG_ENG_ERR_CPM1 = 'cg_eng_err_cpm1'
    CG_ENG_ERR_D_CTL = 'cg_eng_err_d_ctl'
    CG_ENG_ERR_D_STATUS = 'cg_eng_err_d_status'
    CG_ENG_ERR_DLOG_MGR = 'cg_eng_err_dlog_mgr'
    CG_ENG_ERR_DLOGP1 = 'cg_eng_err_dlogp1'
    CG_ENG_ERR_DLOGP2 = 'cg_eng_err_dlogp2'
    CG_ENG_ERR_DLOGP3 = 'cg_eng_err_dlogp3'
    CG_ENG_ERR_DLOGP4 = 'cg_eng_err_dlogp4'
    CG_ENG_ERR_DLOGP5 = 'cg_eng_err_dlogp5'
    CG_ENG_ERR_DLOGP6 = 'cg_eng_err_dlogp6'
    CG_ENG_ERR_DLOGP7 = 'cg_eng_err_dlogp7'
    CG_ENG_ERR_DLOGP8 = 'cg_eng_err_dlogp8'
    CG_ENG_ERR_RCMD = 'cg_eng_err_rcmd'
    CG_ENG_ERR_BCMD = 'cg_eng_err_bcmd'
    CG_ENG_ERRMSG_C_GPS = 'cg_eng_errmsg_c_gps'
    CG_ENG_ERRMSG_C_PPS = 'cg_eng_errmsg_c_pps'
    CG_ENG_ERRMSG_C_CTL = 'cg_eng_errmsg_c_ctl'
    CG_ENG_ERRMSG_C_STATUS = 'cg_eng_errmsg_c_status'
    CG_ENG_ERRMSG_SUPERV = 'cg_eng_errmsg_superv'
    CG_ENG_ERRMSG_C_POWER_SYS = 'cg_eng_errmsg_c_power_sys'
    CG_ENG_ERRMSG_C_TELEM_SYS = 'cg_eng_errmsg_c_telem_sys'
    CG_ENG_ERRMSG_C_IRID = 'cg_eng_errmsg_c_irid'
    CG_ENG_ERRMSG_C_IMM = 'cg_eng_errmsg_c_imm'
    CG_ENG_ERRMSG_CPM1 = 'cg_eng_errmsg_cpm1'
    CG_ENG_ERRMSG_D_CTL = 'cg_eng_errmsg_d_ctl'
    CG_ENG_ERRMSG_D_STATUS = 'cg_eng_errmsg_d_status'
    CG_ENG_ERRMSG_DLOG_MGR = 'cg_eng_errmsg_dlog_mgr'
    CG_ENG_ERRMSG_DLOGP1 = 'cg_eng_errmsg_dlogp1'
    CG_ENG_ERRMSG_DLOGP2 = 'cg_eng_errmsg_dlogp2'
    CG_ENG_ERRMSG_DLOGP3 = 'cg_eng_errmsg_dlogp3'
    CG_ENG_ERRMSG_DLOGP4 = 'cg_eng_errmsg_dlogp4'
    CG_ENG_ERRMSG_DLOGP5 = 'cg_eng_errmsg_dlogp5'
    CG_ENG_ERRMSG_DLOGP6 = 'cg_eng_errmsg_dlogp6'
    CG_ENG_ERRMSG_DLOGP7 = 'cg_eng_errmsg_dlogp7'
    CG_ENG_ERRMSG_DLOGP8 = 'cg_eng_errmsg_dlogp8'
    CG_ENG_ERRMSG_RCMD = 'cg_eng_errmsg_rcmd'
    CG_ENG_ERRMSG_BCMD = 'cg_eng_errmsg_bcmd'
    CG_ENG_CPU_UPTIME = 'cg_eng_cpu_uptime'
    CG_ENG_CPU_LOAD1 = 'cg_eng_cpu_load1'
    CG_ENG_CPU_LOAD5 = 'cg_eng_cpu_load5'
    CG_ENG_CPU_LOAD15 = 'cg_eng_cpu_load15'
    CG_ENG_MEMORY_RAM = 'cg_eng_memory_ram'
    CG_ENG_MEMORY_FREE = 'cg_eng_memory_free'
    CG_ENG_NPROC = 'cg_eng_nproc'
    CG_ENG_MPIC_EFLAG = 'cg_eng_mpic_eflag'
    CG_ENG_MPIC_MAIN_V = 'cg_eng_mpic_main_v'
    CG_ENG_MPIC_MAIN_C = 'cg_eng_mpic_main_c'
    CG_ENG_MPIC_BAT_V = 'cg_eng_mpic_bat_v'
    CG_ENG_MPIC_BAT_C = 'cg_eng_mpic_bat_c'
    CG_ENG_MPIC_TEMP1 = 'cg_eng_mpic_temp1'
    CG_ENG_MPIC_TEMP2 = 'cg_eng_mpic_temp2'
    CG_ENG_MPIC_HUMID = 'cg_eng_mpic_humid'
    CG_ENG_MPIC_PRESS = 'cg_eng_mpic_press'
    CG_ENG_MPIC_GF_ENA = 'cg_eng_mpic_gf_ena'
    CG_ENG_MPIC_GFLT1 = 'cg_eng_mpic_gflt1'
    CG_ENG_MPIC_GFLT2 = 'cg_eng_mpic_gflt2'
    CG_ENG_MPIC_GFLT3 = 'cg_eng_mpic_gflt3'
    CG_ENG_MPIC_GFLT4 = 'cg_eng_mpic_gflt4'
    CG_ENG_MPIC_LD_ENA = 'cg_eng_mpic_ld_ena'
    CG_ENG_MPIC_LDET1 = 'cg_eng_mpic_ldet1'
    CG_ENG_MPIC_LDET2 = 'cg_eng_mpic_ldet2'
    CG_ENG_MPIC_WSRC = 'cg_eng_mpic_wsrc'
    CG_ENG_MPIC_IRID = 'cg_eng_mpic_irid'
    CG_ENG_MPIC_IRID_V = 'cg_eng_mpic_irid_v'
    CG_ENG_MPIC_IRID_C = 'cg_eng_mpic_irid_c'
    CG_ENG_MPIC_IRID_E = 'cg_eng_mpic_irid_e'
    CG_ENG_MPIC_FW_WIFI = 'cg_eng_mpic_fw_wifi'
    CG_ENG_MPIC_FW_WIFI_V = 'cg_eng_mpic_fw_wifi_v'
    CG_ENG_MPIC_FW_WIFI_C = 'cg_eng_mpic_fw_wifi_c'
    CG_ENG_MPIC_FW_WIFI_E = 'cg_eng_mpic_fw_wifi_e'
    CG_ENG_MPIC_GPS = 'cg_eng_mpic_gps'
    CG_ENG_MPIC_SBD = 'cg_eng_mpic_sbd'
    CG_ENG_MPIC_SBD_CE_MSG = 'cg_eng_mpic_sbd_ce_msg'
    CG_ENG_MPIC_PPS = 'cg_eng_mpic_pps'
    CG_ENG_MPIC_DCL = 'cg_eng_mpic_dcl'
    CG_ENG_MPIC_ESW = 'cg_eng_mpic_esw'
    CG_ENG_MPIC_DSL = 'cg_eng_mpic_dsl'
    CG_ENG_MPIC_HBEAT_ENABLE = 'cg_eng_mpic_hbeat_enable'
    CG_ENG_MPIC_HBEAT_DTIME = 'cg_eng_mpic_hbeat_dtime'
    CG_ENG_MPIC_HBEAT_THRESHOLD = 'cg_eng_mpic_hbeat_threshold'
    CG_ENG_MPIC_WAKE_CPM = 'cg_eng_mpic_wake_cpm'
    CG_ENG_MPIC_WPC = 'cg_eng_mpic_wpc'
    CG_ENG_MPIC_EFLAG2 = 'cg_eng_mpic_eflag2'
    CG_ENG_MPIC_LAST_UPDATE = 'cg_eng_mpic_last_update'
    CG_ENG_PWRSYS_MAIN_V = 'cg_eng_pwrsys_main_v'
    CG_ENG_PWRSYS_MAIN_C = 'cg_eng_pwrsys_main_c'
    CG_ENG_PWRSYS_B_CHG = 'cg_eng_pwrsys_b_chg'
    CG_ENG_PWRSYS_OVERRIDE = 'cg_eng_pwrsys_override'
    CG_ENG_PWRSYS_EFLAG1 = 'cg_eng_pwrsys_eflag1'
    CG_ENG_PWRSYS_EFLAG2 = 'cg_eng_pwrsys_eflag2'
    CG_ENG_PWRSYS_EFLAG3 = 'cg_eng_pwrsys_eflag3'
    CG_ENG_PWRSYS_B1_0 = 'cg_eng_pwrsys_b1_0'
    CG_ENG_PWRSYS_B1_1 = 'cg_eng_pwrsys_b1_1'
    CG_ENG_PWRSYS_B1_2 = 'cg_eng_pwrsys_b1_2'
    CG_ENG_PWRSYS_B2_0 = 'cg_eng_pwrsys_b2_0'
    CG_ENG_PWRSYS_B2_1 = 'cg_eng_pwrsys_b2_1'
    CG_ENG_PWRSYS_B2_2 = 'cg_eng_pwrsys_b2_2'
    CG_ENG_PWRSYS_B3_0 = 'cg_eng_pwrsys_b3_0'
    CG_ENG_PWRSYS_B3_1 = 'cg_eng_pwrsys_b3_1'
    CG_ENG_PWRSYS_B3_2 = 'cg_eng_pwrsys_b3_2'
    CG_ENG_PWRSYS_B4_0 = 'cg_eng_pwrsys_b4_0'
    CG_ENG_PWRSYS_B4_1 = 'cg_eng_pwrsys_b4_1'
    CG_ENG_PWRSYS_B4_2 = 'cg_eng_pwrsys_b4_2'
    CG_ENG_PWRSYS_PV1_0 = 'cg_eng_pwrsys_pv1_0'
    CG_ENG_PWRSYS_PV1_1 = 'cg_eng_pwrsys_pv1_1'
    CG_ENG_PWRSYS_PV1_2 = 'cg_eng_pwrsys_pv1_2'
    CG_ENG_PWRSYS_PV2_0 = 'cg_eng_pwrsys_pv2_0'
    CG_ENG_PWRSYS_PV2_1 = 'cg_eng_pwrsys_pv2_1'
    CG_ENG_PWRSYS_PV2_2 = 'cg_eng_pwrsys_pv2_2'
    CG_ENG_PWRSYS_PV3_0 = 'cg_eng_pwrsys_pv3_0'
    CG_ENG_PWRSYS_PV3_1 = 'cg_eng_pwrsys_pv3_1'
    CG_ENG_PWRSYS_PV3_2 = 'cg_eng_pwrsys_pv3_2'
    CG_ENG_PWRSYS_PV4_0 = 'cg_eng_pwrsys_pv4_0'
    CG_ENG_PWRSYS_PV4_1 = 'cg_eng_pwrsys_pv4_1'
    CG_ENG_PWRSYS_PV4_2 = 'cg_eng_pwrsys_pv4_2'
    CG_ENG_PWRSYS_WT1_0 = 'cg_eng_pwrsys_wt1_0'
    CG_ENG_PWRSYS_WT1_1 = 'cg_eng_pwrsys_wt1_1'
    CG_ENG_PWRSYS_WT1_2 = 'cg_eng_pwrsys_wt1_2'
    CG_ENG_PWRSYS_WT2_0 = 'cg_eng_pwrsys_wt2_0'
    CG_ENG_PWRSYS_WT2_1 = 'cg_eng_pwrsys_wt2_1'
    CG_ENG_PWRSYS_WT2_2 = 'cg_eng_pwrsys_wt2_2'
    CG_ENG_PWRSYS_FC1_0 = 'cg_eng_pwrsys_fc1_0'
    CG_ENG_PWRSYS_FC1_1 = 'cg_eng_pwrsys_fc1_1'
    CG_ENG_PWRSYS_FC1_2 = 'cg_eng_pwrsys_fc1_2'
    CG_ENG_PWRSYS_FC2_0 = 'cg_eng_pwrsys_fc2_0'
    CG_ENG_PWRSYS_FC2_1 = 'cg_eng_pwrsys_fc2_1'
    CG_ENG_PWRSYS_FC2_2 = 'cg_eng_pwrsys_fc2_2'
    CG_ENG_PWRSYS_TEMP = 'cg_eng_pwrsys_temp'
    CG_ENG_PWRSYS_FC_LEVEL = 'cg_eng_pwrsys_fc_level'
    CG_ENG_PWRSYS_SWG_0 = 'cg_eng_pwrsys_swg_0'
    CG_ENG_PWRSYS_SWG_1 = 'cg_eng_pwrsys_swg_1'
    CG_ENG_PWRSYS_SWG_2 = 'cg_eng_pwrsys_swg_2'
    CG_ENG_PWRSYS_CVT_0 = 'cg_eng_pwrsys_cvt_0'
    CG_ENG_PWRSYS_CVT_1 = 'cg_eng_pwrsys_cvt_1'
    CG_ENG_PWRSYS_CVT_2 = 'cg_eng_pwrsys_cvt_2'
    CG_ENG_PWRSYS_CVT_3 = 'cg_eng_pwrsys_cvt_3'
    CG_ENG_PWRSYS_CVT_4 = 'cg_eng_pwrsys_cvt_4'
    CG_ENG_PWRSYS_LAST_UPDATE = 'cg_eng_pwrsys_last_update'
    CG_ENG_GPS_MSG_DATE = 'cg_eng_gps_msg_date'
    CG_ENG_GPS_MSG_TIME = 'cg_eng_gps_msg_time'
    CG_ENG_GPS_DATE = 'cg_eng_gps_date'
    CG_ENG_GPS_TIME = 'cg_eng_gps_time'
    CG_ENG_GPS_LATSTR = 'cg_eng_gps_latstr'
    CG_ENG_GPS_LONSTR = 'cg_eng_gps_lonstr'
    CG_ENG_GPS_LAT = 'cg_eng_gps_lat'
    CG_ENG_GPS_LON = 'cg_eng_gps_lon'
    CG_ENG_GPS_SPD = 'cg_eng_gps_spd'
    CG_ENG_GPS_COG = 'cg_eng_gps_cog'
    CG_ENG_GPS_FIX = 'cg_eng_gps_fix'
    CG_ENG_GPS_NSAT = 'cg_eng_gps_nsat'
    CG_ENG_GPS_HDOP = 'cg_eng_gps_hdop'
    CG_ENG_GPS_ALT = 'cg_eng_gps_alt'
    CG_ENG_GPS_LAST_UPDATE = 'cg_eng_gps_last_update'
    CG_ENG_SCHED_SYS = 'cg_eng_sched_sys'
    CG_ENG_SCHED_TYPE = 'cg_eng_sched_type'
    CG_ENG_SCHED_STATUS = 'cg_eng_sched_status'
    CG_ENG_SCHED_STATUS_VAL = 'cg_eng_sched_status_val'
    CG_ENG_SCHED_NUM1 = 'cg_eng_sched_num1'
    CG_ENG_SCHED_NUM2 = 'cg_eng_sched_num2'
    CG_ENG_SCHED_NUM3 = 'cg_eng_sched_num3'
    CG_ENG_SCHED_NUM4 = 'cg_eng_sched_num4'
    CG_ENG_SCHED_NUM5 = 'cg_eng_sched_num5'
    CG_ENG_SCHED_NUM6 = 'cg_eng_sched_num6'
    CG_ENG_SCHED_NUM7 = 'cg_eng_sched_num7'
    CG_ENG_SCHED_TIME = 'cg_eng_sched_time'
    CG_ENG_SCHED_REMAINING = 'cg_eng_sched_remaining'
    CG_ENG_SCHED_LAST_UPDATE = 'cg_eng_sched_last_update'
    CG_ENG_NTP_REFID = 'cg_eng_ntp_refid'
    CG_ENG_NTP_OFFSET = 'cg_eng_ntp_offset'
    CG_ENG_NTP_JITTER = 'cg_eng_ntp_jitter'
    CG_ENG_PPS_LOCK = 'cg_eng_pps_lock'
    CG_ENG_PPS_DELTA = 'cg_eng_pps_delta'
    CG_ENG_PPS_DELTAMIN = 'cg_eng_pps_deltamin'
    CG_ENG_PPS_DELTAMAX = 'cg_eng_pps_deltamax'
    CG_ENG_PPS_BAD_PULSE = 'cg_eng_pps_bad_pulse'
    CG_ENG_PPS_TIMESTAMP = 'cg_eng_pps_timestamp'
    CG_ENG_PPS_LAST_UPDATE = 'cg_eng_pps_last_update'
    CG_ENG_LOADSHED_STATUS = 'cg_eng_loadshed_status'
    CG_ENG_LOADSHED_LAST_UPDATE = 'cg_eng_loadshed_last_update'
    CG_ENG_SBC_ETH0 = 'cg_eng_sbc_eth0'
    CG_ENG_SBC_ETH1 = 'cg_eng_sbc_eth1'
    CG_ENG_SBC_LED0 = 'cg_eng_sbc_led0'
    CG_ENG_SBC_LED1 = 'cg_eng_sbc_led1'
    CG_ENG_SBC_LED2 = 'cg_eng_sbc_led2'
    CG_ENG_SBC_GPO0 = 'cg_eng_sbc_gpo0'
    CG_ENG_SBC_GPO1 = 'cg_eng_sbc_gpo1'
    CG_ENG_SBC_GPO2 = 'cg_eng_sbc_gpo2'
    CG_ENG_SBC_GPO3 = 'cg_eng_sbc_gpo3'
    CG_ENG_SBC_GPO4 = 'cg_eng_sbc_gpo4'
    CG_ENG_SBC_GPIO0 = 'cg_eng_sbc_gpio0'
    CG_ENG_SBC_GPIO1 = 'cg_eng_sbc_gpio1'
    CG_ENG_SBC_GPIO2 = 'cg_eng_sbc_gpio2'
    CG_ENG_SBC_GPIO3 = 'cg_eng_sbc_gpio3'
    CG_ENG_SBC_GPIO4 = 'cg_eng_sbc_gpio4'
    CG_ENG_SBC_GPIO5 = 'cg_eng_sbc_gpio5'
    CG_ENG_SBC_FB1 = 'cg_eng_sbc_fb1'
    CG_ENG_SBC_FB2 = 'cg_eng_sbc_fb2'
    CG_ENG_SBC_CE_LED = 'cg_eng_sbc_ce_led'
    CG_ENG_SBC_WDT = 'cg_eng_sbc_wdt'
    CG_ENG_SBC_BID = 'cg_eng_sbc_bid'
    CG_ENG_SBC_BSTR = 'cg_eng_sbc_bstr'


# Example:
# Platform.time=2014/08/17 12:55:03.730
PLATFORM_TIME_REGEX = r'Platform.time=(?P<' + \
                      CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_TIME + \
                      '>' + DATE_YYYY_MM_DD_REGEX + ' ' + \
                      TIME_HR_MIN_SEC_MSEC_REGEX + ')' + \
                      END_OF_LINE_REGEX

# Example:
# Platform.utime=1408280103.730
PLATFORM_UTIME_REGEX = r'Platform.utime=(?P<' + \
                       CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_UTIME + \
                       '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# ALARM.C_POWER_SYS=ts=2014/08/17 12:53:58.217  severity=Warning  at=4  pc=3  err= NO PSC DATA
ALARM_REGEX = r'ALARM.(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_SYS + \
              '>\w+)=ts=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_TS + \
              '>' + DATE_YYYY_MM_DD_REGEX + ' ' + TIME_HR_MIN_SEC_MSEC_REGEX + \
              ')\s+severity=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_SEVERITY + \
              '>\w+)\s+at=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_AT + \
              '>\d+)\s+pc=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_PC + \
              '>\d+)\s+err=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_ERR + \
              '>.*)' + END_OF_LINE_REGEX

# Example:
# STATUS.msg_cnts=C_GPS=138, NTP=1, C_PPS=7, PWRSYS=2, SUPERV=36, TELEM=0
STATUS_MSG_CNTS_REGEX = \
    r'STATUS.msg_cnts=C_GPS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_GPS + \
    '>\d+), NTP=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_NTP + \
    '>\d+), C_PPS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_PPS + \
    '>\d+), PWRSYS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_POWER_SYS + \
    '>\d+), SUPERV=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_SUPERV + \
    '>\d+), TELEM=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_TELEM + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# STATUS.err_cnts=C_GPS=1, C_PPS=7, SUPERV=9, DLOGP7=3, BCMD=8
STATUS_ERR_CNTS_REGEX = \
    r'STATUS.err_cnts=(?:C_GPS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_GPS + \
    '>\d+)(?:,\s+)?)?(?:C_PPS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_PPS + \
    '>\d+)(?:,\s+)?)?(?:C_CTL=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_CTL + \
    '>\d+)(?:,\s+)?)?(?:C_STATUS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_STATUS + \
    '>\d+)(?:,\s+)?)?(?:SUPERV=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_SUPERV + \
    '>\d+)(?:,\s+)?)?(?:C_POWER_SYS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_POWER_SYS + \
    '>\d+)(?:,\s+)?)?(?:C_TELEM_SYS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_TELEM_SYS + \
    '>\d+)(?:,\s+)?)?(?:C_IRID=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_IRID + \
    '>\d+)(?:,\s+)?)?(?:C_IMM=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_IMM + \
    '>\d+)(?:,\s+)?)?(?:CPM1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_CPM1 + \
    '>\d+)(?:,\s+)?)?(?:D_CTL=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_D_CTL + \
    '>\d+)(?:,\s+)?)?(?:D_STATUS=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_D_STATUS + \
    '>\d+)(?:,\s+)?)?(?:DLOG_MGR=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOG_MGR + \
    '>\d+)(?:,\s+)?)?(?:DLOGP1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP1 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP2 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP3 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP4=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP4 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP5=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP5 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP6=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP6 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP7=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP7 + \
    '>\d+)(?:,\s+)?)?(?:DLOGP8=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP8 + \
    '>\d+)(?:,\s+)?)?(?:RCMD=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_RCMD + \
    '>\d+)(?:,\s+)?)?(?:BCMD=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_BCMD + \
    '>\d+)(?:,\s+)?)?' + END_OF_LINE_REGEX

# Examples:
# STATUS.last_err.C_GPS=***Warning, ...
# STATUS.last_err.C_PPS=C_PPS: W ...
# STATUS.last_err.C_TELEM_SYS= ***...
STATUS_ERR_FORMAT = r'STATUS.last_err.%s=(?P<%s>.*)$'

STATUS_ERR_C_GPS_REGEX = STATUS_ERR_FORMAT % ('C_GPS', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_GPS)
STATUS_ERR_C_PPS_REGEX = STATUS_ERR_FORMAT % ('C_PPS', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_PPS)
STATUS_ERR_C_CTL_REGEX = STATUS_ERR_FORMAT % ('C_CTL', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_CTL)
STATUS_ERR_C_STATUS_REGEX = STATUS_ERR_FORMAT % ('C_STATUS', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_STATUS)
STATUS_ERR_SUPERV_REGEX = STATUS_ERR_FORMAT % ('SUPERV', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_SUPERV)
STATUS_ERR_C_POWER_SYS_REGEX = STATUS_ERR_FORMAT % ('C_POWER_SYS',
                                                    CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_POWER_SYS)
STATUS_ERR_C_TELEM_SYS_REGEX = STATUS_ERR_FORMAT % ('C_TELEM_SYS',
                                                    CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_TELEM_SYS)
STATUS_ERR_C_IRID_REGEX = STATUS_ERR_FORMAT % ('C_IRID', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_IRID)
STATUS_ERR_C_IMM_REGEX = STATUS_ERR_FORMAT % ('C_IMM', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_IMM)
STATUS_ERR_CPM1_REGEX = STATUS_ERR_FORMAT % ('CPM1', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_CPM1)
STATUS_ERR_D_CTL_REGEX = STATUS_ERR_FORMAT % ('D_CTL', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_D_CTL)
STATUS_ERR_D_STATUS_REGEX = STATUS_ERR_FORMAT % ('D_STATUS', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_D_STATUS)
STATUS_ERR_DLOG_MGR_REGEX = STATUS_ERR_FORMAT % ('DLOG_MGR', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOG_MGR)
STATUS_ERR_DLOGP1_REGEX = STATUS_ERR_FORMAT % ('DLOGP1', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP1)
STATUS_ERR_DLOGP2_REGEX = STATUS_ERR_FORMAT % ('DLOGP2', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP2)
STATUS_ERR_DLOGP3_REGEX = STATUS_ERR_FORMAT % ('DLOGP3', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP3)
STATUS_ERR_DLOGP4_REGEX = STATUS_ERR_FORMAT % ('DLOGP4', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP4)
STATUS_ERR_DLOGP5_REGEX = STATUS_ERR_FORMAT % ('DLOGP5', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP5)
STATUS_ERR_DLOGP6_REGEX = STATUS_ERR_FORMAT % ('DLOGP6', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP6)
STATUS_ERR_DLOGP7_REGEX = STATUS_ERR_FORMAT % ('DLOGP7', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP7)
STATUS_ERR_DLOGP8_REGEX = STATUS_ERR_FORMAT % ('DLOGP8', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP8)
STATUS_ERR_RCMD_REGEX = STATUS_ERR_FORMAT % ('RCMD', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_RCMD)
STATUS_ERR_BCMD_REGEX = STATUS_ERR_FORMAT % ('BCMD', CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_BCMD)

# Example:
# CPU.uptime=0 days 00:01:56
CPU_UPTIME_REGEX = \
    r'CPU.uptime=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_UPTIME + '>\d+ days ' + \
    TIME_HR_MIN_SEC_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# CPU.load=2.16 0.85 0.31
CPU_LOAD_REGEX = \
    r'CPU.load=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD1 + '>' + FLOAT_REGEX + \
    ')\s+' + '(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD5 + '>' + FLOAT_REGEX + \
    ')\s+' + '(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD15 + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# CPU.memory=Ram: 127460k  Free: 80972k
CPU_MEMORY_REGEX = \
    r'CPU.memory=Ram: (?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MEMORY_RAM + '>\d+)[A-z]\s+' + \
    'Free: (?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MEMORY_FREE + '>\d+)[A-z]' + END_OF_LINE_REGEX

# Example:
# CPU.nproc=87
CPU_NPROC_REGEX = \
    r'CPU.nproc=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_NPROC + '>\d+)' + END_OF_LINE_REGEX

# Example:
# MPIC.eflag=00000000
MPIC_EFLAG_REGEX = \
    r'MPIC.eflag=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_EFLAG + '>\d+)' + END_OF_LINE_REGEX

# Example:
# MPIC.main_v=31.10
MPIC_MAIN_V_REGEX = \
    r'MPIC.main_v=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_MAIN_V + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.main_c=541.00
MPIC_MAIN_C_REGEX = \
    r'MPIC.main_c=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_MAIN_C + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.bbat_v=0.00
MPIC_BBAT_V_REGEX = \
    r'MPIC.bbat_v=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_BAT_V + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.bbat_c=0.00
MPIC_BBAT_C_REGEX = \
    r'MPIC.bbat_c=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_BAT_C + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.temp=16.0 14.4
MPIC_TEMP_REGEX = \
    r'MPIC.temp=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_TEMP1 + '>' + FLOAT_REGEX + \
    ') ' + '(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_TEMP2 + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.humid=12.6
MPIC_HUMID_REGEX = \
    r'MPIC.humid=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HUMID + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.press=14.2
MPIC_PRESS_REGEX = \
    r'MPIC.press=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_PRESS + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.gf_ena=0f
MPIC_GF_ENA_REGEX = \
    r'MPIC.gf_ena=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GF_ENA + '>' + \
    ASCII_HEX_CHAR_REGEX + '{2})' + END_OF_LINE_REGEX

# Example:
# MPIC.gflt=7.3 76.6 120.6 6.5
MPIC_GFLT_REGEX = \
    r'MPIC.gflt=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT1 + '>' + FLOAT_REGEX + \
    ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT2 + '>' + FLOAT_REGEX + \
    ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT3 + '>' + FLOAT_REGEX + \
    ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT4 + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.ld_ena=03
MPIC_LD_ENA_REGEX = \
    r'MPIC.ld_ena=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LD_ENA + '>\d+)' + \
    END_OF_LINE_REGEX

# Example:
# MPIC.ldet=1204.0 1211.0
MPIC_LDET_REGEX = \
    r'MPIC.ldet=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LDET1 + '>' + FLOAT_REGEX + \
    ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LDET2 + '>' + FLOAT_REGEX + \
    ')' + END_OF_LINE_REGEX

# Example:
# MPIC.hotel=wake 2 ir 0 1.6 4.9 0 fwwf 3 11.9 305.5 0 gps 1 sbd 0 0 pps 0 dcl 08 esw 1 dsl 1
MPIC_HOTEL_REGEX = \
    r'MPIC.hotel=wake\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WSRC + \
    '>\d)\s+ir\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID + '>\d)\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_V + '>' + FLOAT_REGEX + ')\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_C + '>' + FLOAT_REGEX + ')\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_E + '>\d)\s+fwwf\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI + '>\d)\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_V + '>' + FLOAT_REGEX + ')\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_C + '>' + FLOAT_REGEX + ')\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_E + '>' + '\d)\s+gps\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GPS + '>' + '\d)\s+sbd\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_SBD + '>' + '\d)\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_SBD_CE_MSG + '>' + '\d)\s+pps\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_PPS + '>' + '\d)\s+dcl\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_DCL + '>' + '\d{2})\s+esw\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_ESW + '>' + '\d)\s+dsl\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_DSL + '>' + '\d)' + END_OF_LINE_REGEX

# Example:
# MPIC.cpm_hb=enable 1 dtime 125 threshold 2
MPIC_CPM_HB_REGEX = \
    r'MPIC.cpm_hb=enable\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_ENABLE + \
    '>\d)\s+dtime\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_DTIME + \
    '>\d+)\s+threshold\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_THRESHOLD + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# MPIC.wake_cpm=wtc 0.00 wpc 964
MPIC_WAKE_CPM_REGEX = \
    r'MPIC.wake_cpm=wtc\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WAKE_CPM + \
    '>' + FLOAT_REGEX + ')\s+wpc\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WPC + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# MPIC.stc_eflag2=00000000
MPIC_STC_EFLAG2_REGEX = \
    r'MPIC.stc_eflag2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_EFLAG2 + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# MPIC.last_update=0.001
MPIC_LAST_UPDATE_REGEX = \
    r'MPIC.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.main_v=0.00
PWRSYS_MAIN_V_REGEX = \
    r'Pwrsys.main_v=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_MAIN_V + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.main_c=0.00
PWRSYS_MAIN_C_REGEX = \
    r'Pwrsys.main_c=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_MAIN_C + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.b_chg=0.00
PWRSYS_B_CHG_REGEX = \
    r'Pwrsys.b_chg=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B_CHG + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.override=0000
PWRSYS_OVERRIDE_REGEX = \
    r'Pwrsys.override=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_OVERRIDE + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# Pwrsys.eflag1=00000000
PWRSYS_EFLAG1_REGEX = \
    r'Pwrsys.eflag1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG1 + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# Pwrsys.eflag2=00000000
PWRSYS_EFLAG2_REGEX = \
    r'Pwrsys.eflag2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG2 + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# Pwrsys.eflag3=00000000
PWRSYS_EFLAG3_REGEX = \
    r'Pwrsys.eflag3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG3 + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# Pwrsys.b1=0.00 0.00 0.0
PWRSYS_B1_REGEX = \
    r'Pwrsys.b1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_0 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.b2=0.00 0.00 0.0
PWRSYS_B2_REGEX = \
    r'Pwrsys.b2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_0 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.b3=0.00 0.00 0.0
PWRSYS_B3_REGEX = \
    r'Pwrsys.b3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_0 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.b4=0.00 0.00 0.0
PWRSYS_B4_REGEX = \
    r'Pwrsys.b4=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_0 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.pv1=0 0.00 0.00
PWRSYS_PV1_REGEX = \
    r'Pwrsys.pv1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.pv2=0 0.00 0.00
PWRSYS_PV2_REGEX = \
    r'Pwrsys.pv2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.pv3=0 0.00 0.00
PWRSYS_PV3_REGEX = \
    r'Pwrsys.pv3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.pv4=0 0.00 0.00
PWRSYS_PV4_REGEX = \
    r'Pwrsys.pv4=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.wt1=0 0.00 0.00
PWRSYS_WT1_REGEX = \
    r'Pwrsys.wt1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.wt2=0 0.00 0.00
PWRSYS_WT2_REGEX = \
    r'Pwrsys.wt2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.fc1=0 0.00 0.00
PWRSYS_FC1_REGEX = \
    r'Pwrsys.fc1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.fc2=0 0.00 0.00
PWRSYS_FC2_REGEX = \
    r'Pwrsys.fc2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.temp=0.00
PWRSYS_TEMP_REGEX = \
    r'Pwrsys.temp=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_TEMP + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.fc_level=0.00
PWRSYS_FC_LEVEL_REGEX = \
    r'Pwrsys.fc_level=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC_LEVEL + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.swg=0 0.0000 0.0000
PWRSYS_SWG_REGEX = \
    r'Pwrsys.swg=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_2 + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# Pwrsys.cvt=0 0.00 0.00 0  0
PWRSYS_CVT_REGEX = \
    r'Pwrsys.cvt=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_0 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_1 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_2 + \
    '>' + FLOAT_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_3 + \
    '>\d)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_4 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# Pwrsys.last_update=1408280103.730
PWRSYS_LAST_UPDATE_REGEX = \
    r'Pwrsys.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.timestamp=2014/08/17 12:55:02.868
GPS_TIMESTAMP_REGEX = \
    r'GPS.timestamp=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_MSG_DATE + \
    '>' + DATE_YYYY_MM_DD_REGEX + ')\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_MSG_TIME + \
    '>' + TIME_HR_MIN_SEC_MSEC_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.date=170814
GPS_DATE_REGEX = \
    r'GPS.date=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_DATE + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# GPS.time=125502
GPS_TIME_REGEX = \
    r'GPS.time=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_TIME + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# GPS.lat_str=4439.5250 N
GPS_LAT_STR_REGEX = \
    r'GPS.lat_str=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LATSTR + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# GPS.lon_str=12405.7295 W
GPS_LON_STR_REGEX = \
    r'GPS.lon_str=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LONSTR + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# GPS.lat=44.658750
GPS_LAT_REGEX = \
    r'GPS.lat=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LAT + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.lon=-124.095492
GPS_LON_REGEX = \
    r'GPS.lon=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LON + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.spd=0.90
GPS_SPD_REGEX = \
    r'GPS.spd=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_SPD + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.cog=303.50
GPS_COG_REGEX = \
    r'GPS.cog=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_COG + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.fix_q=2
GPS_FIX_Q_REGEX = \
    r'GPS.fix_q=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_FIX + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# GPS.nsat=10
GPS_NSAT_REGEX = \
    r'GPS.nsat=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_NSAT + \
    '>\d+)' + END_OF_LINE_REGEX

# Example:
# GPS.hdop=1.10
GPS_HDOP_REGEX = \
    r'GPS.hdop=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_HDOP + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.alt=1.70
GPS_ALT_REGEX = \
    r'GPS.alt=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_ALT + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# GPS.last_update=0.861
GPS_LAST_UPDATE_REGEX = \
    r'GPS.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Examples:
# Sched.cpm.wake=started 1:0,3,6,9,12,15,18,21:53:33   Remaining: 1857 sec
# Sched.dcl4.pwr=started 1:0,3,6,9,12,15,18,21:54:18   Remaining: 1017 sec
# Sched.dcl6.pwr=stopped 1:0,3,6,9,12,15,18,21:56:14   Start_in: 57  sec
# Sched.dcl7.pwr=stopped 1:0,3,6,9,12,15,18,21:56:14   Start_in: 57  sec
# Sched.telem.irid=started 1:0,3,6,9,12,15,18,21:55:30  Remaining: 1797 sec
SCHED1_REGEX = \
    r'Sched\.(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_SYS + \
    '>\w+)\.(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_TYPE + \
    '>\w+)=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_STATUS + \
    '>\w+)\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_STATUS_VAL + \
    '>\d+):(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM1 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM2 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM3 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM4 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM5 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM6 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM7 + \
    '>\d+),(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_TIME + \
    '>' + TIME_HR_MIN_SEC_REGEX + ')\s+\w+:\s+(?P<' + \
    CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_REMAINING + \
    '>\d+)\s+sec' + END_OF_LINE_REGEX

# Example:
# Sched.last_update=0.147
SCHED_LAST_UPDATE_REGEX = \
    r'Sched.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# NTP.refid=.PPS.
NTP_REFID_REGEX = \
    r'NTP.refid=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_REFID + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# NTP.offset=-5.154
NTP_OFFSET_REGEX = \
    r'NTP.offset=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_OFFSET + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# NTP.jitter=0.095
NTP_JITTER_REGEX = \
    r'NTP.jitter=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_JITTER + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# PPS.status=C_PPS: NMEA_Lock: TRUE  Delta: 1000026 DeltaMin: -0000003 DeltaMax: +0000134 BadPulses: 0002
# TS: 2014/08/17 12:54:56.005
PPS_STATUS_REGEX = \
    r'PPS.status=C_PPS:\s+NMEA_Lock:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_LOCK + \
    '>\w+)\s+Delta:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTA + \
    '>' + INT_REGEX + ')\s+DeltaMin:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTAMIN + \
    '>' + INT_REGEX + ')\s+DeltaMax:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTAMAX + \
    '>' + INT_REGEX + ')\s+BadPulses:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_BAD_PULSE + \
    '>' + INT_REGEX + ')\s+TS:\s+(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_TIMESTAMP + \
    '>' + DATE_YYYY_MM_DD_REGEX + '\s+' + TIME_HR_MIN_SEC_MSEC_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# PPS.last_update=7.724
PPS_LAST_UPDATE_REGEX = \
    r'PPS.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# LoadShed.status=ena 0 low 22.0 high 23.0 priority 6,4,7 shed:
LOADSHED_STATUS_REGEX = \
    r'LoadShed.status=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_LOADSHED_STATUS + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# LoadShed.last_update=0.167
LOADSHED_LAST_UPDATE_REGEX = \
    r'LoadShed.last_update=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_LOADSHED_LAST_UPDATE + \
    '>' + FLOAT_REGEX + ')' + END_OF_LINE_REGEX

# Example:
# sbc.eth0=1
SBC_ETH0_REGEX = \
    r'sbc.eth0=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_ETH0 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.eth1=0
SBC_ETH1_REGEX = \
    r'sbc.eth1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_ETH1 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.led0=1
SBC_LED0_REGEX = \
    r'sbc.led0=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED0 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.led1=0
SBC_LED1_REGEX = \
    r'sbc.led1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED1 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.led2=0
SBC_LED2_REGEX = \
    r'sbc.led2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED2 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpo0=0
SBC_GPO0_REGEX = \
    r'sbc.gpo0=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO0 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpo1=0
SBC_GPO1_REGEX = \
    r'sbc.gpo1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO1 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpo2=0
SBC_GPO2_REGEX = \
    r'sbc.gpo2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO2 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpo3=0
SBC_GPO3_REGEX = \
    r'sbc.gpo3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO3 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpo4=0
SBC_GPO4_REGEX = \
    r'sbc.gpo4=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO4 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi0=1
SBC_GPI0_REGEX = \
    r'sbc.gpi0=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO0 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi1=1
SBC_GPI1_REGEX = \
    r'sbc.gpi1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO1 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi2=1
SBC_GPI2_REGEX = \
    r'sbc.gpi2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO2 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi3=1
SBC_GPI3_REGEX = \
    r'sbc.gpi3=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO3 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi4=0
SBC_GPI4_REGEX = \
    r'sbc.gpi4=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO4 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.gpi5=0
SBC_GPI5_REGEX = \
    r'sbc.gpi5=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO5 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.fb1=0
SBC_FB1_REGEX = \
    r'sbc.fb1=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_FB1 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.fb2=0
SBC_FB2_REGEX = \
    r'sbc.fb2=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_FB2 + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.ce_led=1
SBC_CE_LED_REGEX = \
    r'sbc.ce_led=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_CE_LED + \
    '>\d)' + END_OF_LINE_REGEX

# Example:
# sbc.wdt=0x7a0
SBC_WDT_REGEX = \
    r'sbc.wdt=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_WDT + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# sbc.bid=0x6c88
SBC_BID_REGEX = \
    r'sbc.bid=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BID + \
    '>.*)' + END_OF_LINE_REGEX

# Example:
# sbc.bstr=0x10
SBC_BSTR_REGEX = \
    r'sbc.bstr=(?P<' + CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BSTR + \
    '>.*)' + END_OF_LINE_REGEX


class ExpectedInstancesEnum(BaseEnum):
    """
    Enum for the number of expected instances of a line in specific line for a
    cg_cpm_eng_cpm input file.
    """
    ONE = 0
    MANY = 1


# The following structure specified the regex's to use for each line in the file, the set of parameter
# names, types and initial values for data extracted for the regex and the number of instances
# expected.
PARAM_REGEX_RULES_AND_VALUES = [
    (PLATFORM_TIME_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_TIME, str, None],),
     ExpectedInstancesEnum.ONE),
    (PLATFORM_UTIME_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_UTIME, float, None],),
     ExpectedInstancesEnum.ONE),
    (ALARM_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_SYS, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_TS, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_SEVERITY, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_AT, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_PC, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ALARM_ERR, str, None],
      ), ExpectedInstancesEnum.MANY),
    (STATUS_MSG_CNTS_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_GPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_NTP, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_PPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_POWER_SYS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_SUPERV, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MSG_CNTS_C_TELEM, int, None],
      ), ExpectedInstancesEnum.ONE),
    (STATUS_ERR_CNTS_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_GPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_PPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_CTL, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_STATUS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_SUPERV, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_POWER_SYS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_TELEM_SYS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_IRID, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_C_IMM, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_CPM1, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_D_CTL, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_D_STATUS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOG_MGR, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP1, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP2, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP3, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP4, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP5, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP6, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP7, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_DLOGP8, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_RCMD, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_ERR_BCMD, int, None],
      ), ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_GPS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_GPS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_PPS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_PPS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_CTL_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_CTL, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_STATUS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_STATUS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_SUPERV_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_SUPERV, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_POWER_SYS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_POWER_SYS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_TELEM_SYS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_TELEM_SYS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_IRID_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_IRID, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_C_IMM_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_C_IMM, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_CPM1_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_CPM1, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_D_CTL_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_D_CTL, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_D_STATUS_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_D_STATUS, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOG_MGR_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOG_MGR, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP1_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP1, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP2_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP2, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP3_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP3, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP4_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP4, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP5_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP5, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP6_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP6, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP7_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP7, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_DLOGP8_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_DLOGP8, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_RCMD_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_RCMD, str, None],),
     ExpectedInstancesEnum.ONE),
    (STATUS_ERR_BCMD_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_ERRMSG_BCMD, str, None],),
     ExpectedInstancesEnum.ONE),
    (CPU_UPTIME_REGEX, ([CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_UPTIME, str, None],),
     ExpectedInstancesEnum.ONE),
    (CPU_LOAD_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD5, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_CPU_LOAD15, float, None],
      ), ExpectedInstancesEnum.ONE),
    (CPU_MEMORY_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MEMORY_RAM, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MEMORY_FREE, int, None],
      ), ExpectedInstancesEnum.ONE),
    (CPU_NPROC_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_NPROC, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_EFLAG_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_EFLAG, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_MAIN_V_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_MAIN_V, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_MAIN_C_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_MAIN_C, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_BBAT_V_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_BAT_V, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_BBAT_C_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_BAT_C, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_TEMP_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_TEMP1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_TEMP2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_HUMID_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HUMID, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_PRESS_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_PRESS, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_GF_ENA_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GF_ENA, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_GFLT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT2, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT3, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GFLT4, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_LD_ENA_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LD_ENA, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_LDET_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LDET1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LDET2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_HOTEL_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WSRC, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_V, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_C, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_IRID_E, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_V, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_C, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_FW_WIFI_E, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_SBD, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_SBD_CE_MSG, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_PPS, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_DCL, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_ESW, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_DSL, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_CPM_HB_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_ENABLE, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_DTIME, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_HBEAT_THRESHOLD, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_WAKE_CPM_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WAKE_CPM, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WPC, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_STC_EFLAG2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_EFLAG2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (MPIC_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_MAIN_V_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_MAIN_V, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_MAIN_C_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_MAIN_C, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_B_CHG_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B_CHG, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_OVERRIDE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_OVERRIDE, int, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_EFLAG1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_EFLAG2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_EFLAG3_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_EFLAG3, int, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_B1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_0, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B1_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_B2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_0, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B2_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_B3_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_0, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B3_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_B4_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_0, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_B4_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_PV1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV1_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_PV2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV2_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_PV3_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV3_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_PV4_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_PV4_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_WT1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT1_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_WT2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_WT2_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_FC1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC1_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_FC2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC2_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_TEMP_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_TEMP, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_FC_LEVEL_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_FC_LEVEL, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_SWG_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_SWG_2, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_CVT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_0, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_1, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_2, float, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_3, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_CVT_4, int, None],
      ), ExpectedInstancesEnum.ONE),
    (PWRSYS_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PWRSYS_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_TIMESTAMP_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_MSG_DATE, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_MSG_TIME, str, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_DATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_DATE, int, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_TIME_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_TIME, int, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_LAT_STR_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LATSTR, str, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_LON_STR_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LONSTR, str, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_LAT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LAT, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_LON_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LON, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_SPD_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_SPD, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_COG_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_COG, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_FIX_Q_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_FIX, int, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_NSAT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_NSAT, int, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_HDOP_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_HDOP, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_ALT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_ALT, float, None],
      ), ExpectedInstancesEnum.ONE),
    (GPS_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_GPS_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (SCHED1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_SYS, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_TYPE, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_STATUS, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_STATUS_VAL, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM1, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM2, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM3, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM4, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM5, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM6, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_NUM7, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_TIME, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_REMAINING, int, None],
      ), ExpectedInstancesEnum.MANY),
    (SCHED_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SCHED_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (NTP_REFID_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_REFID, str, None],
      ), ExpectedInstancesEnum.ONE),
    (NTP_OFFSET_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_OFFSET, float, None],
      ), ExpectedInstancesEnum.ONE),
    (NTP_JITTER_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_NTP_JITTER, float, None],
      ), ExpectedInstancesEnum.ONE),
    (PPS_STATUS_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_LOCK, str, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTA, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTAMIN, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_DELTAMAX, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_BAD_PULSE, int, None],
      [CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_TIMESTAMP, str, None],
      ), ExpectedInstancesEnum.ONE),
    (PPS_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_PPS_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (LOADSHED_STATUS_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_LOADSHED_STATUS, str, None],
      ), ExpectedInstancesEnum.ONE),
    (LOADSHED_LAST_UPDATE_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_LOADSHED_LAST_UPDATE, float, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_ETH0_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_ETH0, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_ETH1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_ETH1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_LED0_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED0, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_LED1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_LED2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_LED2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPO0_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO0, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPO1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPO2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPO3_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO3, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPO4_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPO4, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI0_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO0, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI3_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO3, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI4_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO4, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_GPI5_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_GPIO5, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_FB1_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_FB1, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_FB2_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_FB2, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_CE_LED_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_CE_LED, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_WDT_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_WDT, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_BID_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BID, int, None],
      ), ExpectedInstancesEnum.ONE),
    (SBC_BSTR_REGEX,
     ([CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BSTR, int, None],
      ), ExpectedInstancesEnum.ONE),
]


# The following constants are used for indexing into the tuples associated with the data structure
# above.
PARAM_REGEX_RULES_AND_VALUES_PARAM_NAME_INDEX = 0
PARAM_REGEX_RULES_AND_VALUES_PARAM_ENCODING_INDEX = 1
PARAM_REGEX_RULES_AND_VALUES_PARAM_VALUE_INDEX = 2
PARAM_REGEX_RULES_AND_VALUES_NUM_PARAM_INDICES = 3

# The following constants are used for indexing into tuple associated with the dictionary build and
# processed bu the data particle _build_parsed_values.
PARTICLE_DATA_PARAM_ENCODING_INDEX = 0
PARTICLE_DATA_PARAM_VALUE_INDEX = 1

# The following is a list of regular expressions for lines in the file to ignore.
IGNORE_REGEX_LIST = [r'CI_SYS_STAT.status=.*', 'CI_SYS_STAT.last_update=.*', END_OF_LINE_REGEX]


class CgCpmEngCpmDataParticle(DataParticle):
    """
    Abstract Class for the cg_cpm_eng_cmp data set
    """
    _data_particle_type = None

    def __init__(self,
                 raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(CgCpmEngCpmDataParticle, self).__init__(
            raw_data,
            port_timestamp,
            internal_timestamp,
            preferred_timestamp,
            quality_flag,
            new_sequence)

        self._data_dict = dict()

    def _build_parsed_values(self):
        """
        Build and return the parsed values for cg_cpm_eng_cpm data particle.
        """
        result = []

        # Let's make sure the raw_data is a dict
        if type(self.raw_data) is not dict:
            raise SampleException("Invalid raw_data format for CgCpmEngCpmDataParticle")

        # Assigning to a new variable for clarity
        data_dict = self.raw_data

        if CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_UTIME not in data_dict.keys():
            raise SampleException("Missing required platform utime parameter.")

        # Let's iterate through the sorted list of dict keys
        for param_name in sorted(data_dict.keys()):

            # Assigning to the dict value to a variable for clarity
            dict_value = data_dict[param_name]

            # Let's make sure we're dealing with a tuple
            if type(dict_value) is not tuple:
                raise SampleException("Invalid raw_data format for CgCpmEngCpmDataParticle")

            else:

                # Obtain the encoding rule
                encoding_rule = dict_value[PARTICLE_DATA_PARAM_ENCODING_INDEX]

                # Obtain the value part of the tuple
                value_part = dict_value[PARTICLE_DATA_PARAM_VALUE_INDEX]

                # Let's see if we're dealing with a list
                if type(value_part) is list:

                    # Init the result list which will contain the converted values
                    result_val_list = []

                    # Iterate through the list of values
                    for value in value_part:

                        # If the value is not None, let's apply the encoding_rule
                        if value is not None:
                            result_val = encoding_rule(value)

                        # OK. Let's just set it to None
                        else:
                            result_val = value

                        # Append the converted value, or None whichever we ended up with
                        result_val_list.append(result_val)

                    # OK.  Let's encode the result as a list
                    result.append(self._encode_value(param_name, result_val_list, list))

                # OK.  We are not dealing with a list
                else:

                    # If the value is None, let's generate the particle param using the None
                    # and append it
                    if value_part is None:
                        result.append({DataParticleKey.VALUE_ID: param_name,
                                       DataParticleKey.VALUE: None})

                    # So we are not dealing with a None
                    else:

                        # Reassigning for clarity
                        value = value_part

                        # If the param is the platform utime, we need to use that as the internal
                        # timestamp
                        if param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_PLATFORM_UTIME:

                            # Convert it to a float and set it as the unix_time
                            self.set_internal_timestamp(unix_time=float(value))

                        # If we end up with one of the following parameters, we need to convert
                        # the hex string to an int
                        if param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BID or \
                           param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_BSTR or \
                           param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_SBC_WDT:

                            value = int(value, 0)

                        if param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_WAKE_CPM:
                            value = int(float(value))

                        if param_name == CgCpmEngCpmParserDataParticleKey.CG_ENG_MPIC_GF_ENA:
                            value = int('0x'+value, 0)

                        # Append the encoded value
                        result.append(self._encode_value(param_name,
                                                         value,
                                                         encoding_rule))

        log.trace("Returning result: %s", result)

        return result


class CgCpmEngCpmRecoveredDataParticle(CgCpmEngCpmDataParticle):
    """
    Class for the recovered cg_stc_eng_stc data set
    """
    _data_particle_type = CgCpmEngCpmDataParticleType.CG_CPM_ENG_CPM_RECOVERED


class CgCpmEngCpmTelemeteredDataParticle(CgCpmEngCpmDataParticle):
    """
    Class for the telemetered cg_stc_eng_stc data set
    """
    _data_particle_type = CgCpmEngCpmDataParticleType.CG_CPM_ENG_CPM_TELEMETERED


class CgCpmEngCpmParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 *args, **kwargs):

        # no sieve function since we are not using the chunker here
        super(CgCpmEngCpmParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback)

        self._particle_class = eval(config[DataSetDriverConfigKeys.PARTICLE_CLASS])

    def parse_file(self):
        """
        This method will parse a cg_cpm_eng_cpm input file and collect the
        particles.
        """

        param_rules_and_values_dict = dict()

        # Initialize a working copy of the PARAM_REGEX_RULES_AND_VALUES which we will modify
        param_regex_rules_and_values = copy.deepcopy(PARAM_REGEX_RULES_AND_VALUES)

        # Read the first line in the file
        line = self._stream_handle.readline()

        # While a new line in the file exists
        while line:

            log.trace("Line: %s", line)

            match_found = False

            # Init the index at -1, since we will increment it on entry into the loop
            index = -1

            # Iterate through the list containing the regex, param rules and values and expected instance
            # info.  The list will be modified within the loop. Cases where the expected number of instances 
            # is ONE will result in popping the item off the list to eliminate unnecessary regex match checking.
            for regex, param_rules_and_values, expected_instances in \
                    param_regex_rules_and_values:

                index += 1

                match = re.match(regex, line)
                if match:

                    # Set a flag indicating we found a match
                    match_found = True

                    # Get the match group dictionary 
                    match_group_dict = match.groupdict()

                    # Iterate through each param, rule and value in the list to process
                    for param_rule_and_value in param_rules_and_values:

                        # Assigning the param name to a variable to for clarity
                        param_name = param_rule_and_value[PARAM_REGEX_RULES_AND_VALUES_PARAM_NAME_INDEX]

                        # Get the match group param value associated with the param name
                        value = match_group_dict.get(param_name)

                        # Update the value to the param in the working data structure
                        param_rule_and_value[PARAM_REGEX_RULES_AND_VALUES_PARAM_VALUE_INDEX] = value

                        # Copy the param rule and value portion into its own list
                        param_rule_and_value_copy = copy.deepcopy(
                            param_rule_and_value[PARAM_REGEX_RULES_AND_VALUES_PARAM_ENCODING_INDEX:
                                                 PARAM_REGEX_RULES_AND_VALUES_NUM_PARAM_INDICES])

                        # Are we dealing with only one expected instance?
                        if expected_instances == ExpectedInstancesEnum.ONE:
                            # Convert the list to a tuple and save
                            data_particle_tuple = tuple(param_rule_and_value_copy)

                        # OK.  We are expecting many.
                        else:
                            # Let's first check to see if the param_rules_and_values_dict already
                            # has a key that matches the param_name
                            if param_name not in param_rules_and_values_dict:

                                value_list = []

                                # Create a tuple containing the encoding and an empty list
                                data_particle_tuple =  \
                                    (param_rule_and_value_copy[
                                        PARTICLE_DATA_PARAM_ENCODING_INDEX], value_list)

                                # Now insert the value into the list within the tuple
                                value_list.append(param_rule_and_value_copy[
                                    PARTICLE_DATA_PARAM_VALUE_INDEX])

                            # OK.  So we did not find an entry in the dictionary
                            else:

                                # Assigning the tuple to a variable for clarity
                                data_particle_tuple = param_rules_and_values_dict[param_name]

                                # If the value is not None, let's process it
                                if value is not None:

                                    # Assigning the vaule list to a variable for clarity
                                    value_list = data_particle_tuple[PARTICLE_DATA_PARAM_VALUE_INDEX]

                                    # If we found a None in the list, let's remove it.  The None value
                                    # was just temporary until we found a non-None value
                                    if None in value_list:
                                        log.trace("Removing None value for %s since we now have a non-None value",
                                                  param_name)
                                        value_list.remove(None)

                                    log.trace("Appending value for %s", param_name)
                                    # Now insert the value into the list within the tuple
                                    value_list.append(value)

                        # Let's save off the results data_particle_tuple which we will use with
                        # _extract_sample
                        param_rules_and_values_dict[param_name] = data_particle_tuple

                    # If the number of expected instances was one, let's remove the entry so we don't
                    # keep looking for it
                    if expected_instances == ExpectedInstancesEnum.ONE:

                        log.trace("Dropping regex, rules and value entry")
                        # Done with that tuple, remove it
                        param_regex_rules_and_values.pop(index)

                    # Exit the loop.  We're done iterating through the param_regex_rules_and_values
                    # for this line in the file.
                    break

            # If we did not find a match, let's iterate through the ignore regex list
            if not match_found:

                for regex in IGNORE_REGEX_LIST:

                    if re.match(regex, line):
                        match_found = True
                        log.trace("Expected data to ignore: %r", line)

            # If we still did not find a match, there was unexpected data,  Let's report that.
            if not match_found:
                message = "Unexpected data: %r" % line
                log.error(message)
                self._exception_callback(UnexpectedDataException(message))

            # Read the next line in the file
            line = self._stream_handle.readline()

        # fill in any missing expected values so long as one value was present
        if param_rules_and_values_dict:
            for regex, param_rules_and_values, expected_instances in param_regex_rules_and_values:
                if expected_instances == ExpectedInstancesEnum.ONE:
                    for param_rule_and_value in param_rules_and_values:
                        param_name, encoding, value = param_rule_and_value
                        param_rules_and_values_dict[param_name] = (encoding, value)

        # Let's attempt to extract the single record expected in the file and append it to the record
        # buffer.
        record = self._extract_sample(self._particle_class, None, param_rules_and_values_dict, None)
        self._record_buffer.append(record)

        # Set an indication that the file was fully parsed
        self._file_parsed = True
