#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_glider Base dataset parser test code
@file mi/dataset/parser/test/test_glider.py
@author Chris Wingard, Stuart Pearce, Nick Almonte
@brief Test code for a Glider data parser.
"""

import os
from StringIO import StringIO
from nose.plugins.attrib import attr

from mi.core.exceptions import ConfigurationException
from mi.core.log import get_logger
from mi.dataset.parser.utilities import particle_to_yml

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.driver.moas.gl.ctdgv.resource import RESOURCE_PATH as CTDGV_RESOURCE_PATH
from mi.dataset.driver.moas.gl.dosta.resource import RESOURCE_PATH as DOSTA_RESOURCE_PATH
from mi.dataset.driver.moas.gl.engineering.resource import RESOURCE_PATH as ENG_RESOURCE_PATH
from mi.dataset.driver.moas.gl.flord_m.resource import RESOURCE_PATH as FLORD_M_RESOURCE_PATH
from mi.dataset.driver.moas.gl.flort_m.resource import RESOURCE_PATH as FLORT_M_RESOURCE_PATH
from mi.dataset.driver.moas.gl.flort_o.resource import RESOURCE_PATH as FLORT_O_RESOURCE_PATH
from mi.dataset.driver.moas.gl.parad.resource import RESOURCE_PATH as PARAD_RESOURCE_PATH
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.glider import GliderParser, GliderEngineeringParser
from mi.dataset.parser.glider import CtdgvRecoveredDataParticle, CtdgvTelemeteredDataParticle, CtdgvParticleKey
from mi.dataset.parser.glider import DostaTelemeteredDataParticle, DostaTelemeteredParticleKey
from mi.dataset.parser.glider import DostaRecoveredDataParticle, DostaRecoveredParticleKey
from mi.dataset.parser.glider import FlordRecoveredDataParticle, FlordTelemeteredDataParticle, FlordParticleKey
from mi.dataset.parser.glider import FlortRecoveredDataParticle, FlortRecoveredParticleKey
from mi.dataset.parser.glider import FlortTelemeteredDataParticle, FlortTelemeteredParticleKey
from mi.dataset.parser.glider import FlortODataParticle, FlortODataParticleKey
from mi.dataset.parser.glider import ParadRecoveredDataParticle, ParadRecoveredParticleKey
from mi.dataset.parser.glider import ParadTelemeteredDataParticle, ParadTelemeteredParticleKey
from mi.dataset.parser.glider import EngineeringTelemeteredParticleKey
from mi.dataset.parser.glider import EngineeringTelemeteredDataParticle
from mi.dataset.parser.glider import EngineeringScienceTelemeteredParticleKey
from mi.dataset.parser.glider import EngineeringScienceTelemeteredDataParticle
from mi.dataset.parser.glider import EngineeringMetadataParticleKey
from mi.dataset.parser.glider import EngineeringMetadataDataParticle
from mi.dataset.parser.glider import EngineeringMetadataRecoveredDataParticle
from mi.dataset.parser.glider import EngineeringRecoveredParticleKey
from mi.dataset.parser.glider import EngineeringRecoveredDataParticle
from mi.dataset.parser.glider import EngineeringScienceRecoveredParticleKey
from mi.dataset.parser.glider import EngineeringScienceRecoveredDataParticle, EngineeringClassKey
from mi.dataset.parser.glider import GpsPositionDataParticle, GpsPositionParticleKey

log = get_logger()

HEADER = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: unit_363-2013-245-6-6
the8x3_filename: 01790006
filename_extension: sbd
filename_label: unit_363-2013-245-6-6-sbd(01790006)
mission_name: TRANS58.MI
fileopen_time: Thu_Sep__5_02:46:15_2013
sensors_per_cycle: 29
num_label_lines: 3
num_segments: 1
segment_filename_0: unit_363-2013-245-6-6
c_battpos c_wpt_lat c_wpt_lon m_battpos m_coulomb_amphr_total m_coulomb_current m_depth m_de_oil_vol m_gps_lat m_gps_lon m_heading m_lat m_lon m_pitch m_present_secs_into_mission m_present_time m_speed m_water_vx m_water_vy x_low_power_status sci_flbb_bb_units sci_flbb_chlor_units sci_m_present_secs_into_mission sci_m_present_time sci_oxy4_oxygen sci_oxy4_saturation sci_water_cond sci_water_pressure sci_water_temp
in lat lon in amp-hrs amp m cc lat lon rad lat lon rad sec timestamp m/s m/s m/s nodim nodim ug/l sec timestamp um % s/m bar degc
4 8 8 4 4 4 4 4 8 8 4 8 8 4 4 8 4 4 4 4 4 4 4 8 4 4 4 4 4 """

# header from sample data in ctdgv driver test
HEADER2 = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: unit_363-2013-245-6-6
the8x3_filename: 01790006
filename_extension: sbd
filename_label: unit_363-2013-245-6-6-sbd(01790006)
mission_name: TRANS58.MI
fileopen_time: Thu_Sep__5_02:46:15_2013
sensors_per_cycle: 29
num_label_lines: 3
num_segments: 1
segment_filename_0: unit_363-2013-245-6-6
c_battpos c_wpt_lat c_wpt_lon m_battpos m_coulomb_amphr_total m_coulomb_current m_depth m_de_oil_vol m_gps_lat m_gps_lon m_heading m_lat m_lon m_pitch m_present_secs_into_mission m_present_time m_speed m_water_vx m_water_vy x_low_power_status sci_flbb_bb_units sci_flbb_chlor_units sci_m_present_secs_into_mission sci_m_present_time sci_oxy4_oxygen sci_oxy4_saturation sci_water_cond sci_water_pressure sci_water_temp
in lat lon in amp-hrs amp m cc lat lon rad lat lon rad sec timestamp m/s m/s m/s nodim nodim ug/l sec timestamp um % s/m bar degc
4 8 8 4 4 4 4 4 8 8 4 8 8 4 4 8 4 4 4 4 4 4 4 8 4 4 4 4 4  """

HEADER3 = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: unit_247-2012-051-0-0-sf
the8x3_filename: 01840000
filename_extension: dbd
filename_label: unit_247-2012-051-0-0-dbd(01840000)
mission_name: ENDUR1.MI
fileopen_time: Tue_Feb_21_18:39:39_2012
sensors_per_cycle: 346
num_label_lines: 3
num_segments: 1
segment_filename_0: unit_247-2012-051-0-0
c_air_pump c_ballast_pumped c_battpos c_battroll c_bsipar_on c_de_oil_vol c_dvl_on c_flbbcd_on c_heading c_oxy3835_wphase_on c_pitch c_profile_on c_wpt_lat c_wpt_lon m_1meg_persistor m_aground_water_depth m_air_fill m_air_pump m_altimeter_status m_altimeter_voltage m_altitude m_altitude_rate m_appear_to_be_at_surface m_argos_is_xmitting m_argos_on m_argos_sent_data m_argos_timestamp m_at_risk_depth m_avbot_enable m_avbot_power m_avg_climb_rate m_avg_depth_rate m_avg_dive_rate m_avg_downward_inflection_time m_avg_speed m_avg_system_clock_lags_gps m_avg_upward_inflection_time m_avg_yo_time m_ballast_pumped m_ballast_pumped_energy m_ballast_pumped_vel m_battery m_battery_inst m_battpos m_battpos_vel m_battroll m_battroll_vel m_bpump_fault_bit m_certainly_at_surface m_chars_tossed_by_abend m_chars_tossed_with_cd_off m_chars_tossed_with_power_off m_climb_tot_time m_console_cd m_console_on m_cop_tickle m_coulomb_amphr m_coulomb_amphr_raw m_coulomb_amphr_total m_coulomb_current m_coulomb_current_raw m_cycle_number m_depth m_depth_rate m_depth_rate_avg_final m_depth_rate_running_avg m_depth_rate_running_avg_n m_depth_rate_subsampled m_depth_rejected m_depth_state m_depth_subsampled m_device_drivers_called_abnormally m_device_error m_device_oddity m_device_warning m_de_oil_vol m_de_oil_vol_pot_voltage m_de_pump_fault_count m_digifin_cmd_done m_digifin_cmd_error m_digifin_leakdetect_reading m_digifin_motorstep_counter m_digifin_resp_data m_digifin_status m_disk_free m_disk_usage m_dist_to_wpt m_dive_depth m_dive_tot_time m_dr_fix_time m_dr_postfix_time m_dr_surf_x_lmc m_dr_surf_y_lmc m_dr_time m_dr_x_actual_err m_dr_x_ini_err m_dr_x_postfix_drift m_dr_x_ta_postfix_drift m_dr_y_actual_err m_dr_y_ini_err m_dr_y_postfix_drift m_dr_y_ta_postfix_drift m_est_time_to_surface m_fin m_final_water_vx m_final_water_vy m_fin_vel m_fluid_pumped m_fluid_pumped_aft_hall_voltage m_fluid_pumped_fwd_hall_voltage m_fluid_pumped_vel m_free_heap m_gps_dist_from_dr m_gps_fix_x_lmc m_gps_fix_y_lmc m_gps_full_status m_gps_heading m_gps_ignored_lat m_gps_ignored_lon m_gps_invalid_lat m_gps_invalid_lon m_gps_lat m_gps_lon m_gps_mag_var m_gps_num_satellites m_gps_on m_gps_postfix_x_lmc m_gps_postfix_y_lmc m_gps_speed m_gps_status m_gps_toofar_lat m_gps_toofar_lon m_gps_uncertainty m_gps_utc_day m_gps_utc_hour m_gps_utc_minute m_gps_utc_month m_gps_utc_second m_gps_utc_year m_gps_x_lmc m_gps_y_lmc m_hdg_derror m_hdg_error m_hdg_ierror m_hdg_rate m_heading m_initial_water_vx m_initial_water_vy m_iridium_attempt_num m_iridium_call_num m_iridium_connected m_iridium_console_on m_iridium_dialed_num m_iridium_on m_iridium_redials m_iridium_signal_strength m_iridium_status m_iridium_waiting_redial_delay m_iridium_waiting_registration m_is_ballast_pump_moving m_is_battpos_moving m_is_battroll_moving m_is_de_pump_moving m_is_fin_moving m_is_fpitch_pump_moving m_is_speed_estimated m_is_thermal_valve_moving m_last_yo_time m_lat m_leak m_leakdetect_voltage m_leakdetect_voltage_forward m_leak_forward m_lithium_battery_relative_charge m_lithium_battery_status m_lithium_battery_time_to_charge m_lithium_battery_time_to_discharge m_lon m_min_free_heap m_min_spare_heap m_mission_avg_speed_climbing m_mission_avg_speed_diving m_mission_start_time m_num_half_yos_in_segment m_pitch m_pitch_energy m_pitch_error m_present_secs_into_mission m_present_time m_pressure m_pressure_raw_voltage_sample0 m_pressure_raw_voltage_sample19 m_pressure_voltage m_raw_altitude m_raw_altitude_rejected m_roll m_science_clothesline_lag m_science_on m_science_ready_for_consci m_science_sent_some_data m_science_sync_time m_science_unreadiness_for_consci m_spare_heap m_speed m_stable_comms m_strobe_ctrl m_surface_est_cmd m_surface_est_ctd m_surface_est_fw m_surface_est_gps m_surface_est_irid m_surface_est_total m_system_clock_lags_gps m_tcm3_is_calibrated m_tcm3_magbearth m_tcm3_poll_time m_tcm3_recv_start_time m_tcm3_recv_stop_time m_tcm3_stddeverr m_tcm3_xcoverage m_tcm3_ycoverage m_tcm3_zcoverage m_thermal_acc_pres m_thermal_acc_pres_voltage m_thermal_acc_vol m_thermal_enuf_acc_vol m_thermal_pump m_thermal_updown m_thermal_valve m_time_til_wpt m_tot_ballast_pumped_energy m_tot_horz_dist m_tot_num_inflections m_tot_on_time m_vacuum m_vehicle_temp m_veh_overheat m_veh_temp m_vmg_to_wpt m_vx_lmc m_vy_lmc m_water_cond m_water_delta_vx m_water_delta_vy m_water_depth m_water_pressure m_water_temp m_water_vx m_water_vy m_why_started m_x_lmc m_y_lmc x_last_wpt_lat x_last_wpt_lon x_system_clock_adjusted sci_bsipar_is_installed sci_bsipar_par sci_bsipar_sensor_volts sci_bsipar_supply_volts sci_bsipar_temp sci_bsipar_timestamp sci_ctd41cp_is_installed sci_ctd41cp_timestamp sci_dvl_bd_range_to_bottom sci_dvl_bd_time_since_last_good_vel sci_dvl_bd_u_dist sci_dvl_bd_v_dist sci_dvl_bd_w_dist sci_dvl_be_u_vel sci_dvl_be_v_vel sci_dvl_be_vel_good sci_dvl_be_w_vel sci_dvl_bi_err_vel sci_dvl_bi_vel_good sci_dvl_bi_x_vel sci_dvl_bi_y_vel sci_dvl_bi_z_vel sci_dvl_bs_longitudinal_vel sci_dvl_bs_normal_vel sci_dvl_bs_transverse_vel sci_dvl_bs_vel_good sci_dvl_ensemble_offset sci_dvl_error sci_dvl_is_installed sci_dvl_sa_heading sci_dvl_sa_pitch sci_dvl_sa_roll sci_dvl_ts_bit sci_dvl_ts_depth sci_dvl_ts_sal sci_dvl_ts_sound_speed sci_dvl_ts_temp sci_dvl_ts_timestamp sci_dvl_wd_range_to_water_mass_center sci_dvl_wd_time_since_last_good_vel sci_dvl_wd_u_dist sci_dvl_wd_v_dist sci_dvl_wd_w_dist sci_dvl_we_u_vel sci_dvl_we_v_vel sci_dvl_we_vel_good sci_dvl_we_w_vel sci_dvl_wi_err_vel sci_dvl_wi_vel_good sci_dvl_wi_x_vel sci_dvl_wi_y_vel sci_dvl_wi_z_vel sci_dvl_ws_longitudinal_vel sci_dvl_ws_normal_vel sci_dvl_ws_transverse_vel sci_dvl_ws_vel_good sci_flbbcd_bb_ref sci_flbbcd_bb_sig sci_flbbcd_bb_units sci_flbbcd_cdom_ref sci_flbbcd_cdom_sig sci_flbbcd_cdom_units sci_flbbcd_chlor_ref sci_flbbcd_chlor_sig sci_flbbcd_chlor_units sci_flbbcd_is_installed sci_flbbcd_therm sci_flbbcd_timestamp sci_m_disk_free sci_m_disk_usage sci_m_free_heap sci_m_min_free_heap sci_m_min_spare_heap sci_m_present_secs_into_mission sci_m_present_time sci_m_science_on sci_m_spare_heap sci_oxy3835_is_installed sci_oxy3835_oxygen sci_oxy3835_saturation sci_oxy3835_temp sci_oxy3835_timestamp sci_reqd_heartbeat sci_software_ver sci_wants_comms sci_wants_surface sci_water_cond sci_water_pressure sci_water_temp sci_x_disk_files_removed sci_x_sent_data_files
enum cc in rad sec cc sec sec rad sec rad sec lat lon bool m bool bool enum volts m m/s bool bool bool bool timestamp m bool bool m/s m/s m/s sec m/s sec sec sec cc joules cc/sec volts volts in in/sec rad rad/sec bool bool nodim nodim nodim s bool bool bool amp-hrs nodim amp-hrs amp nodim nodim m m/s m/s m/s enum m/s bool enum m nodim nodim nodim nodim cc volts nodim nodim nodim nodim nodim nodim nodim Mbytes Mbytes m m s sec sec m m sec m m m m m m m m sec rad m/s m/s rad/sec cc volts volts cc/sec bytes m m m enum rad lat lon lat lon lat lon rad nodim bool m m m/s enum lat lat nodim byte byte byte byte nodim byte m m rad/sec rad rad-sec rad/sec rad m/s m/s nodim nodim bool enum nodim bool nodim nodim enum bool bool bool bool bool bool bool bool bool bool sec lat bool volts volts bool % nodim mins mins lon bytes bytes m/s m/s timestamp nodim rad joules rad sec timestamp bar volts volts volts m bool rad s bool bool nodim timestamp enum bytes m/s bool bool nodim nodim nodim nodim nodim nodim sec bool uT ms ms ms uT % % % bar volts cc bool enum enum enum s kjoules km nodim days inHg degC bool c m/s m/s m/s S/m m/s m/s m bar degC m/s m/s enum m m lat lon sec bool ue/m^2sec volts volts degc timestamp bool timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim bool deg deg deg nodim m ppt m/s degc timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim nodim nodim nodim ppb nodim nodim ug/l bool nodim timestamp mbytes mbytes bytes bytes bytes sec timestamp bool bytes bool nodim nodim nodim timestamp secs nodim bool enum s/m bar degc nodim nodim
1 4 4 4 4 4 4 4 4 4 4 4 8 8 1 4 1 1 1 4 4 4 1 1 1 1 8 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 1 4 4 4 4 1 1 1 4 4 4 4 4 4 4 4 4 4 1 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 4 8 8 8 8 8 8 4 4 1 4 4 4 1 8 8 4 1 1 1 1 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 4 1 4 4 1 1 1 1 1 1 1 1 1 1 1 4 8 1 4 4 1 4 4 4 4 8 4 4 4 4 8 4 4 4 4 4 8 4 4 4 4 4 1 4 4 1 1 4 8 1 4 4 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 4 1 4 4 8 8 4 1 4 4 4 4 8 1 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 4 4 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 1 4 8 4 4 4 4 4 4 8 1 4 1 4 4 4 8 4 4 1 1 4 4 4 4 4 """

HEADER4 = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: unit_247-2012-051-0-0-sf
the8x3_filename: 01840000
filename_extension: dbd
filename_label: unit_247-2012-051-0-0-dbd(01840000)
mission_name: ENDUR1.MI
fileopen_time: Tue_Feb_21_18:39:39_2012
sensors_per_cycle: 347
num_label_lines: 3
num_segments: 1
segment_filename_0: unit_247-2012-051-0-0
c_air_pump c_ballast_pumped c_battpos c_battroll c_bsipar_on c_de_oil_vol c_dvl_on c_flbbcd_on c_heading c_oxy3835_wphase_on c_pitch c_profile_on c_wpt_lat c_wpt_lon m_1meg_persistor m_aground_water_depth m_air_fill m_air_pump m_altimeter_status m_altimeter_voltage m_altitude m_altitude_rate m_appear_to_be_at_surface m_argos_is_xmitting m_argos_on m_argos_sent_data m_argos_timestamp m_at_risk_depth m_avbot_enable m_avbot_power m_avg_climb_rate m_avg_depth_rate m_avg_dive_rate m_avg_downward_inflection_time m_avg_speed m_avg_system_clock_lags_gps m_avg_upward_inflection_time m_avg_yo_time m_ballast_pumped m_ballast_pumped_energy m_ballast_pumped_vel m_battery m_battery_inst m_battpos m_battpos_vel m_battroll m_battroll_vel m_bpump_fault_bit m_certainly_at_surface m_chars_tossed_by_abend m_chars_tossed_with_cd_off m_chars_tossed_with_power_off m_climb_tot_time m_console_cd m_console_on m_cop_tickle m_coulomb_amphr m_coulomb_amphr_raw m_coulomb_amphr_total m_coulomb_current m_coulomb_current_raw m_cycle_number m_depth m_depth_rate m_depth_rate_avg_final m_depth_rate_running_avg m_depth_rate_running_avg_n m_depth_rate_subsampled m_depth_rejected m_depth_state m_depth_subsampled m_device_drivers_called_abnormally m_device_error m_device_oddity m_device_warning m_de_oil_vol m_de_oil_vol_pot_voltage m_de_pump_fault_count m_digifin_cmd_done m_digifin_cmd_error m_digifin_leakdetect_reading m_digifin_motorstep_counter m_digifin_resp_data m_digifin_status m_disk_free m_disk_usage m_dist_to_wpt m_dive_depth m_dive_tot_time m_dr_fix_time m_dr_postfix_time m_dr_surf_x_lmc m_dr_surf_y_lmc m_dr_time m_dr_x_actual_err m_dr_x_ini_err m_dr_x_postfix_drift m_dr_x_ta_postfix_drift m_dr_y_actual_err m_dr_y_ini_err m_dr_y_postfix_drift m_dr_y_ta_postfix_drift m_est_time_to_surface m_fin m_final_water_vx m_final_water_vy m_fin_vel m_fluid_pumped m_fluid_pumped_aft_hall_voltage m_fluid_pumped_fwd_hall_voltage m_fluid_pumped_vel m_free_heap m_gps_dist_from_dr m_gps_fix_x_lmc m_gps_fix_y_lmc m_gps_full_status m_gps_heading m_gps_ignored_lat m_gps_ignored_lon m_gps_invalid_lat m_gps_invalid_lon m_gps_lat m_gps_lon m_gps_mag_var m_gps_num_satellites m_gps_on m_gps_postfix_x_lmc m_gps_postfix_y_lmc m_gps_speed m_gps_status m_gps_toofar_lat m_gps_toofar_lon m_gps_uncertainty m_gps_utc_day m_gps_utc_hour m_gps_utc_minute m_gps_utc_month m_gps_utc_second m_gps_utc_year m_gps_x_lmc m_gps_y_lmc m_hdg_derror m_hdg_error m_hdg_ierror m_hdg_rate m_heading m_initial_water_vx m_initial_water_vy m_iridium_attempt_num m_iridium_call_num m_iridium_connected m_iridium_console_on m_iridium_dialed_num m_iridium_on m_iridium_redials m_iridium_signal_strength m_iridium_status m_iridium_waiting_redial_delay m_iridium_waiting_registration m_is_ballast_pump_moving m_is_battpos_moving m_is_battroll_moving m_is_de_pump_moving m_is_fin_moving m_is_fpitch_pump_moving m_is_speed_estimated m_is_thermal_valve_moving m_last_yo_time m_lat m_leak m_leakdetect_voltage m_leakdetect_voltage_forward m_leak_forward m_lithium_battery_relative_charge m_lithium_battery_status m_lithium_battery_time_to_charge m_lithium_battery_time_to_discharge m_lon m_min_free_heap m_min_spare_heap m_mission_avg_speed_climbing m_mission_avg_speed_diving m_mission_start_time m_num_half_yos_in_segment m_pitch m_pitch_energy m_pitch_error m_present_secs_into_mission m_present_time m_pressure m_pressure_raw_voltage_sample0 m_pressure_raw_voltage_sample19 m_pressure_voltage m_raw_altitude m_raw_altitude_rejected m_roll m_science_clothesline_lag m_science_on m_science_ready_for_consci m_science_sent_some_data m_science_sync_time m_science_unreadiness_for_consci m_spare_heap m_speed m_stable_comms m_strobe_ctrl m_surface_est_cmd m_surface_est_ctd m_surface_est_fw m_surface_est_gps m_surface_est_irid m_surface_est_total m_system_clock_lags_gps m_tcm3_is_calibrated m_tcm3_magbearth m_tcm3_poll_time m_tcm3_recv_start_time m_tcm3_recv_stop_time m_tcm3_stddeverr m_tcm3_xcoverage m_tcm3_ycoverage m_tcm3_zcoverage m_thermal_acc_pres m_thermal_acc_pres_voltage m_thermal_acc_vol m_thermal_enuf_acc_vol m_thermal_pump m_thermal_updown m_thermal_valve m_time_til_wpt m_tot_ballast_pumped_energy m_tot_horz_dist m_tot_num_inflections m_tot_on_time m_vacuum m_vehicle_temp m_veh_overheat m_veh_temp m_vmg_to_wpt m_vx_lmc m_vy_lmc m_water_cond m_water_delta_vx m_water_delta_vy m_water_depth m_water_pressure m_water_temp m_water_vx m_water_vy m_why_started m_x_lmc m_y_lmc x_last_wpt_lat x_last_wpt_lon x_system_clock_adjusted sci_bsipar_is_installed sci_bsipar_par sci_bsipar_sensor_volts sci_bsipar_supply_volts sci_bsipar_temp sci_bsipar_timestamp sci_ctd41cp_is_installed sci_ctd41cp_timestamp sci_dvl_bd_range_to_bottom sci_dvl_bd_time_since_last_good_vel sci_dvl_bd_u_dist sci_dvl_bd_v_dist sci_dvl_bd_w_dist sci_dvl_be_u_vel sci_dvl_be_v_vel sci_dvl_be_vel_good sci_dvl_be_w_vel sci_dvl_bi_err_vel sci_dvl_bi_vel_good sci_dvl_bi_x_vel sci_dvl_bi_y_vel sci_dvl_bi_z_vel sci_dvl_bs_longitudinal_vel sci_dvl_bs_normal_vel sci_dvl_bs_transverse_vel sci_dvl_bs_vel_good sci_dvl_ensemble_offset sci_dvl_error sci_dvl_is_installed sci_dvl_sa_heading sci_dvl_sa_pitch sci_dvl_sa_roll sci_dvl_ts_bit sci_dvl_ts_depth sci_dvl_ts_sal sci_dvl_ts_sound_speed sci_dvl_ts_temp sci_dvl_ts_timestamp sci_dvl_wd_range_to_water_mass_center sci_dvl_wd_time_since_last_good_vel sci_dvl_wd_u_dist sci_dvl_wd_v_dist sci_dvl_wd_w_dist sci_dvl_we_u_vel sci_dvl_we_v_vel sci_dvl_we_vel_good sci_dvl_we_w_vel sci_dvl_wi_err_vel sci_dvl_wi_vel_good sci_dvl_wi_x_vel sci_dvl_wi_y_vel sci_dvl_wi_z_vel sci_dvl_ws_longitudinal_vel sci_dvl_ws_normal_vel sci_dvl_ws_transverse_vel sci_dvl_ws_vel_good sci_flbbcd_bb_ref sci_flbbcd_bb_sig sci_flbbcd_bb_units sci_flbbcd_cdom_ref sci_flbbcd_cdom_sig sci_flbbcd_cdom_units sci_flbbcd_chlor_ref sci_flbbcd_chlor_sig sci_flbbcd_chlor_units sci_flbbcd_is_installed sci_flbbcd_therm sci_flbbcd_timestamp sci_m_disk_free sci_m_disk_usage sci_m_free_heap sci_m_min_free_heap sci_m_min_spare_heap sci_m_present_secs_into_mission sci_m_present_time sci_m_science_on sci_m_spare_heap sci_oxy3835_is_installed sci_oxy3835_oxygen sci_oxy3835_saturation sci_oxy3835_temp sci_oxy3835_timestamp sci_reqd_heartbeat sci_software_ver sci_wants_comms sci_wants_surface sci_water_cond sci_water_pressure sci_water_temp sci_x_disk_files_removed sci_x_sent_data_files x_low_power_status
enum cc in rad sec cc sec sec rad sec rad sec lat lon bool m bool bool enum volts m m/s bool bool bool bool timestamp m bool bool m/s m/s m/s sec m/s sec sec sec cc joules cc/sec volts volts in in/sec rad rad/sec bool bool nodim nodim nodim s bool bool bool amp-hrs nodim amp-hrs amp nodim nodim m m/s m/s m/s enum m/s bool enum m nodim nodim nodim nodim cc volts nodim nodim nodim nodim nodim nodim nodim Mbytes Mbytes m m s sec sec m m sec m m m m m m m m sec rad m/s m/s rad/sec cc volts volts cc/sec bytes m m m enum rad lat lon lat lon lat lon rad nodim bool m m m/s enum lat lat nodim byte byte byte byte nodim byte m m rad/sec rad rad-sec rad/sec rad m/s m/s nodim nodim bool enum nodim bool nodim nodim enum bool bool bool bool bool bool bool bool bool bool sec lat bool volts volts bool % nodim mins mins lon bytes bytes m/s m/s timestamp nodim rad joules rad sec timestamp bar volts volts volts m bool rad s bool bool nodim timestamp enum bytes m/s bool bool nodim nodim nodim nodim nodim nodim sec bool uT ms ms ms uT % % % bar volts cc bool enum enum enum s kjoules km nodim days inHg degC bool c m/s m/s m/s S/m m/s m/s m bar degC m/s m/s enum m m lat lon sec bool ue/m^2sec volts volts degc timestamp bool timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim bool deg deg deg nodim m ppt m/s degc timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim nodim nodim nodim ppb nodim nodim ug/l bool nodim timestamp mbytes mbytes bytes bytes bytes sec timestamp bool bytes bool nodim nodim nodim timestamp secs nodim bool enum s/m bar degc nodim nodim volts
1 4 4 4 4 4 4 4 4 4 4 4 8 8 1 4 1 1 1 4 4 4 1 1 1 1 8 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 1 4 4 4 4 1 1 1 4 4 4 4 4 4 4 4 4 4 1 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 4 8 8 8 8 8 8 4 4 1 4 4 4 1 8 8 4 1 1 1 1 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 4 1 4 4 1 1 1 1 1 1 1 1 1 1 1 4 8 1 4 4 1 4 4 4 4 8 4 4 4 4 8 4 4 4 4 4 8 4 4 4 4 4 1 4 4 1 1 4 8 1 4 4 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 4 1 4 4 8 8 4 1 4 4 4 4 8 1 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 4 4 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 1 4 8 4 4 4 4 4 4 8 1 4 1 4 4 4 8 4 4 1 1 4 4 4 4 4 4 """

HEADER5 = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: unit_247-2012-051-0-0-sf
the8x3_filename: 01840000
filename_extension: dbd
filename_label: unit_247-2012-051-0-0-dbd(01840000)
mission_name: ENDUR1.MI
fileopen_time: Tue_Feb_21_18:39:39_2012
sensors_per_cycle: 354
num_label_lines: 3
num_segments: 1
segment_filename_0: unit_247-2012-051-0-0
c_air_pump c_ballast_pumped c_battpos c_battroll c_bsipar_on c_de_oil_vol c_dvl_on c_flbbcd_on c_heading c_oxy3835_wphase_on c_pitch c_profile_on c_wpt_lat c_wpt_lon m_1meg_persistor m_aground_water_depth m_air_fill m_air_pump m_altimeter_status m_altimeter_voltage m_altitude m_altitude_rate m_appear_to_be_at_surface m_argos_is_xmitting m_argos_on m_argos_sent_data m_argos_timestamp m_at_risk_depth m_avbot_enable m_avbot_power m_avg_climb_rate m_avg_depth_rate m_avg_dive_rate m_avg_downward_inflection_time m_avg_speed m_avg_system_clock_lags_gps m_avg_upward_inflection_time m_avg_yo_time m_ballast_pumped m_ballast_pumped_energy m_ballast_pumped_vel m_battery m_battery_inst m_battpos m_battpos_vel m_battroll m_battroll_vel m_bpump_fault_bit m_certainly_at_surface m_chars_tossed_by_abend m_chars_tossed_with_cd_off m_chars_tossed_with_power_off m_climb_tot_time m_console_cd m_console_on m_cop_tickle m_coulomb_amphr m_coulomb_amphr_raw m_coulomb_amphr_total m_coulomb_current m_coulomb_current_raw m_cycle_number m_depth m_depth_rate m_depth_rate_avg_final m_depth_rate_running_avg m_depth_rate_running_avg_n m_depth_rate_subsampled m_depth_rejected m_depth_state m_depth_subsampled m_device_drivers_called_abnormally m_device_error m_device_oddity m_device_warning m_de_oil_vol m_de_oil_vol_pot_voltage m_de_pump_fault_count m_digifin_cmd_done m_digifin_cmd_error m_digifin_leakdetect_reading m_digifin_motorstep_counter m_digifin_resp_data m_digifin_status m_disk_free m_disk_usage m_dist_to_wpt m_dive_depth m_dive_tot_time m_dr_fix_time m_dr_postfix_time m_dr_surf_x_lmc m_dr_surf_y_lmc m_dr_time m_dr_x_actual_err m_dr_x_ini_err m_dr_x_postfix_drift m_dr_x_ta_postfix_drift m_dr_y_actual_err m_dr_y_ini_err m_dr_y_postfix_drift m_dr_y_ta_postfix_drift m_est_time_to_surface m_fin m_final_water_vx m_final_water_vy m_fin_vel m_fluid_pumped m_fluid_pumped_aft_hall_voltage m_fluid_pumped_fwd_hall_voltage m_fluid_pumped_vel m_free_heap m_gps_dist_from_dr m_gps_fix_x_lmc m_gps_fix_y_lmc m_gps_full_status m_gps_heading m_gps_ignored_lat m_gps_ignored_lon m_gps_invalid_lat m_gps_invalid_lon m_gps_lat m_gps_lon m_gps_mag_var m_gps_num_satellites m_gps_on m_gps_postfix_x_lmc m_gps_postfix_y_lmc m_gps_speed m_gps_status m_gps_toofar_lat m_gps_toofar_lon m_gps_uncertainty m_gps_utc_day m_gps_utc_hour m_gps_utc_minute m_gps_utc_month m_gps_utc_second m_gps_utc_year m_gps_x_lmc m_gps_y_lmc m_hdg_derror m_hdg_error m_hdg_ierror m_hdg_rate m_heading m_initial_water_vx m_initial_water_vy m_iridium_attempt_num m_iridium_call_num m_iridium_connected m_iridium_console_on m_iridium_dialed_num m_iridium_on m_iridium_redials m_iridium_signal_strength m_iridium_status m_iridium_waiting_redial_delay m_iridium_waiting_registration m_is_ballast_pump_moving m_is_battpos_moving m_is_battroll_moving m_is_de_pump_moving m_is_fin_moving m_is_fpitch_pump_moving m_is_speed_estimated m_is_thermal_valve_moving m_last_yo_time m_lat m_leak m_leakdetect_voltage m_leakdetect_voltage_forward m_leak_forward m_lithium_battery_relative_charge m_lithium_battery_status m_lithium_battery_time_to_charge m_lithium_battery_time_to_discharge m_lon m_min_free_heap m_min_spare_heap m_mission_avg_speed_climbing m_mission_avg_speed_diving m_mission_start_time m_num_half_yos_in_segment m_pitch m_pitch_energy m_pitch_error m_present_secs_into_mission m_present_time m_pressure m_pressure_raw_voltage_sample0 m_pressure_raw_voltage_sample19 m_pressure_voltage m_raw_altitude m_raw_altitude_rejected m_roll m_science_clothesline_lag m_science_on m_science_ready_for_consci m_science_sent_some_data m_science_sync_time m_science_unreadiness_for_consci m_spare_heap m_speed m_stable_comms m_strobe_ctrl m_surface_est_cmd m_surface_est_ctd m_surface_est_fw m_surface_est_gps m_surface_est_irid m_surface_est_total m_system_clock_lags_gps m_tcm3_is_calibrated m_tcm3_magbearth m_tcm3_poll_time m_tcm3_recv_start_time m_tcm3_recv_stop_time m_tcm3_stddeverr m_tcm3_xcoverage m_tcm3_ycoverage m_tcm3_zcoverage m_thermal_acc_pres m_thermal_acc_pres_voltage m_thermal_acc_vol m_thermal_enuf_acc_vol m_thermal_pump m_thermal_updown m_thermal_valve m_time_til_wpt m_tot_ballast_pumped_energy m_tot_horz_dist m_tot_num_inflections m_tot_on_time m_vacuum m_vehicle_temp m_veh_overheat m_veh_temp m_vmg_to_wpt m_vx_lmc m_vy_lmc m_water_cond m_water_delta_vx m_water_delta_vy m_water_depth m_water_pressure m_water_temp m_water_vx m_water_vy m_why_started m_x_lmc m_y_lmc x_last_wpt_lat x_last_wpt_lon x_system_clock_adjusted sci_bsipar_is_installed sci_bsipar_par sci_bsipar_sensor_volts sci_bsipar_supply_volts sci_bsipar_temp sci_bsipar_timestamp sci_ctd41cp_is_installed sci_ctd41cp_timestamp sci_dvl_bd_range_to_bottom sci_dvl_bd_time_since_last_good_vel sci_dvl_bd_u_dist sci_dvl_bd_v_dist sci_dvl_bd_w_dist sci_dvl_be_u_vel sci_dvl_be_v_vel sci_dvl_be_vel_good sci_dvl_be_w_vel sci_dvl_bi_err_vel sci_dvl_bi_vel_good sci_dvl_bi_x_vel sci_dvl_bi_y_vel sci_dvl_bi_z_vel sci_dvl_bs_longitudinal_vel sci_dvl_bs_normal_vel sci_dvl_bs_transverse_vel sci_dvl_bs_vel_good sci_dvl_ensemble_offset sci_dvl_error sci_dvl_is_installed sci_dvl_sa_heading sci_dvl_sa_pitch sci_dvl_sa_roll sci_dvl_ts_bit sci_dvl_ts_depth sci_dvl_ts_sal sci_dvl_ts_sound_speed sci_dvl_ts_temp sci_dvl_ts_timestamp sci_dvl_wd_range_to_water_mass_center sci_dvl_wd_time_since_last_good_vel sci_dvl_wd_u_dist sci_dvl_wd_v_dist sci_dvl_wd_w_dist sci_dvl_we_u_vel sci_dvl_we_v_vel sci_dvl_we_vel_good sci_dvl_we_w_vel sci_dvl_wi_err_vel sci_dvl_wi_vel_good sci_dvl_wi_x_vel sci_dvl_wi_y_vel sci_dvl_wi_z_vel sci_dvl_ws_longitudinal_vel sci_dvl_ws_normal_vel sci_dvl_ws_transverse_vel sci_dvl_ws_vel_good sci_flbbcd_bb_ref sci_flbbcd_bb_sig sci_flbbcd_bb_units sci_flbbcd_cdom_ref sci_flbbcd_cdom_sig sci_flbbcd_cdom_units sci_flbbcd_chlor_ref sci_flbbcd_chlor_sig sci_flbbcd_chlor_units sci_flbbcd_is_installed sci_flbbcd_therm sci_flbbcd_timestamp sci_m_disk_free sci_m_disk_usage sci_m_free_heap sci_m_min_free_heap sci_m_min_spare_heap sci_m_present_secs_into_mission sci_m_present_time sci_m_science_on sci_m_spare_heap sci_oxy3835_is_installed sci_oxy3835_oxygen sci_oxy3835_saturation sci_oxy3835_temp sci_oxy3835_timestamp sci_reqd_heartbeat sci_software_ver sci_wants_comms sci_wants_surface sci_water_cond sci_water_pressure sci_water_temp sci_x_disk_files_removed sci_x_sent_data_files sci_flbb_timestamp sci_flbb_bb_ref sci_flbb_bb_sig sci_flbb_bb_units sci_flbb_chlor_ref sci_flbb_chlor_sig sci_flbb_chlor_units sci_flbb_therm
enum cc in rad sec cc sec sec rad sec rad sec lat lon bool m bool bool enum volts m m/s bool bool bool bool timestamp m bool bool m/s m/s m/s sec m/s sec sec sec cc joules cc/sec volts volts in in/sec rad rad/sec bool bool nodim nodim nodim s bool bool bool amp-hrs nodim amp-hrs amp nodim nodim m m/s m/s m/s enum m/s bool enum m nodim nodim nodim nodim cc volts nodim nodim nodim nodim nodim nodim nodim Mbytes Mbytes m m s sec sec m m sec m m m m m m m m sec rad m/s m/s rad/sec cc volts volts cc/sec bytes m m m enum rad lat lon lat lon lat lon rad nodim bool m m m/s enum lat lat nodim byte byte byte byte nodim byte m m rad/sec rad rad-sec rad/sec rad m/s m/s nodim nodim bool enum nodim bool nodim nodim enum bool bool bool bool bool bool bool bool bool bool sec lat bool volts volts bool % nodim mins mins lon bytes bytes m/s m/s timestamp nodim rad joules rad sec timestamp bar volts volts volts m bool rad s bool bool nodim timestamp enum bytes m/s bool bool nodim nodim nodim nodim nodim nodim sec bool uT ms ms ms uT % % % bar volts cc bool enum enum enum s kjoules km nodim days inHg degC bool c m/s m/s m/s S/m m/s m/s m bar degC m/s m/s enum m m lat lon sec bool ue/m^2sec volts volts degc timestamp bool timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim bool deg deg deg nodim m ppt m/s degc timestamp m sec m m m mm/s mm/s bool mm/s mm/s bool mm/s mm/s mm/s mm/s mm/s mm/s bool nodim nodim nodim nodim nodim ppb nodim nodim ug/l bool nodim timestamp mbytes mbytes bytes bytes bytes sec timestamp bool bytes bool nodim nodim nodim timestamp secs nodim bool enum s/m bar degc nodim nodim seconds 1 1 1 1 1 ug/L 1
1 4 4 4 4 4 4 4 4 4 4 4 8 8 1 4 1 1 1 4 4 4 1 1 1 1 8 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 1 4 4 4 4 1 1 1 4 4 4 4 4 4 4 4 4 4 1 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 1 4 8 8 8 8 8 8 4 4 1 4 4 4 1 8 8 4 1 1 1 1 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 4 1 4 4 1 1 1 1 1 1 1 1 1 1 1 4 8 1 4 4 1 4 4 4 4 8 4 4 4 4 8 4 4 4 4 4 8 4 4 4 4 4 1 4 4 1 1 4 8 1 4 4 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 1 1 1 1 4 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 4 4 4 1 4 4 8 8 4 1 4 4 4 4 8 1 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 4 4 8 4 4 4 4 4 4 4 1 4 4 1 4 4 4 4 4 4 1 4 4 4 4 4 4 4 4 4 1 4 8 4 4 4 4 4 4 8 1 4 1 4 4 4 8 4 4 1 1 4 4 4 4 4 4 4 4 4 4 4 4 4 """

HEADER6 = """dbd_label: DBD_ASC(dinkum_binary_data_ascii)file
encoding_ver: 2
num_ascii_tags: 14
all_sensors: 0
filename: gi_528-2015-228-3-0
the8x3_filename: 00540000
filename_extension: sbd
filename_label: gi_528-2015-228-3-0-sbd(00540000)
mission_name: INI0.MI
fileopen_time: Mon_Aug_17_14:45:23_2015
sensors_per_cycle: 40
num_label_lines: 3
num_segments: 1
segment_filename_0: gi_528-2015-228-3-0
c_battpos c_wpt_lat c_wpt_lon m_battpos m_coulomb_amphr_total m_coulomb_current m_depth m_de_oil_vol m_gps_lat m_gps_lon m_lat m_leakdetect_voltage m_leakdetect_voltage_forward m_lon m_pitch m_present_secs_into_mission m_present_time m_speed m_water_vx m_water_vy x_low_power_status sci_bb3slo_b470_sig sci_bb3slo_b532_sig sci_bb3slo_b660_sig sci_bb3slo_temp sci_bsipar_par sci_flbbcd_bb_sig sci_flbbcd_cdom_sig sci_flbbcd_chlor_sig sci_m_present_secs_into_mission sci_m_present_time sci_oxy4_oxygen sci_oxy4_saturation sci_oxy4_temp sci_suna_nitrate_mg sci_suna_nitrate_um sci_suna_record_offset sci_water_cond sci_water_pressure sci_water_temp
in lat lon in amp-hrs amp m cc lat lon lat volts volts lon rad sec timestamp m/s m/s m/s nodim nodim nodim nodim nodim ue/m^2sec nodim nodim nodim sec timestamp um % degc mg/l umol/l bytes s/m bar degc
4 8 8 4 4 4 4 4 8 8 8 4 4 8 4 4 8 4 4 4 4 4 4 4 4 4 4 4 4 4 8 4 4 4 4 4 4 4 4 4 """

FLORD_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 153.928 1329849722.92795 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 664.424 0.401911 10.572 10.25 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 700 139 0.000281336 460 72 2.0352 695 114 0.8349 NaN 560 1000.1 NaN NaN NaN NaN NaN 153.928 1329849722.92795 NaN NaN NaN 266.42 93.49 9.48 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 700 139 0.000281 695 114 0.8349 560
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 154.944 1329849723.94394 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 645.569 0.390792 10.572 10.25 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 892 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 700 133 0.000262988 460 73 2.12 695 115 0.847 NaN 559 1000.1 NaN NaN NaN NaN NaN 154.944 1329849723.94394 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 700 133 0.000263 695 115 0.847 559"""

ENGSCI_RECORD = """
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 4330 -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6873 10.7871 0.703717 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9937 -9944 303.803 0.485094 -1634 0 0.258982 0 0.00472497 0 0 0.00136254 0 0 0.258982 8 6 21 6 259.77 1.43611 0 0 0 1022 6 0 4194300 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0 -0.0616963 -0.144984 0 0 0 0 0 304128 0.916352 0 0 0 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 4328.2683 -12523.7965 -0.279253 11 0 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 1 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 4328.26830007145 0 2.46526 2.45955 0 57.8052 0 0 0 -12523.7965000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 102687000 -0.0426476 0 1329849569.26294 0.0258982 0 0 0.137179 16.967 1 -0.10821 32.0756 0 0 1371 1329849561.95532 1 284672 0.348396 1 0 1 0 0 7.58463e-23 1 2 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 10.0444 0 0 13.1124 -0.283741 0.300996 -0.0683846 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 40389 -1904.23 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 1000.1 NaN NaN NaN 1000.1 1000.1 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 4330 -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6806 10.6208 0.695632 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9972 -9947 303.806 0.0955938 -322 1 0.148777 0 0.00472497 0 0 0.00136254 0 0 0.258982 3 6 21 6 259.742 1.43605 0 0 0 1023 3 0 4194310 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0.0127162 -0.0616963 -0.144984 0 0 0 0 0 324608 0.916352 0 0 7 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 4328.2683 -12523.7965 -0.279253 11 1 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 0 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 4328.26830007145 0 2.46386 2.45876 0 57.8047 0 0 0 -12523.7965000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 115832000 -0.0426476 49.646 1329849618.79962 0.0148777 0 0 0.137057 16.967 1 -0.10821 32.0756 1 0 59 1329849561.95532 1 283648 0.348396 0 0 1 0 0 6.63787e-23 0.875173 1.87517 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 7.84544 0 0 13.1954 -0.283741 0 0 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 0 0 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.2 1000.2 NaN NaN NaN 1000.2 1000.2 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 """

ENGSCI_RECORD_69 = """
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 4330 -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6873 10.7871 0.703717 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9937 -9944 303.803 0.485094 -1634 0 0.258982 0 0.00472497 0 0 0.00136254 0 0 0.258982 8 6 21 6 259.77 1.43611 0 0 0 1022 6 0 4194300 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0 -0.0616963 -0.144984 0 0 0 0 0 304128 0.916352 0 0 0 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 69696969 69696969 -0.279253 11 0 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 1 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 696969690007145 0 2.46526 2.45955 0 57.8052 0 0 0 69696969000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 102687000 -0.0426476 0 1329849569.26294 0.0258982 0 0 0.137179 16.967 1 -0.10821 32.0756 0 0 1371 1329849561.95532 1 284672 0.348396 1 0 1 0 0 7.58463e-23 1 2 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 10.0444 0 0 13.1124 -0.283741 0.300996 -0.0683846 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 40389 -1904.23 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 1000.1 NaN NaN NaN 1000.1 1000.1 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 4330 -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6806 10.6208 0.695632 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9972 -9947 303.806 0.0955938 -322 1 0.148777 0 0.00472497 0 0 0.00136254 0 0 0.258982 3 6 21 6 259.742 1.43605 0 0 0 1023 3 0 4194310 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0.0127162 -0.0616963 -0.144984 0 0 0 0 0 324608 0.916352 0 0 7 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 69696969 69696969 -0.279253 11 1 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 0 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 696969690007145 0 2.46386 2.45876 0 57.8047 0 0 0 69696969000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 115832000 -0.0426476 49.646 1329849618.79962 0.0148777 0 0 0.137057 16.967 1 -0.10821 32.0756 1 0 59 1329849561.95532 1 283648 0.348396 0 0 1 0 0 6.63787e-23 0.875173 1.87517 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 7.84544 0 0 13.1954 -0.283741 0 0 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 0 0 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.2 1000.2 NaN NaN NaN 1000.2 1000.2 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 """

ENGSCI_BAD_LAT_RECORD = """
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 433X -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6873 10.7871 0.703717 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9937 -9944 303.803 0.485094 -1634 0 0.258982 0 0.00472497 0 0 0.00136254 0 0 0.258982 8 6 21 6 259.77 1.43611 0 0 0 1022 6 0 4194300 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0 -0.0616963 -0.144984 0 0 0 0 0 304128 0.916352 0 0 0 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 4328.2683 -12523.7965 -0.279253 11 0 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 1 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 4328.26830007145 0 2.46526 2.45955 0 57.8052 0 0 0 -12523.7965000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 102687000 -0.0426476 0 1329849569.26294 0.0258982 0 0 0.137179 16.967 1 -0.10821 32.0756 0 0 1371 1329849561.95532 1 284672 0.348396 1 0 1 0 0 7.58463e-23 1 2 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 10.0444 0 0 13.1124 -0.283741 0.300996 -0.0683846 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 40389 -1904.23 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 1000.1 NaN NaN NaN 1000.1 1000.1 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1
1 260 0.7 0 -1 260 -1 -1 4.96727 -1 0.4528 -1 30 -12600 0 -1 1 1 2 2.44548 12.9695 -0.219681 1 0 1 0 1329843706.03265 1196.2 0 0 -0.183911 0.00699798 0.166781 155.923 0.379813 0.55692 124.082 4971.02 0 0 0 10.6806 10.6208 0.695632 0.141578 0 0 0 1 59 1 1 -1 0 1 1 40.9972 -9947 303.806 0.0955938 -322 1 0.148777 0 0.00472497 0 0 0.00136254 0 0 0.258982 3 6 21 6 259.742 1.43605 0 0 0 1023 3 0 4194310 1781.12 219.812 48926.2 -1 -1 -1 -1 0 0 -1 0 0 0 0 0 0 0 0 43.0556 0.0127162 -0.0616963 -0.144984 0 0 0 0 0 324608 0.916352 0 0 7 1.7942 4328.2816 -12523.8141 4328.2925 -12523.8189 4328.2683 -12523.7965 -0.279253 11 1 0 0 0.308667 0 4328.6173 -12513.3557 0.9 21 18 3 2 35 12 40389 -1904.23 0.0197767 0.11338 0.120462 -0.0173492 5.05447 -0.0616291 -0.145094 0 518 0 0 3323 0 0 5 99 0 0 0 0 0 0 0 0 0 0 4756.23 4328.26830007145 0 2.46386 2.45876 0 57.8047 0 0 0 -12523.7965000589 289792 270336 0.430413 0.350943 1329849569 0 0.518363 115832000 -0.0426476 49.646 1329849618.79962 0.0148777 0 0 0.137057 16.967 1 -0.10821 32.0756 1 0 59 1329849561.95532 1 283648 0.348396 0 0 1 0 0 6.63787e-23 0.875173 1.87517 1 0 -1 0 0 0 -1 -1 -1 -1 0 0 0 0 0 3 2 -172433 0.74206 605.857 3115 5.06637 7.84544 0 0 13.1954 -0.283741 0 0 3 -0.0218157 0.0107268 -1 49.141 10 -0.0616963 -0.144984 16 0 0 4330 -12600 -12 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.2 1000.2 NaN NaN NaN 1000.2 1000.2 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1000.1 """

FLORT_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 153.928 1329849722.92795 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 664.424 0.401911 10.572 10.25 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 700 139 0.000281336 460 72 2.0352 695 114 0.8349 NaN 560 NaN NaN NaN NaN NaN NaN 153.928 1329849722.92795 NaN NaN NaN 266.42 93.49 9.48 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 154.944 1329849723.94394 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 645.569 0.390792 10.572 10.25 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 892 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 700 133 0.000262988 460 73 2.12 695 115 0.847 NaN 559 NaN NaN NaN NaN NaN NaN 154.944 1329849723.94394 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN """

FLORT_O_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1783.96 1439822650.9566 NaN NaN NaN NaN 70 101 169 550 77399.4 4004 134 2903 1783.96 1439822650.9566 261.501 93.736 10.508 0.0795487 5.67933 0 -2e-05 0 11.1774
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 221.447 1439822869.44687 NaN NaN NaN NaN 148 217 4047 556 738528000 280 60 95 221.447 1439822869.44687 NaN NaN NaN NaN NaN NaN NaN NaN NaN """

EMPTY_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN """

ZERO_GPS_VALUE = """
NaN 0 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN """

INT_GPS_VALUE = """
NaN 2012 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN """

CTDGV_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 121147 1378349241.82962 NaN NaN NaN NaN NaN NaN 121147 1378349241.82962 NaN NaN 4.03096 0.021 15.3683
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 121207 1378349302.10907 NaN NaN NaN NaN NaN NaN 121207 1378349302.10907 NaN NaN 4.03113 0.093 15.3703 """

DOSTA_RECORD = """
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 121144 1378349238.77789 NaN NaN NaN NaN NaN NaN 121144 1378349238.77789 242.217 96.009 NaN NaN NaN
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 121204 1378349299.09106 NaN NaN NaN NaN NaN NaN 121204 1378349299.09106 242.141 95.988 NaN NaN NaN """

ENG_RECORD = """
0.273273 NaN NaN 0.335 149.608 0.114297 33.9352 -64.3506 NaN NaN NaN 5011.38113678061 -14433.5809717525 NaN 121546 1378349641.79871 NaN NaN NaN 0 NaN NaN NaN NaN NaN NaN NaN NaN NaN
NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN 1.23569 NaN NaN -0.0820305 121379 1378349475.09927 0.236869 NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN """


@attr('UNIT', group='mi')
class GliderParserUnitTestCase(ParserUnitTestCase):
    """
    Glider Parser unit test base class and common tests.
    """
    config = {}

    def set_data(self, *args):
        """
        Accept strings of data in args[] joined together and then a file handle
        to the concatenated string is returned.
        """
        io = StringIO()
        for count, data in enumerate(args):
            io.write(data)

        # log.debug("Test data file: %s", io.getvalue())
        io.seek(0)
        self.test_data = io

    def assert_no_more_data(self):
        """
        Verify we don't find any other records in the data file.
        """
        records = self.parser.get_records(1)
        self.assertEqual(len(records), 0)

    def assert_generate_particle(self, particle_type, values_dict=None):
        """
        Verify that we can generate a particle of the correct type and that
        the state is set properly.
        @param values_dict key value pairs to test in the particle.
        """

        records = self.parser.get_records(1)

        self.assertIsNotNone(records)
        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 1)

        self.assert_type(records, particle_type)

        # Verify the data
        if values_dict:
            self.assert_particle_values(records[0], values_dict)

        return records

    def assert_particle_values(self, particle, expected_values):
        """
        Verify the data in expected values is the data in the particle
        """
        data_dict = particle.generate_dict()
        log.debug("Data in particle: %s", data_dict)
        log.debug("Expected Data: %s", expected_values)

        for key in expected_values.keys():
            for value in data_dict['values']:
                if value['value_id'] == key:
                    self.assertEqual(value['value'], expected_values[key])

    def assert_type(self, records, particle_class):
        for particle in records:
            str_of_type = particle.data_particle_type()
            self.assertEqual(particle_class._data_particle_type, str_of_type)


@attr('UNIT', group='mi')
class CtdgvTelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for ctdgv glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdgvTelemeteredDataParticle'
    }

    def test_ctdgv_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER, CTDGV_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {CtdgvParticleKey.SCI_WATER_TEMP: 15.3683, CtdgvParticleKey.SCI_WATER_COND: 4.03096,
                    CtdgvParticleKey.SCI_WATER_PRESSURE: 0.021}
        record_2 = {CtdgvParticleKey.SCI_WATER_TEMP: 15.3703, CtdgvParticleKey.SCI_WATER_COND: 4.03113,
                    CtdgvParticleKey.SCI_WATER_PRESSURE: 0.093}

        self.assert_generate_particle(CtdgvTelemeteredDataParticle, record_1)
        self.assert_generate_particle(CtdgvTelemeteredDataParticle, record_2)
        self.assert_no_more_data()

    def test_gps(self):
        self.set_data(HEADER, ZERO_GPS_VALUE)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)
        records = self.parser.get_records(1)
        self.assertEqual(len(records), 0)

        self.set_data(HEADER, INT_GPS_VALUE)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)
        records = self.parser.get_records(1)
        self.assertEqual(len(records), 0)

    def test_single_yml(self):
        """
        Test with a yml file with a single record
        """
        with open(os.path.join(CTDGV_RESOURCE_PATH, 'single_ctdgv_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(1)
            self.assert_particles(record, 'single_ctdgv_record.mrg.result.yml', CTDGV_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_multiple_yml(self):
        """
        Test with a yml file with multiple records
        """
        with open(os.path.join(CTDGV_RESOURCE_PATH, 'multiple_ctdgv_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(4)
            self.assert_particles(record, 'multiple_ctdgv_record.mrg.result.yml', CTDGV_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_real(self):
        """
        Test with several real files and confirm no exceptions occur
        """
        with open(os.path.join(CTDGV_RESOURCE_PATH, 'unit_363_2013_199_0_0.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(1107)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])

        with open(os.path.join(CTDGV_RESOURCE_PATH, 'unit_363_2013_199_5_0.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(108)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])

        with open(os.path.join(CTDGV_RESOURCE_PATH, 'unit_363_2013_245_6_6.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(240)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])

        with open(os.path.join(CTDGV_RESOURCE_PATH, 'unit_364_2013_192_1_0.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(4)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class CtdgvRecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for ctdgv glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdgvRecoveredDataParticle'
    }

    def test_ctdgv_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER, CTDGV_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {CtdgvParticleKey.SCI_WATER_TEMP: 15.3683, CtdgvParticleKey.SCI_WATER_COND: 4.03096,
                    CtdgvParticleKey.SCI_WATER_PRESSURE: 0.021}
        record_2 = {CtdgvParticleKey.SCI_WATER_TEMP: 15.3703, CtdgvParticleKey.SCI_WATER_COND: 4.03113,
                    CtdgvParticleKey.SCI_WATER_PRESSURE: 0.093}

        self.assert_generate_particle(CtdgvRecoveredDataParticle, record_1)
        self.assert_generate_particle(CtdgvRecoveredDataParticle, record_2)
        self.assert_no_more_data()

    def test_gps(self):
        self.set_data(HEADER, ZERO_GPS_VALUE)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)
        records = self.parser.get_records(1)
        self.assertEqual(len(records), 0)

        self.set_data(HEADER, INT_GPS_VALUE)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)
        records = self.parser.get_records(1)
        self.assertEqual(len(records), 0)


@attr('UNIT', group='mi')
class DOSTATelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for dosta glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaTelemeteredDataParticle'
    }

    def test_dosta_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER, DOSTA_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {DostaTelemeteredParticleKey.SCI_OXY4_OXYGEN: 242.217,
                    DostaTelemeteredParticleKey.SCI_OXY4_SATURATION: 96.009}
        record_2 = {DostaTelemeteredParticleKey.SCI_OXY4_OXYGEN: 242.141,
                    DostaTelemeteredParticleKey.SCI_OXY4_SATURATION: 95.988}

        self.assert_generate_particle(DostaTelemeteredDataParticle, record_1)
        self.assert_generate_particle(DostaTelemeteredDataParticle, record_2)
        self.assert_no_more_data()

    def test_multiple_yml(self):
        """
        Test with a yml file with a multiple records
        """

        with open(os.path.join(DOSTA_RESOURCE_PATH, 'multiple_dosta_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(4)
            self.assert_particles(record, 'multiple_dosta_record.mrg.result.yml', DOSTA_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_real(self):
        """
        Test with a real file and confirm no exceptions occur
        """
        with open(os.path.join(DOSTA_RESOURCE_PATH, 'unit_363_2013_245_6_6.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(240)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class DOSTARecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for recovered dosta glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaRecoveredDataParticle'
    }

    def test_dosta_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER, DOSTA_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {DostaRecoveredParticleKey.SCI_OXY4_OXYGEN: 242.217,
                    DostaRecoveredParticleKey.SCI_OXY4_SATURATION: 96.009}
        record_2 = {DostaRecoveredParticleKey.SCI_OXY4_OXYGEN: 242.141,
                    DostaRecoveredParticleKey.SCI_OXY4_SATURATION: 95.988}

        self.assert_generate_particle(DostaRecoveredDataParticle, record_1)
        self.assert_generate_particle(DostaRecoveredDataParticle, record_2)
        self.assert_no_more_data()


@attr('UNIT', group='mi')
class FLORTTelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for flort glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortTelemeteredDataParticle'
    }

    def test_flort_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER3, FLORT_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlortTelemeteredParticleKey.SCI_FLBBCD_BB_UNITS: 0.000281336,
                    FlortTelemeteredParticleKey.SCI_FLBBCD_CDOM_UNITS: 2.0352,
                    FlortTelemeteredParticleKey.SCI_FLBBCD_CHLOR_UNITS: 0.8349}
        record_2 = {FlortTelemeteredParticleKey.SCI_FLBBCD_BB_UNITS: 0.000262988,
                    FlortTelemeteredParticleKey.SCI_FLBBCD_CDOM_UNITS: 2.12,
                    FlortTelemeteredParticleKey.SCI_FLBBCD_CHLOR_UNITS: 0.847}

        self.assert_generate_particle(FlortTelemeteredDataParticle, record_1)
        self.assert_generate_particle(FlortTelemeteredDataParticle, record_2)
        self.assert_no_more_data()

    def test_multiple_yml(self):
        """
        Test with a yml file with multiple records
        """
        with open(os.path.join(FLORT_M_RESOURCE_PATH, 'multiple_glider_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(4)
            self.assert_particles(record, 'multiple_flort_record.mrg.result.yml', FLORT_M_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class FLORTRecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for recovered flort glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortRecoveredDataParticle'
    }

    def test_flort_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER3, FLORT_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlortRecoveredParticleKey.SCI_FLBBCD_BB_UNITS: 0.000281336,
                    FlortRecoveredParticleKey.SCI_FLBBCD_CDOM_UNITS: 2.0352,
                    FlortRecoveredParticleKey.SCI_FLBBCD_CHLOR_UNITS: 0.8349}
        record_2 = {FlortRecoveredParticleKey.SCI_FLBBCD_BB_UNITS: 0.000262988,
                    FlortRecoveredParticleKey.SCI_FLBBCD_CDOM_UNITS: 2.12,
                    FlortRecoveredParticleKey.SCI_FLBBCD_CHLOR_UNITS: 0.847}

        self.assert_generate_particle(FlortRecoveredDataParticle, record_1)
        self.assert_generate_particle(FlortRecoveredDataParticle, record_2)
        self.assert_no_more_data()


@attr('UNIT', group='mi')
class FlortOTelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for FLORT-O glider data.
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortODataParticle'
    }

    def test_flort_o_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER6, FLORT_O_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlortODataParticleKey.SCI_BB3SLO_B470_SIG: 70,
                    FlortODataParticleKey.SCI_BB3SLO_B532_SIG: 101,
                    FlortODataParticleKey.SCI_BB3SLO_B660_SIG: 169}
        record_2 = {FlortODataParticleKey.SCI_BB3SLO_B470_SIG: 148,
                    FlortODataParticleKey.SCI_BB3SLO_B532_SIG: 217,
                    FlortODataParticleKey.SCI_BB3SLO_B660_SIG: 4047}

        self.assert_generate_particle(FlortODataParticle, record_1)
        self.assert_generate_particle(FlortODataParticle, record_2)
        self.assert_no_more_data()

    def test_merged_data(self):
        """
        Test with a FLORT-O merged telemetered data file.
        """
        with open(os.path.join(FLORT_O_RESOURCE_PATH, 'merged_flort_o_telemetered_data.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(5)
            self.assert_particles(record, 'merged_flort_o_telemetered_data.mrg.result.yml', FLORT_O_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class FlortORecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for recovered FLORT-O glider data.
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortODataParticle'
    }

    def test_flort_o_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER6, FLORT_O_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlortODataParticleKey.SCI_BB3SLO_B470_SIG: 70,
                    FlortODataParticleKey.SCI_BB3SLO_B532_SIG: 101,
                    FlortODataParticleKey.SCI_BB3SLO_B660_SIG: 169}
        record_2 = {FlortODataParticleKey.SCI_BB3SLO_B470_SIG: 148,
                    FlortODataParticleKey.SCI_BB3SLO_B532_SIG: 217,
                    FlortODataParticleKey.SCI_BB3SLO_B660_SIG: 4047}

        self.assert_generate_particle(FlortODataParticle, record_1)
        self.assert_generate_particle(FlortODataParticle, record_2)
        self.assert_no_more_data()

    def test_merged_data(self):
        """
        Test with a FLORT-O merged recovered data file.
        """
        with open(os.path.join(FLORT_O_RESOURCE_PATH, 'merged_flort_o_recovered_data.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(5)
            self.assert_particles(record, 'merged_flort_o_recovered_data.mrg.result.yml', FLORT_O_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class PARADTelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for parad glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'ParadTelemeteredDataParticle'
    }

    def test_parad_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        # reused the FLORT record data for this Parad test
        self.set_data(HEADER3, FLORT_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {ParadTelemeteredParticleKey.SCI_BSIPAR_PAR: 664.424}
        record_2 = {ParadTelemeteredParticleKey.SCI_BSIPAR_PAR: 645.569}

        # (10553 = file size up to start of last row) 10553 - 19 bytes (for 19 lines of Carriage returns above) = 10534
        self.assert_generate_particle(ParadTelemeteredDataParticle, record_1)
        # (11997 = file size in bytes) 11997 - 20 bytes (for 20 lines of Carriage returns above) = 11977
        self.assert_generate_particle(ParadTelemeteredDataParticle, record_2)
        self.assert_no_more_data()

    def test_multiple_yml(self):
        """
        Test with a yml file with multiple records
        """
        with open(os.path.join(PARAD_RESOURCE_PATH, 'multiple_glider_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(4)
            self.assert_particles(record, 'multiple_parad_record.mrg.result.yml', PARAD_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class PARADRecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for recovered parad glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'ParadRecoveredDataParticle'
    }

    def test_parad_recovered_particle(self):
        """

        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        # reused the FLORT record data for this Parad test
        self.set_data(HEADER3, FLORT_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {ParadRecoveredParticleKey.SCI_BSIPAR_PAR: 664.424}
        record_2 = {ParadRecoveredParticleKey.SCI_BSIPAR_PAR: 645.569}

        # (10553 = file size up to start of last row) 10553 - 19 bytes (for 19 lines of Carriage returns above) = 10534
        self.assert_generate_particle(ParadRecoveredDataParticle, record_1)
        # (11997 = file size in bytes) 11997 - 20 bytes (for 20 lines of Carriage returns above) = 11977
        self.assert_generate_particle(ParadRecoveredDataParticle, record_2)
        self.assert_no_more_data()


@attr('UNIT', group='mi')
class FLORDTelemeteredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for flord glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordTelemeteredDataParticle'
    }

    def test_flord_telemetered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        # reused the FLORT record data for this Flord test
        self.set_data(HEADER5, FLORD_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlordParticleKey.SCI_FLBB_BB_UNITS: 0.000281, FlordParticleKey.SCI_FLBB_CHLOR_UNITS: 0.8349}
        record_2 = {FlordParticleKey.SCI_FLBB_BB_UNITS: 0.000263, FlordParticleKey.SCI_FLBB_CHLOR_UNITS: 0.847}

        self.assert_generate_particle(FlordTelemeteredDataParticle, record_1)

        self.assert_generate_particle(FlordTelemeteredDataParticle, record_2)
        self.assert_no_more_data()

    def test_multiple_yml(self):
        """
        Test with a yml file with a single record
        """
        with open(os.path.join(FLORD_M_RESOURCE_PATH, 'multiple_flord_record.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            record = parser.get_records(4)
            self.assert_particles(record, 'multiple_flord_record.mrg.result.yml', FLORD_M_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_real(self):
        """
        Test with a real file and confirm no exceptions occur
        """
        with open(os.path.join(FLORD_M_RESOURCE_PATH, 'unit_363_2013_245_6_6.mrg'), 'rU') as file_handle:
            parser = GliderParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(240)
            self.assert_(len(records) > 0)
            self.assertEquals(self.exception_callback_value, [])


@attr('UNIT', group='mi')
class FLORDRecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for flord glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordRecoveredDataParticle'
    }

    def test_flord_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        # reused the FLORT record data for this Flord test
        self.set_data(HEADER5, FLORD_RECORD)
        self.parser = GliderParser(self.config, self.test_data, self.exception_callback)

        record_1 = {FlordParticleKey.SCI_FLBB_BB_UNITS: 0.000281, FlordParticleKey.SCI_FLBB_CHLOR_UNITS: 0.8349}
        record_2 = {FlordParticleKey.SCI_FLBB_BB_UNITS: 0.000263, FlordParticleKey.SCI_FLBB_CHLOR_UNITS: 0.847}

        self.assert_generate_particle(FlordRecoveredDataParticle, record_1)

        self.assert_generate_particle(FlordRecoveredDataParticle, record_2)
        self.assert_no_more_data()


@attr('UNIT', group='mi')
class ENGGliderTest(GliderParserUnitTestCase):
    """
    Test cases for eng glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            EngineeringClassKey.METADATA: 'EngineeringMetadataDataParticle',
            EngineeringClassKey.DATA: 'EngineeringTelemeteredDataParticle',
            EngineeringClassKey.SCIENCE: 'EngineeringScienceTelemeteredDataParticle',
            EngineeringClassKey.GPS: 'GpsPositionDataParticle'
        }
    }

    def test_eng_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER4, ENGSCI_RECORD)
        self.parser = GliderEngineeringParser(self.config, self.test_data, self.exception_callback)

        meta_record = {EngineeringMetadataParticleKey.GLIDER_ENG_FILENAME: 'unit_247-2012-051-0-0-dbd(01840000)',
                       EngineeringMetadataParticleKey.GLIDER_MISSION_NAME: 'ENDUR1.MI',
                       EngineeringMetadataParticleKey.GLIDER_ENG_FILEOPEN_TIME: 'Tue_Feb_21_18:39:39_2012'}

        record_1 = {EngineeringTelemeteredParticleKey.M_BATTPOS: 0.703717,
                    EngineeringTelemeteredParticleKey.M_HEADING: 5.05447}
        record_2 = {EngineeringTelemeteredParticleKey.M_BATTPOS: 0.695632,
                    EngineeringTelemeteredParticleKey.M_HEADING: 5.05447}

        record_sci_1 = {EngineeringScienceTelemeteredParticleKey.SCI_M_DISK_FREE: 1000.1,
                        EngineeringScienceTelemeteredParticleKey.SCI_M_DISK_USAGE: 1000.1}
        record_sci_2 = {EngineeringScienceTelemeteredParticleKey.SCI_M_DISK_FREE: 1000.2,
                        EngineeringScienceTelemeteredParticleKey.SCI_M_DISK_USAGE: 1000.2}

        record_gps_1 = {GpsPositionParticleKey.M_GPS_LAT: 43.47113833333333,
                        GpsPositionParticleKey.M_GPS_LON: -125.39660833333333,
                        GpsPositionParticleKey.M_LAT: 43.47113833452416,
                        GpsPositionParticleKey.M_LON: -125.39660833431499}
        record_gps_2 = {GpsPositionParticleKey.M_GPS_LAT: 43.47113833333333,
                        GpsPositionParticleKey.M_GPS_LON: -125.39660833333333,
                        GpsPositionParticleKey.M_LAT: 43.47113833452416,
                        GpsPositionParticleKey.M_LON: -125.39660833431499}

        self.assert_generate_particle(EngineeringMetadataDataParticle, meta_record)
        # 1 sample line generates 3 particles
        self.assert_generate_particle(EngineeringTelemeteredDataParticle, record_1)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_1)
        self.assert_generate_particle(EngineeringScienceTelemeteredDataParticle, record_sci_1)

        self.assert_generate_particle(EngineeringTelemeteredDataParticle, record_2)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_2)
        self.assert_generate_particle(EngineeringScienceTelemeteredDataParticle, record_sci_2)
        self.assert_no_more_data()

    def test_encode_lat(self):
        """
        Test that encoding a latitude value that doesn't match the regex produces an encoding exception
        """
        self.set_data(HEADER4, ENGSCI_BAD_LAT_RECORD)
        self.parser = GliderEngineeringParser(self.config, self.test_data, self.exception_callback)

        record_1 = {EngineeringTelemeteredParticleKey.M_BATTPOS: 0.703717,
                    EngineeringTelemeteredParticleKey.M_HEADING: 5.05447,
                    EngineeringTelemeteredParticleKey.C_WPT_LAT: None,
                    EngineeringTelemeteredParticleKey.C_WPT_LON: -126.0}
        record_2 = {EngineeringTelemeteredParticleKey.M_BATTPOS: 0.695632,
                    EngineeringTelemeteredParticleKey.M_HEADING: 5.05447,
                    EngineeringTelemeteredParticleKey.C_WPT_LAT: 0.5,
                    EngineeringTelemeteredParticleKey.C_WPT_LON: -126.0}

        # just check the data records, the other particle classes were checked above
        self.assert_generate_particle(EngineeringMetadataDataParticle)
        self.assert_generate_particle(EngineeringTelemeteredDataParticle, record_1)
        self.assert_generate_particle(GpsPositionDataParticle)
        self.assert_generate_particle(EngineeringScienceTelemeteredDataParticle)
        self.assert_generate_particle(EngineeringTelemeteredDataParticle, record_2)
        self.assert_generate_particle(GpsPositionDataParticle)
        self.assert_generate_particle(EngineeringScienceTelemeteredDataParticle)
        self.assert_no_more_data()

    def test_bad_config(self):
        """
        Test that a bad config causes as exception
        """

        # bad metadata class, this one does not exist
        bad_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                EngineeringClassKey.METADATA: 'EngineeringDataParticle',
                EngineeringClassKey.DATA: 'EngineeringTelemeteredDataParticle',
                EngineeringClassKey.SCIENCE: 'EngineeringScienceTelemeteredDataParticle'
            }
        }

        self.set_data(HEADER4, ENGSCI_RECORD)
        with self.assertRaises(ConfigurationException):
            self.parser = GliderEngineeringParser(bad_config, self.test_data, self.exception_callback)

        # no config
        with self.assertRaises(ConfigurationException):
            self.parser = GliderEngineeringParser({}, self.test_data, self.exception_callback)

        # no particle classes dict in config
        bad_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        }
        with self.assertRaises(ConfigurationException):
            self.parser = GliderEngineeringParser(bad_config, self.test_data, self.exception_callback)


@attr('UNIT', group='mi')
class ENGRecoveredGliderTest(GliderParserUnitTestCase):
    """
    Test cases for recovered eng glider data
    """
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            EngineeringClassKey.METADATA: 'EngineeringMetadataRecoveredDataParticle',
            EngineeringClassKey.DATA: 'EngineeringRecoveredDataParticle',
            EngineeringClassKey.SCIENCE: 'EngineeringScienceRecoveredDataParticle',
            EngineeringClassKey.GPS: 'GpsPositionDataParticle'
        }
    }

    def test_eng_recovered_particle(self):
        """
        Verify we publish particles as expected.  Ensure particle is published and
        that state is returned.
        """
        self.set_data(HEADER4, ENGSCI_RECORD)
        self.parser = GliderEngineeringParser(self.config, self.test_data, self.exception_callback)

        meta_record = {EngineeringMetadataParticleKey.GLIDER_ENG_FILENAME: 'unit_247-2012-051-0-0-dbd(01840000)',
                       EngineeringMetadataParticleKey.GLIDER_MISSION_NAME: 'ENDUR1.MI',
                       EngineeringMetadataParticleKey.GLIDER_ENG_FILEOPEN_TIME: 'Tue_Feb_21_18:39:39_2012'}

        record_1 = {EngineeringRecoveredParticleKey.M_BATTPOS: 0.703717,
                    EngineeringRecoveredParticleKey.M_HEADING: 5.05447}
        record_2 = {EngineeringRecoveredParticleKey.M_BATTPOS: 0.695632,
                    EngineeringRecoveredParticleKey.M_HEADING: 5.05447}

        record_sci_1 = {EngineeringScienceRecoveredParticleKey.SCI_M_DISK_FREE: 1000.1,
                        EngineeringScienceRecoveredParticleKey.SCI_M_DISK_USAGE: 1000.1}
        record_sci_2 = {EngineeringScienceRecoveredParticleKey.SCI_M_DISK_FREE: 1000.2,
                        EngineeringScienceRecoveredParticleKey.SCI_M_DISK_USAGE: 1000.2}

        record_gps_1 = {GpsPositionParticleKey.M_GPS_LAT: 43.47113833333333,
                        GpsPositionParticleKey.M_GPS_LON: -125.39660833333333,
                        GpsPositionParticleKey.M_LAT: 43.47113833452416,
                        GpsPositionParticleKey.M_LON: -125.39660833431499}
        record_gps_2 = {GpsPositionParticleKey.M_GPS_LAT: 43.47113833333333,
                        GpsPositionParticleKey.M_GPS_LON: -125.39660833333333,
                        GpsPositionParticleKey.M_LAT: 43.47113833452416,
                        GpsPositionParticleKey.M_LON: -125.39660833431499}

        self.assert_generate_particle(EngineeringMetadataRecoveredDataParticle, meta_record)
        # 1 sample line generates 2 particles
        self.assert_generate_particle(EngineeringRecoveredDataParticle, record_1)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_1)
        self.assert_generate_particle(EngineeringScienceRecoveredDataParticle, record_sci_1)
        # total file size in bytes
        self.assert_generate_particle(EngineeringRecoveredDataParticle, record_2)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_2)
        self.assert_generate_particle(EngineeringScienceRecoveredDataParticle, record_sci_2)
        self.assert_no_more_data()

    def test_multiple_yml(self):
        """
        Test with a yml file with a multiple records
        """
        with open(os.path.join(ENG_RESOURCE_PATH, 'multiple_glider_record-engDataOnly.mrg'), 'rU') as file_handle:
            parser = GliderEngineeringParser(self.config, file_handle, self.exception_callback)
            particles = parser.get_records(13)
            self.assert_particles(particles, 'multiple_glider_record_recovered-engDataOnly.mrg.result.yml',
                                  ENG_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_real(self):
        """
        Test a real file and confirm no exceptions occur
        """
        with open(os.path.join(ENG_RESOURCE_PATH, 'unit_363_2013_245_6_6.mrg'), 'rU') as file_handle:
            parser = GliderEngineeringParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(240)
            self.assert_(len(records) > 3)
            self.assertEquals(self.exception_callback_value, [])

    def test_ingest_errors(self):
        """
        Test to check handling of inf fill values in real file
        """
        with open(os.path.join(ENG_RESOURCE_PATH, 'cp_388_2014_280_0_245.full.mrg'), 'rU') as file_handle:
            parser = GliderEngineeringParser(self.config, file_handle, self.exception_callback)
            particles = parser.get_records(32000)

            for particle in particles:
                data_dict = particle.generate_dict()

            # self.assert_particles(particles, 'multiple_glider_record_recovered-engDataOnly.mrg.result.yml',
            #                       ENG_RESOURCE_PATH)
            self.assertEquals(self.exception_callback_value, [])

    def test_for_69_file(self):
        """
        Test a real file and confirm no exceptions occur with file containing 69696969 fill values
        """
        with open(os.path.join(ENG_RESOURCE_PATH, 'cp_388_2016_012_0_0.mrg'), 'rU') as file_handle:
            parser = GliderEngineeringParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(2000)
            self.assert_(len(records) > 3)
            self.assertEquals(self.exception_callback_value, [])

    def test_69_values(self):
        """
        Test that encoding a value of 69696969 results in param values of None
        """

        record_gps_1 = {GpsPositionParticleKey.M_GPS_LAT: None,
                        GpsPositionParticleKey.M_GPS_LON: None}
        record_gps_2 = {GpsPositionParticleKey.M_GPS_LAT: None,
                        GpsPositionParticleKey.M_GPS_LON: None}

        self.set_data(HEADER4, ENGSCI_RECORD_69)
        self.parser = GliderEngineeringParser(self.config, self.test_data, self.exception_callback)

        # just check the data records, the other particle classes were checked above
        self.assert_generate_particle(EngineeringMetadataRecoveredDataParticle)
        self.assert_generate_particle(EngineeringRecoveredDataParticle)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_1)
        self.assert_generate_particle(EngineeringScienceRecoveredDataParticle)
        self.assert_generate_particle(EngineeringRecoveredDataParticle)
        self.assert_generate_particle(GpsPositionDataParticle, record_gps_2)
        self.assert_generate_particle(EngineeringScienceRecoveredDataParticle)
        self.assert_no_more_data()

    def test_for_nan_NaN(self):
        """
        Test a real file and confirm no exceptions occur when nan is used instead of NaN
        """
        with open(os.path.join(ENG_RESOURCE_PATH, 'cp_388_2016_012_1_0.mrg'), 'rU') as file_handle:
            parser = GliderEngineeringParser(self.config, file_handle, self.exception_callback)
            records = parser.get_records(2000)
            self.assert_(len(records) > 3)
            self.assertEquals(self.exception_callback_value, [])
