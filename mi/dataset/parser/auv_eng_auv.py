"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/auv_eng_auv
@author Jeff Roy
@brief Parser and particle Classes and tools for the auv_eng_auv data
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

import ntplib

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.auv_common import \
    AuvCommonParticle, \
    AuvCommonParser


# The structure below is a list of tuples
# Each tuple consists of
# parameter name, index into raw data parts list, encoding function
AUV_ENG_AUV_IMAGENEX_852_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('range_setting', 6, int),
    ('pitch_deg', 7, float),
    ('look_down_angle', 8, float),
    ('m_altitude', 9, float),
    ('sonar_range', 10, float),
    ('roll_deg', 11, float),
    ('sonar_range_minimum', 12, float),
    ('estimated_range', 13, float),
    ('estimated_rate', 14, float),
    ('obstacle_range_rate_minimum', 15, float),
    ('obstacle_range_maximum', 16, float),
    ('obstacle_range_critical', 17, float),
    ('obstacle_range_rate_critical', 18, float),
    ('pings', 19, int),
    ('status', 20, int),
    ('median_filter_range', 21, float)
]

AUV_ENG_AUV_IMAGENEX_852_ID = '1102'  # message ID
AUV_ENG_AUV_IMAGENEX_852_COUNT = 22  # number of expected fields


class AuvEngAuvImagenex852Particle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_IMAGENEX_852_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvImagenex852TelemParticle(AuvEngAuvImagenex852Particle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_imagenex_852"


class AuvEngAuvImagenex852RecovParticle(AuvEngAuvImagenex852Particle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_imagenex_852_recovered"


AUV_ENG_AUV_DIGITAL_USBL_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('compass_true_heading', 4, float),
    ('auv_speed_of_sound', 5, float),
    ('reserved', 6, float),
    ('range_minimum', 7, float),
    ('range_maximum', 8, float),
    ('latency', 9, float),
    ('x_angle', 10, float),
    ('y_angle', 11, float),
    ('range', 12, float),
    ('gain_1', 13, int),
    ('gain_2', 14, int),
    ('array_sound_speed', 15, int),
    ('reason', 16, int),
    ('x_center', 17, int),
    ('y_center', 18, int),
    ('inband_snr', 19, int),
    ('outband_snr', 20, int),
    ('transponder_table_index', 21, int),
    ('mission_time', 22, int)
]

AUV_ENG_AUV_DIGITAL_USBL_ID = '1098'  # message ID
AUV_ENG_AUV_DIGITAL_USBL_COUNT = 23  # number of expected fields


class AuvEngAuvDigitalUsblParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_DIGITAL_USBL_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvDigitalUsblTelemParticle(AuvEngAuvDigitalUsblParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_digital_usbl"


class AuvEngAuvDigitalUsblRecovParticle(AuvEngAuvDigitalUsblParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_digital_usbl_recovered"


AUV_ENG_AUV_TRI_FIN_MOTOR_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('fin_count', 2, int),
    ('fin_pitch_position', 3, int),
    ('fin_rudder_position', 4, int),
    ('fin_roll_position', 5, int),
    ('fin_pitch_command', 6, float),
    ('fin_rudder_command', 7, float),
    ('fin_roll_command', 8, float),
    ('fin_command_data_1', 9, float),
    ('fin_position_data_1', 10, float),
    ('fin_command_data_2', 11, float),
    ('fin_position_data_2', 12, float),
    ('fin_command_data_3', 13, float),
    ('fin_position_data_3', 14, float),
    ('fin_command_data_4', 15, float),
    ('fin_position_data_4', 16, float),
    ('fin_command_data_5', 17, float),
    ('fin_position_data_5', 18, float),
    ('fin_command_data_6', 19, float),
    ('fin_position_data_6', 20, float),
    ('fin_yaw_translation_command', 21, float),
    ('fin_depth_translation_command', 22, float),
    ('fin_yaw_translation_position', 23, int),
    ('fin_depth_translation_position', 24, int),
]

AUV_ENG_AUV_TRI_FIN_MOTOR_ID = '1089'  # message ID
AUV_ENG_AUV_TRI_FIN_MOTOR_COUNT = 25  # number of expected fields


class AuvEngAuvTriFinMotorParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_TRI_FIN_MOTOR_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvTriFinMotorTelemParticle(AuvEngAuvTriFinMotorParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_tri_fin_motor"


class AuvEngAuvTriFinMotorRecovParticle(AuvEngAuvTriFinMotorParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_tri_fin_motor_recovered"

AUV_ENG_AUV_EMERGENCY_BOARD_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('board_voltage', 2, float),
    ('status', 3, int),
    ('descent_status', 4, int),
    ('ascent_status', 5, int),
    ('pickup_status', 6, int),
    ('descent_continuity', 7, int),
    ('ascent_continuity', 8, int),
    ('pickup_continuity', 9, int),
    ('secondary_status', 10, int)
]

AUV_ENG_AUV_EMERGENCY_BOARD_ID = '1076'  # message ID
AUV_ENG_AUV_EMERGENCY_BOARD_COUNT = 11  # number of expected fields


class AuvEngAuvEmergencyBoardParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_EMERGENCY_BOARD_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvEmergencyBoardTelemParticle(AuvEngAuvEmergencyBoardParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_emergency_board"


class AuvEngAuvEmergencyBoardRecovParticle(AuvEngAuvEmergencyBoardParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_emergency_board_recovered"

AUV_ENG_AUV_OIL_COMPENSATOR_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('m_depth', 4, float),
    ('raw_value', 5, int),
    ('oil_level', 6, float),
    ('mission_time', 7, int),
    ('oil_location', 8, int)
]

AUV_ENG_AUV_OIL_COMPENSATOR_ID = '1075'  # message ID
AUV_ENG_AUV_OIL_COMPENSATOR_COUNT = 9  # number of expected fields


class AuvEngAuvOilCompensatorParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_OIL_COMPENSATOR_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvOilCompensatorTelemParticle(AuvEngAuvOilCompensatorParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_oil_compensator"


class AuvEngAuvOilCompensatorRecovParticle(AuvEngAuvOilCompensatorParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_oil_compensator_recovered"

AUV_ENG_AUV_SMART_BATTERY_PARAM_MAP = [
    # message ID is typically index 0
    ('flags', 1, int),
    ('battery_index', 2, int),
    ('battery_count', 3, int),
    ('address_mask', 4, int),
    ('smart_battery_voltage', 5, int),
    ('end_of_discharge', 6, int),
    ('average_current', 7, int),
    ('temperature_one_tenth', 8, int),
    ('full_charge_capacity', 9, int),
    ('remaining_capacity', 10, int),
    ('desired_charge_rate', 11, int),
    ('serial', 12, int),
    ('battery_status', 13, int),
    ('battery_flags', 14, int),
    ('cycle_count', 15, int),
    ('mission_epoch', 16, int),
    ('available_power', 17, float),
    ('auv_temperature', 18, float),
    ('pressure_mbar', 19, float),
    ('pic_charge_value', 20, int),
    ('pic_balance_enabled', 21, int),
    ('pic_fet_state', 22, int),
    ('pic_faults', 23, int),
    ('pic_cell_voltage_1', 24, int),
    ('pic_cell_voltage_2', 25, int),
    ('pic_cell_voltage_3', 26, int),
    ('pic_cell_voltage_4', 27, int),
    ('pic_cell_voltage_5', 28, int),
    ('pic_cell_voltage_6', 29, int),
    ('pic_cell_voltage_7', 30, int),
    ('battery_temperature', 31, float)
]

AUV_ENG_AUV_SMART_BATTERY_ID = '1071'  # message ID
AUV_ENG_AUV_SMART_BATTERY_COUNT = 32  # number of expected fields


class AuvEngAuvSmartBatteryParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_SMART_BATTERY_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvSmartBatteryTelemParticle(AuvEngAuvSmartBatteryParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_smart_battery"


class AuvEngAuvSmartBatteryRecovParticle(AuvEngAuvSmartBatteryParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_smart_battery_recovered"

AUV_ENG_AUV_DIGITAL_TX_BOARD_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('transponder_table_index_1', 2, int),
    ('transponder_table_index_2', 3, int),
    ('inband_channel_1_snr', 4, int),
    ('inband_channel_2_snr', 5, int),
    ('interrogate_channel_1_snr', 6, int),
    ('interrogate_channel_2_snr', 7, int),
    ('receive_channel_1', 8, int),
    ('receive_channel_2', 9, int),
    ('range_1', 10, float),
    ('range_2', 11, float),
    ('reply_age_1', 12, int),
    ('reply_age_2', 13, int),
    ('auv_latitude', 14, float),
    ('auv_longitude', 15, float),
    ('compass_true_heading', 16, float),
    ('auv_speed_of_sound', 17, float),
    ('fail_flag', 18, int),
    ('received_bits', 19, int),
    ('mission_time', 20, int),
    ('outband_channel_1_snr', 21, int),
    ('outband_channel_2_snr', 22, int)
]

AUV_ENG_AUV_DIGITAL_TX_BOARD_ID = '1050'  # message ID
AUV_ENG_AUV_DIGITAL_TX_BOARD_COUNT = 23  # number of expected fields


class AuvEngAuvDigitalTxBoardParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_DIGITAL_TX_BOARD_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvDigitalTxBoardTelemParticle(AuvEngAuvDigitalTxBoardParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_digital_tx_board"


class AuvEngAuvDigitalTxBoardRecovParticle(AuvEngAuvDigitalTxBoardParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_digital_tx_board_recovered"

AUV_ENG_AUV_FAULT_MESSAGE_PARAM_MAP = [
    # message ID is typically index 0
    ('filename', 1, str),
    ('line', 2, int),
    ('mission_epoch', 3, int),
    ('message', 4, str)
]

AUV_ENG_AUV_FAULT_MESSAGE_ID = '1001'  # message ID
AUV_ENG_AUV_FAULT_MESSAGE_COUNT = 5  # number of expected fields


class AuvEngAuvFaultMessageParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_FAULT_MESSAGE_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvFaultMessageTelemParticle(AuvEngAuvFaultMessageParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_fault_message"


class AuvEngAuvFaultMessageRecovParticle(AuvEngAuvFaultMessageParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_fault_message_recovered"

AUV_ENG_AUV_STATE_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('software_version', 2, int),
    ('auv_temperature', 3, float),
    ('heading_rate', 4, float),
    ('pressure_mbar', 5, float),
    ('m_depth', 6, float),
    ('depth_goal', 7, float),
    ('obs', 8, float),
    ('auv_state_voltage', 9, float),
    ('auv_current', 10, float),
    ('gfi', 11, float),
    ('pitch_deg', 12, float),
    ('pitch_goal', 13, float),
    ('roll_deg', 14, float),
    ('thruster', 15, int),
    ('thruster_goal', 16, int),
    ('compass_true_heading', 17, float),
    ('heading_goal', 18, float),
    ('mission_time', 19, int),
    ('days_since_1970', 20, int),
    ('auv_latitude', 21, float),
    ('auv_longitude', 22, float),
    ('dr_latitude', 23, float),
    ('dr_longitude', 24, float),
    ('goal_latitude', 25, float),
    ('goal_longitude', 26, float),
    ('estimated_velocity', 27, float),
    ('heading_offset', 28, float),
    ('flags', 29, int),
    ('thruster_command', 30, int),
    ('pitch_command', 31, int),
    ('rudder_command', 32, int),
    ('pitch_fin_position', 33, int),
    ('rudder_fin_position', 34, int),
    ('total_objectives', 35, int),
    ('current_objective', 36, int),
    ('cpu_usage', 37, int),
    ('objective_index', 38, int),
    ('leg_number', 39, int),
    ('spare_slider', 40, float),
    ('roll_rate', 41, float),
    ('pitch_rate', 42, float),
    ('faults', 43, int),
    ('navigation_mode', 44, int),
    ('secondary_faults', 45, int)
]

AUV_ENG_AUV_STATE_ID = '1000'  # message ID
AUV_ENG_AUV_STATE_COUNT = 46  # number of expected fields


class AuvEngAuvStateParticle(AuvCommonParticle):

    _auv_param_map = AUV_ENG_AUV_STATE_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class AuvEngAuvStateTelemParticle(AuvEngAuvStateParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_state"


class AuvEngAuvStateRecovParticle(AuvEngAuvStateParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "auv_eng_auv_state_recovered"


def compute_timestamp_from_parts(parts, epoch_id, time_id=None):
    """
    This method is generic way of computing timestamps from the parts found
    in raw_data of the auv particle classes.  It can be used to create the
    the timestamp methods required as part of the auv_message_map passed to the
    AuvCommonParser constructor

    :param parts: a list of the individual parts of an input record
    :param epoch_id: the index where the epoch time can be found
    :param time_id: optional parameter of time field
    :return: a timestamp in the float64 NTP format.
    """
    unix_time = float(parts[epoch_id])

    if time_id is not None:

        mission_time = parts[time_id]
        milliseconds = int(mission_time[-3:])/1000.0
        unix_time += milliseconds

    timestamp = ntplib.system_to_ntp_time(unix_time)

    return timestamp


AUV_ENG_AUV_TELEMETERED_MESSAGE_MAP = [(AUV_ENG_AUV_IMAGENEX_852_ID,
                                        AUV_ENG_AUV_IMAGENEX_852_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, 4),
                                        AuvEngAuvImagenex852TelemParticle),
                                       (AUV_ENG_AUV_DIGITAL_USBL_ID,
                                        AUV_ENG_AUV_DIGITAL_USBL_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, 22),
                                        AuvEngAuvDigitalUsblTelemParticle),
                                       (AUV_ENG_AUV_TRI_FIN_MOTOR_ID,
                                        AUV_ENG_AUV_TRI_FIN_MOTOR_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, None),
                                        AuvEngAuvTriFinMotorTelemParticle),
                                       (AUV_ENG_AUV_EMERGENCY_BOARD_ID,
                                        AUV_ENG_AUV_EMERGENCY_BOARD_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, None),
                                        AuvEngAuvEmergencyBoardTelemParticle),
                                       (AUV_ENG_AUV_OIL_COMPENSATOR_ID,
                                        AUV_ENG_AUV_OIL_COMPENSATOR_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, 7),
                                        AuvEngAuvOilCompensatorTelemParticle),
                                       (AUV_ENG_AUV_SMART_BATTERY_ID,
                                        AUV_ENG_AUV_SMART_BATTERY_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 16, None),
                                        AuvEngAuvSmartBatteryTelemParticle),
                                       (AUV_ENG_AUV_DIGITAL_TX_BOARD_ID,
                                        AUV_ENG_AUV_DIGITAL_TX_BOARD_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, 20),
                                        AuvEngAuvDigitalTxBoardTelemParticle),
                                       (AUV_ENG_AUV_FAULT_MESSAGE_ID,
                                        AUV_ENG_AUV_FAULT_MESSAGE_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 3, None),
                                        AuvEngAuvFaultMessageTelemParticle),
                                       (AUV_ENG_AUV_STATE_ID,
                                        AUV_ENG_AUV_STATE_COUNT,
                                        lambda parts: compute_timestamp_from_parts(parts, 1, 19),
                                        AuvEngAuvStateTelemParticle)]


AUV_ENG_AUV_RECOVERED_MESSAGE_MAP = [(AUV_ENG_AUV_IMAGENEX_852_ID,
                                      AUV_ENG_AUV_IMAGENEX_852_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, 4),
                                      AuvEngAuvImagenex852RecovParticle),
                                     (AUV_ENG_AUV_DIGITAL_USBL_ID,
                                      AUV_ENG_AUV_DIGITAL_USBL_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, 22),
                                      AuvEngAuvDigitalUsblRecovParticle),
                                     (AUV_ENG_AUV_TRI_FIN_MOTOR_ID,
                                      AUV_ENG_AUV_TRI_FIN_MOTOR_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, None),
                                      AuvEngAuvTriFinMotorRecovParticle),
                                     (AUV_ENG_AUV_EMERGENCY_BOARD_ID,
                                      AUV_ENG_AUV_EMERGENCY_BOARD_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, None),
                                      AuvEngAuvEmergencyBoardRecovParticle),
                                     (AUV_ENG_AUV_OIL_COMPENSATOR_ID,
                                      AUV_ENG_AUV_OIL_COMPENSATOR_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, 7),
                                      AuvEngAuvOilCompensatorRecovParticle),
                                     (AUV_ENG_AUV_SMART_BATTERY_ID,
                                      AUV_ENG_AUV_SMART_BATTERY_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 16, None),
                                      AuvEngAuvSmartBatteryRecovParticle),
                                     (AUV_ENG_AUV_DIGITAL_TX_BOARD_ID,
                                      AUV_ENG_AUV_DIGITAL_TX_BOARD_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, 20),
                                      AuvEngAuvDigitalTxBoardRecovParticle),
                                     (AUV_ENG_AUV_FAULT_MESSAGE_ID,
                                      AUV_ENG_AUV_FAULT_MESSAGE_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 3, None),
                                      AuvEngAuvFaultMessageRecovParticle),
                                     (AUV_ENG_AUV_STATE_ID,
                                      AUV_ENG_AUV_STATE_COUNT,
                                      lambda parts: compute_timestamp_from_parts(parts, 1, 19),
                                      AuvEngAuvStateRecovParticle)]


class AuvEngAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            message_map = AUV_ENG_AUV_TELEMETERED_MESSAGE_MAP
        else:
            message_map = AUV_ENG_AUV_RECOVERED_MESSAGE_MAP

        super(AuvEngAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              message_map)








