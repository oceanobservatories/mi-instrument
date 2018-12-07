"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/adcpa_n_auv
@author Jeff Roy
@brief Parser and particle Classes and tools for the adcpa_n_auv data
Release notes:

initial release
"""

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.auv_common import \
    AuvCommonParticle, \
    AuvCommonParser, \
    compute_timestamp


# The structure below is a list of tuples
# Each tuple consists of
# parameter name, index into raw data parts list, encoding function
ADCPA_N_AUV_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('heading', 6, float),
    ('pitch', 7, float),
    ('roll', 8, float),
    ('altitude', 9, float),
    ('altitude_track_range_beam_1', 10, float),
    ('altitude_track_range_beam_2', 11, float),
    ('altitude_track_range_beam_3', 12, float),
    ('altitude_track_range_beam_4', 13, float),
    ('forward_velocity', 14, float),
    ('starboard_velocity', 15, float),
    ('vertical_velocity', 16, float),
    ('adcpa_n_auv_error_velocity', 17, float),
    ('temperature', 18, float),
    ('ensemble_number', 19, int),
    ('binary_velocity_data_1', 20, int),
    ('binary_velocity_data_2', 21, int),
    ('binary_velocity_data_3', 22, int),
    ('binary_velocity_data_4', 23, int),
    ('coordinates_transformation', 24, int),
    ('average_current', 25, float),
    ('average_direction', 26, float)
]


class AdcpaNAuvInstrumentParticle(AuvCommonParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "adcpa_n_auv_instrument"

    _auv_param_map = ADCPA_N_AUV_PARAM_MAP
    # must provide a parameter map for _build_parsed_values

ADCPA_N_AUV_ID = '1141'  # message ID of adcpa records
ADCPA_N_AUV_FIELD_COUNT = 27  # number of expected fields in an adcpa record


ADCPA_N_MESSAGE_MAP = [(ADCPA_N_AUV_ID,
                        ADCPA_N_AUV_FIELD_COUNT,
                        compute_timestamp,
                        AdcpaNAuvInstrumentParticle)]


class AdcpaNAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback):

        # provide message ID and # of fields to parent class
        super(AdcpaNAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              ADCPA_N_MESSAGE_MAP)








