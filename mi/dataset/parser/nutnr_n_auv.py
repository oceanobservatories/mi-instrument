"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/nutnr_n_auv.py
@author Jeff Roy
@brief Parser and particle Classes and tools for the nutnr_n_auv data
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


def encode_bool(bool_string):
    # interpret a string as boolean, convert to 0 or 1

    bool_string = bool_string.lower()

    if bool_string == 'true':
        return 1
    elif bool_string == 'false':
        return 0
    else:
        raise TypeError


# The structure below is a list of tuples
# Each tuple consists of
# parameter name, index into raw data parts list, encoding function
NUTNR_N_AUV_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('sample_time', 6, float),
    ('nitrate_concentration', 7, float),
    ('nutnr_nitrogen_in_nitrate', 8, float),
    ('nutnr_spectral_avg_last_dark', 9, int),
    ('temp_spectrometer', 10, float),
    ('lamp_state', 11, encode_bool),
    ('temp_lamp', 12, float),
    ('lamp_time_cumulative', 13, int),
    ('humidity', 14, float),
    ('voltage_main', 15, float),
    ('voltage_lamp', 16, float),
    ('nutnr_voltage_int', 17, float),
    ('nutnr_current_main', 18, float)
]


class NutnrNAuvInstrumentParticle(AuvCommonParticle):

    _auv_param_map = NUTNR_N_AUV_PARAM_MAP
    # must provide a parameter map for _build_parsed_values

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "nutnr_n_auv_instrument"


NUTNR_N_AUV_ID = '1174'  # message ID of nutnr_n records
NUTNR_N_AUV_FIELD_COUNT = 19  # number of expected fields in an dost_ln record


NUTNR_N_AUV_MESSAGE_MAP = [(NUTNR_N_AUV_ID,
                            NUTNR_N_AUV_FIELD_COUNT,
                            compute_timestamp,
                            NutnrNAuvInstrumentParticle)]


class NutnrNAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback):

        # provide message ID and # of fields to parent class
        super(NutnrNAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              NUTNR_N_AUV_MESSAGE_MAP)








