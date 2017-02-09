"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/parad_n_auv.py
@author Jeff Roy
@brief Parser and particle Classes and tools for the parad_n_auv data
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
PARAD_N_AUV_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('biospherical_mobile_sensor_voltage', 6, float),
    ('sensor_temperature', 7, float),
    ('parad_n_auv_supply_voltage', 8, float)
]


class ParadNAuvInstrumentParticle(AuvCommonParticle):

    _auv_param_map = PARAD_N_AUV_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class ParadNAuvTelemeteredParticle(ParadNAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "parad_n_auv_instrument"


class ParadNAuvRecoveredParticle(ParadNAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "parad_n_auv_instrument_recovered"


PARAD_N_AUV_ID = '1173'  # message ID of parad_n records
PARAD_N_AUV_FIELD_COUNT = 9  # number of expected fields in an parad_n record


PARAD_N_AUV_TELEMETERED_MESSAGE_MAP = [(PARAD_N_AUV_ID,
                                        PARAD_N_AUV_FIELD_COUNT,
                                        compute_timestamp,
                                        ParadNAuvTelemeteredParticle)]


PARAD_N_AUV_RECOVERED_MESSAGE_MAP = [(PARAD_N_AUV_ID,
                                      PARAD_N_AUV_FIELD_COUNT,
                                      compute_timestamp,
                                      ParadNAuvRecoveredParticle)]


class ParadNAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            message_map = PARAD_N_AUV_TELEMETERED_MESSAGE_MAP
        else:
            message_map = PARAD_N_AUV_RECOVERED_MESSAGE_MAP

        # provide message ID and # of fields to parent class
        super(ParadNAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              message_map)








