"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/dosta_ln_auv.py
@author Jeff Roy
@brief Parser and particle Classes and tools for the dosta_ln_auv data
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
DOSTA_LN_AUV_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('salinity', 6, float),
    ('product_number', 7, int),
    ('serial_number', 8, str),
    ('estimated_oxygen_concentration', 9, float),
    ('estimated_oxygen_saturation', 10, float),
    ('optode_temperature', 11, float),
    ('calibrated_phase', 12, float),
    ('blue_phase', 13, float),
    ('red_phase', 14, float),
    ('blue_amplitude', 15, float),
    ('b_pot', 16, float),
    ('red_amplitude', 17, float),
    ('raw_temperature', 18, float),
    ('calculated_oxygen_concentration', 19, float),
    ('calculated_oxygen_saturation', 20, float),
    ('external_temperature', 21, float)
]


class DostaLnAuvInstrumentParticle(AuvCommonParticle):

    _auv_param_map = DOSTA_LN_AUV_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class DostaLnAuvTelemeteredParticle(DostaLnAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "dosta_ln_auv_instrument"


class DostaLnAuvRecoveredParticle(DostaLnAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "dosta_ln_auv_instrument_recovered"


DOSTA_LN_AUV_ID = '1109'  # message ID of dost_ln records
DOSTA_LN_AUV_FIELD_COUNT = 22  # number of expected fields in an dost_ln record


DOSTA_LN_AUV_TELEMETERED_MESSAGE_MAP = [(DOSTA_LN_AUV_ID,
                                         DOSTA_LN_AUV_FIELD_COUNT,
                                         compute_timestamp,
                                         DostaLnAuvTelemeteredParticle)]


DOSTA_LN_AUV_RECOVERED_MESSAGE_MAP = [(DOSTA_LN_AUV_ID,
                                       DOSTA_LN_AUV_FIELD_COUNT,
                                       compute_timestamp,
                                       DostaLnAuvRecoveredParticle)]


class DostaLnAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            message_map = DOSTA_LN_AUV_TELEMETERED_MESSAGE_MAP
        else:
            message_map = DOSTA_LN_AUV_RECOVERED_MESSAGE_MAP

        # provide message ID and # of fields to parent class
        super(DostaLnAuvParser, self).__init__(stream_handle,
                                               exception_callback,
                                               message_map)








