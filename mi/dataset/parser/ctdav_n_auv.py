"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/ctdav_n_auv.py
@author Jeff Roy
@brief Parser and particle Classes and tools for the ctdav_n_auv data
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
CTDAV_N_AUV_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('m_depth', 5, float),
    ('ctdav_n_auv_conductivity', 6, float),
    ('temperature', 7, float),
    ('salinity', 8, float),
    ('speed_of_sound', 9, float),
    ('dissolved_oxygen', 10, float),
    ('powered_on', 11, int)
]


class CtdavNAuvInstrumentParticle(AuvCommonParticle):

    _auv_param_map = CTDAV_N_AUV_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class CtdavNAuvTelemeteredParticle(CtdavNAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "ctdav_n_auv_instrument"


class CtdavNAuvRecoveredParticle(CtdavNAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "ctdav_n_auv_instrument_recovered"


CTDAV_N_AUV_ID = '1181'  # message ID of ctdav_n records
CTDAV_N_AUV_FIELD_COUNT = 12  # number of expected fields in an ctdav_n record


CTDAV_N_AUV_TELEMETERED_MESSAGE_MAP = [(CTDAV_N_AUV_ID,
                                        CTDAV_N_AUV_FIELD_COUNT,
                                        compute_timestamp,
                                        CtdavNAuvTelemeteredParticle)]


CTDAV_N_AUV_RECOVERED_MESSAGE_MAP = [(CTDAV_N_AUV_ID,
                                      CTDAV_N_AUV_FIELD_COUNT,
                                      compute_timestamp,
                                      CtdavNAuvRecoveredParticle)]


class CtdavNAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            message_map = CTDAV_N_AUV_TELEMETERED_MESSAGE_MAP
        else:
            message_map = CTDAV_N_AUV_RECOVERED_MESSAGE_MAP

        # provide message ID and # of fields to parent class
        super(CtdavNAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              message_map)








