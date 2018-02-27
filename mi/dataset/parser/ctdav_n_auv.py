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
    compute_timestamp, \
    CTDAV_AUV_COMMON_PARAM_MAP

# The structure below is a list of common tuples extended with a list
# of specific ctdav_n_auv tuples.  Each tuple consists of parameter
# name, index into raw data parts list, encoding function.
CTDAV_N_AUV_PARAM_MAP = list(CTDAV_AUV_COMMON_PARAM_MAP)
CTDAV_N_AUV_PARAM_MAP.extend(
    [
        ('dissolved_oxygen', 10, float),
        ('powered_on', 11, int)
    ]
)


class CtdavNAuvParticle(AuvCommonParticle):

    # must provide a parameter map for _build_parsed_values
    _auv_param_map = CTDAV_N_AUV_PARAM_MAP
    _data_particle_type = "ctdav_auv_data"


CTDAV_N_AUV_ID = '1181'  # message ID of ctdav_n records
CTDAV_N_AUV_FIELD_COUNT = len(CTDAV_N_AUV_PARAM_MAP) + 1  # number of expected fields in an ctdav_n record


CTDAV_N_AUV_MESSAGE_MAP = [(CTDAV_N_AUV_ID,
                            CTDAV_N_AUV_FIELD_COUNT,
                            compute_timestamp,
                            CtdavNAuvParticle)]


class CtdavNAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback):

        # provide message ID and # of fields to parent class
        super(CtdavNAuvParser, self).__init__(stream_handle,
                                              exception_callback,
                                              CTDAV_N_AUV_MESSAGE_MAP)
