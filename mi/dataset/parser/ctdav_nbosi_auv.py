"""
@package mi.dataset.parser
@file mi-instrument/mi/dataset/parser/ctdav_nbosi_auv.py
@author Rene Gelinas
@brief Parser and particle Classes and tools for the ctdav_nbosi_auv data
Release notes:

initial release
"""

__author__ = 'Rene Gelinas'


from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.auv_common import \
    AuvCommonParticle, \
    AuvCommonParser, \
    compute_timestamp, \
    CTDAV_AUV_COMMON_PARAM_MAP

# The structure below is a list of common tuples extended with a list
# of specific ctdav_nbosi_auv tuples.  Each tuple consists of parameter
# name, index into raw data parts list, encoding function.
CTDAV_NBOSI_AUV_PARAM_MAP = list(CTDAV_AUV_COMMON_PARAM_MAP)
CTDAV_NBOSI_AUV_PARAM_MAP.extend(
    [
        ('pressure', 10, float)
    ]
)


class CtdavNbosiAuvParticle(AuvCommonParticle):

    # must provide a parameter map for _build_parsed_values
    _auv_param_map = CTDAV_NBOSI_AUV_PARAM_MAP
    _data_particle_type = "ctdav_auv_data"


CTDAV_NBOSI_AUV_ID = '1107'  # message ID of ctdav_auv_data records
CTDAV_NBOSI_AUV_FIELD_COUNT = len(CTDAV_NBOSI_AUV_PARAM_MAP) + 1  # number of expected fields in an ctdav_nbois record


CTDAV_NBOSI_AUV_MESSAGE_MAP = [(CTDAV_NBOSI_AUV_ID,
                                CTDAV_NBOSI_AUV_FIELD_COUNT,
                                compute_timestamp,
                                CtdavNbosiAuvParticle)]


class CtdavNbosiAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback):

        # provide message ID and # of fields to parent class
        super(CtdavNbosiAuvParser, self).__init__(stream_handle,
                                                  exception_callback,
                                                  CTDAV_NBOSI_AUV_MESSAGE_MAP)
