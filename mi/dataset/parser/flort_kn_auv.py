"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/flort_kn_auv
@author Jeff Roy
@brief Parser and particle Classes and tools for the flort_kn_auv data
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
    AuvCommonParser, \
    compute_timestamp


# The structure below is a list of tuples
# Each tuple consists of
# parameter name, index into raw data parts list, encoding function
FLORT_KN_AUV_METADATA_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('device_id', 2, int),
    ('device_count', 3, int),
    ('parameter_name', 4, str),
    ('parameter_unit', 5, str),
    ('parameter_id', 6, int),
    ('eco_data_offset', 7, int),
    ('html_plot', 8, int),
    ('parameter_type', 9, int),
    ('mx', 10, float),
    ('b', 11, float),
    ('sensor_name', 12, str)
]


def compute_metadata_timestamp(parts):
    """
    This method is required as part of the auv_message_map passed to the
    AuvCommonParser constructor
    This version uses just the mission_epoch from parts item 1

    :param parts: a list of the individual parts of an input record
    :return: a timestamp in the float64 NTP format.
    """
    mission_epoch = parts[1]

    unix_time = float(mission_epoch)
    timestamp = ntplib.system_to_ntp_time(unix_time)

    return timestamp


class FlortKnAuvMetadataParticle(AuvCommonParticle):

    _auv_param_map = FLORT_KN_AUV_METADATA_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class FlortKnAuvMetadataTelemParticle(FlortKnAuvMetadataParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "flort_kn_auv_metadata"


class FlortKnMetadataRecovParticle(FlortKnAuvMetadataParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "flort_kn_auv_metadata_recovered"


FLORT_KN_AUV_METADATA_ID = '1117'  # message ID of flort_kn metadata records
FLORT_KN_AUV_METADATA_FIELD_COUNT = 13  # number of expected fields in an flort_kn metadata record


# The structure below is a list of tuples
# Each tuple consists of
# parameter name, index into raw data parts list, encoding function
FLORT_KN_AUV_INSTRUMENT_PARAM_MAP = [
    # message ID is typically index 0
    ('mission_epoch', 1, int),
    ('auv_latitude', 2, float),
    ('auv_longitude', 3, float),
    ('mission_time', 4, int),
    ('device_id', 5, int),
    ('device_count', 6, int),
    ('m_depth', 7, float),
    ('version', 8, int),
    ('parameter_0', 9, float),
    ('parameter_1', 10, float),
    ('parameter_2', 11, float),
    ('parameter_3', 12, float),
    ('parameter_4', 13, float),
    ('parameter_5', 14, float),
    ('parameter_6', 15, float),
    ('parameter_7', 16, float),
    ('parameter_8', 17, float)
    # index 18 is ignored
]


class FlortKnAuvInstrumentParticle(AuvCommonParticle):

    _auv_param_map = FLORT_KN_AUV_INSTRUMENT_PARAM_MAP
    # must provide a parameter map for _build_parsed_values


class FlortKnAuvInstrumentTelemParticle(FlortKnAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "flort_kn_auv_instrument"


class FlortKnAuvInstrumentRecovParticle(FlortKnAuvInstrumentParticle):

    # set the data_particle_type for the DataParticle class
    _data_particle_type = "flort_kn_auv_instrument_recovered"


FLORT_KN_AUV_INSTRUMENT_ID = '1118'  # message ID of flort_kn instrument records
FLORT_KN_AUV_INSTRUMENT_COUNT = 19  # number of expected fields in an flort_kn instrument record


FLORT_KN_AUV_TELEMETERED_MESSAGE_MAP = [(FLORT_KN_AUV_METADATA_ID,
                                         FLORT_KN_AUV_METADATA_FIELD_COUNT,
                                         compute_metadata_timestamp,
                                         FlortKnAuvMetadataTelemParticle),
                                        (FLORT_KN_AUV_INSTRUMENT_ID,
                                         FLORT_KN_AUV_INSTRUMENT_COUNT,
                                         compute_timestamp,
                                         FlortKnAuvInstrumentTelemParticle)]


FLORT_KN_AUV_RECOVERED_MESSAGE_MAP = [(FLORT_KN_AUV_METADATA_ID,
                                       FLORT_KN_AUV_METADATA_FIELD_COUNT,
                                       compute_metadata_timestamp,
                                       FlortKnMetadataRecovParticle),
                                      (FLORT_KN_AUV_INSTRUMENT_ID,
                                       FLORT_KN_AUV_INSTRUMENT_COUNT,
                                       compute_timestamp,
                                       FlortKnAuvInstrumentRecovParticle)]


class FlortKnAuvParser(AuvCommonParser):

    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            message_map = FLORT_KN_AUV_TELEMETERED_MESSAGE_MAP
        else:
            message_map = FLORT_KN_AUV_RECOVERED_MESSAGE_MAP

        super(FlortKnAuvParser, self).__init__(stream_handle,
                                               exception_callback,
                                               message_map)








