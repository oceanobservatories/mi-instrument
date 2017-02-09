"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/nutnr_n.py
@author Emily Hahn
@brief Parser for the nutnr_n dataset driver
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re

from mi.dataset.parser.suna_common import SunaParser, PARAMETER_MAP_START, SunaDataParticle

# all records are 632 bytes
FRAME_SIZE = 632

START_FRAME_REGEX = r'SATS[LD]B\d{4}'
START_FRAME_MATCHER = re.compile(START_FRAME_REGEX)

LIGHT_SPECTRAL_MAP = [('spectral_channels',  slice(13, 269), list)]
DARK_SPECTRAL_MAP = [('dark_frame_spectral_channels',  slice(13, 269), list)]

PARAMETER_MAP_END = [
    ('temp_interior',      269,            float),
    ('temp_spectrometer',  270,            float),
    ('temp_lamp',          271,            float),
    ('lamp_time',          272,            int),
    ('humidity',           273,            float),
    ('voltage_main',       274,            float),
    ('voltage_lamp',       275,            float),
    ('nutnr_voltage_int',  276,            float),
    ('nutnr_current_main', 277,            float),
    ('aux_fitting_1',      278,            float),
    ('aux_fitting_2',      279,            float),
    ('nutnr_fit_base_1',   280,            float),
    ('nutnr_fit_base_2',   281,            float),
    ('nutnr_fit_rmse',     282,            float),
    ('ctd_time_uint32',    283,            int),
    ('ctd_psu',            284,            float),
    ('ctd_temp',           285,            float),
    ('ctd_dbar',           286,            float)
]

LIGHT_PARAMETER_MAP = PARAMETER_MAP_START + LIGHT_SPECTRAL_MAP + PARAMETER_MAP_END
DARK_PARAMETER_MAP = PARAMETER_MAP_START + DARK_SPECTRAL_MAP + PARAMETER_MAP_END


class NutnrNDataParticle(SunaDataParticle):

    _data_particle_type = 'nutnr_n_instrument_recovered'
    _param_map = LIGHT_PARAMETER_MAP


class NutnrNDarkDataParticle(SunaDataParticle):

    _data_particle_type = 'nutnr_n_dark_instrument_recovered'
    _param_map = DARK_PARAMETER_MAP


class NutnrNParser(SunaParser):
    def __init__(self,
                 stream_handle,
                 exception_callback):

        # string to use to unpack the binary data record
        unpack_string = '>3s3s4sId5f2HB256H3fI10fI3fB'

        # no config for this parser, pass in empty dict
        super(NutnrNParser, self).__init__(stream_handle,
                                           exception_callback,
                                           START_FRAME_MATCHER,
                                           FRAME_SIZE,
                                           unpack_string,
                                           NutnrNDataParticle,
                                           NutnrNDarkDataParticle)
