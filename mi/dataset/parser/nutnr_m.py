"""
@package mi.dataset.parser
@file marine-integrations/mi/dataset/parser/nutnr_m.py
@author Emily Hahn
@brief Parser for the nutnr_m dataset driver
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re

from mi.dataset.parser.suna_common import SunaParser, PARAMETER_MAP_START, SunaDataParticle

# all records are 144 bytes
FRAME_SIZE = 144

START_FRAME_REGEX = r'SATS[LD]R\d{4}'
START_FRAME_MATCHER = re.compile(START_FRAME_REGEX)

LIGHT_SPECTRAL_MAP = [('spectral_channels',  slice(13, 45), list)]
DARK_SPECTRAL_MAP = [('dark_frame_spectral_channels',  slice(13, 45), list)]

PARAMETER_MAP_END = [
    ('temp_spectrometer',  45,            float),
    ('temp_lamp',          46,            float),
    ('humidity',           47,            float),
    ('nutnr_fit_rmse',     48,            float),
    ('ctd_time_uint32',    49,            int),
    ('ctd_psu',            50,            float),
    ('ctd_temp',           51,            float),
    ('ctd_dbar',           52,            float)
]

LIGHT_PARAMETER_MAP = PARAMETER_MAP_START + LIGHT_SPECTRAL_MAP + PARAMETER_MAP_END
DARK_PARAMETER_MAP = PARAMETER_MAP_START + DARK_SPECTRAL_MAP + PARAMETER_MAP_END


class NutnrMDataParticle(SunaDataParticle):

    _data_particle_type = 'nutnr_m_instrument_recovered'
    _param_map = LIGHT_PARAMETER_MAP


class NutnrMDarkDataParticle(SunaDataParticle):

    _data_particle_type = 'nutnr_m_dark_instrument_recovered'
    _param_map = DARK_PARAMETER_MAP


class NutnrMParser(SunaParser):
    def __init__(self,
                 stream_handle,
                 exception_callback):

        # string to use to unpack the binary data record
        unpack_string = '>3s3s4sId5f2HB32H4fI3fB'

        # no config for this parser, pass in empty dict
        super(NutnrMParser, self).__init__(stream_handle,
                                           exception_callback,
                                           START_FRAME_MATCHER,
                                           FRAME_SIZE,
                                           unpack_string,
                                           NutnrMDataParticle,
                                           NutnrMDarkDataParticle)