#!/usr/bin/env python

"""
@package mi.dataset.parser.flntu_x_mmp_cds
@file marine-integrations/mi/dataset/parser/flntu_x_mmp_cds.py
@author Jeremy Amundson
@brief Parser for the FlntuXMmpCds dataset driver
Release notes:

initial release
"""

from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.dataset.parser.mmp_cds_base import MmpCdsParserDataParticle

log = get_logger()

__author__ = 'Jeremy Amundson'
__license__ = 'Apache 2.0'


class DataParticleType(BaseEnum):
    INSTRUMENT = 'flntu_x_mmp_cds_instrument'


class FlntuXMmpCdsParserDataParticleKey(BaseEnum):

    CHLAFLO = 'chlaflo'
    NTUFLO = 'ntuflo'


class FlntuXMmpCdsParserDataParticle(MmpCdsParserDataParticle):
    """
    Class for parsing data from the FlntuXMmpCds data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def _get_mmp_cds_subclass_particle_params(self, dict_data):
        """
        This method is required to be implemented by classes that extend the MmpCdsParserDataParticle class.
        This implementation returns the particle parameters specific for FlntuXMmpCds.
        @returns a list of particle params specific to FlntuXMmpCds
        """

        chlorophyll = self._encode_value(FlntuXMmpCdsParserDataParticleKey.CHLAFLO,
                                         dict_data[FlntuXMmpCdsParserDataParticleKey.CHLAFLO], int)

        ntuflo = self._encode_value(FlntuXMmpCdsParserDataParticleKey.NTUFLO,
                                    dict_data[FlntuXMmpCdsParserDataParticleKey.NTUFLO], int)

        return [chlorophyll, ntuflo]


