#!/usr/bin/env python

"""
@package mi.dataset.parser.flcdr_x_mmp_cds
@file marine-integrations/mi/dataset/parser/flcdr_x_mmp_cds.py
@author Jeremy Amundson
@brief Parser for the FlcdrXMmpCds dataset driver
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
    INSTRUMENT = 'flcdr_x_mmp_cds_instrument'


class FlcdrXMmpCdsParserDataParticleKey(BaseEnum):

    CDOMFLO = 'cdomflo'


class FlcdrXMmpCdsParserDataParticle(MmpCdsParserDataParticle):
    """
    Class for parsing data from the FlcdrXMmpCds data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def _get_mmp_cds_subclass_particle_params(self, dict_data):
        """
        This method is required to be implemented by classes that extend the MmpCdsParserDataParticle class.
        This implementation returns the particle parameters specific for FlcdrXMmpCds.
        @returns a list of particle params specific to FlcdrXMmpCds
        """

        cdomflo = self._encode_value(FlcdrXMmpCdsParserDataParticleKey.CDOMFLO,
                                     dict_data[FlcdrXMmpCdsParserDataParticleKey.CDOMFLO], int)

        return [cdomflo]


