#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_ckl_mmp_cds
@file marine-integrations/mi/dataset/parser/ctdpf_ckl_mmp_cds.py
@author Mark Worden
@brief Particle for the CtdpfCklMmpCds dataset driver
Release notes:

initial release
"""


from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.dataset.parser.mmp_cds_base import MmpCdsParserDataParticle

log = get_logger()

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'


class DataParticleType(BaseEnum):
    INSTRUMENT = 'ctdpf_ckl_mmp_cds_instrument'


class CtdpfCklMmpCdsParserDataParticleKey(BaseEnum):
    CONDUCTIVITY = 'conductivity'
    TEMPERATURE = 'temperature'
    PRESSURE = 'pressure'


class CtdpfCklMmpCdsParserDataParticle(MmpCdsParserDataParticle):
    """
    Class for parsing data from the CtdpfCklMmpCds data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def _get_mmp_cds_subclass_particle_params(self, dict_data):
        """
        This method is required to be implemented by classes that extend the MmpCdsParserDataParticle class.
        This implementation returns the particle parameters specific for CtdpfCklMmpCds.  As noted in the
        base, it is okay to allow the following exceptions to propagate: ValueError, TypeError, IndexError.
        @returns a list of particle params specific to CtdpfCklMmpCds
        """

        conductivity = self._encode_value(CtdpfCklMmpCdsParserDataParticleKey.CONDUCTIVITY,
                                          dict_data['condwat'], float)
        temperature = self._encode_value(CtdpfCklMmpCdsParserDataParticleKey.TEMPERATURE,
                                         dict_data['tempwat'], float)
        pressure = self._encode_value(CtdpfCklMmpCdsParserDataParticleKey.PRESSURE,
                                      dict_data['preswat'], float)

        subclass_particle_params = [conductivity, temperature, pressure]

        return subclass_particle_params
