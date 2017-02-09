
#!/usr/bin/env python

"""
@package mi.dataset.parser.vel3d_a_mmp_cds
@file marine-integrations/mi/dataset/parser/vel3d_a_mmp_cds.py
@author Jeremy Amundson
@brief Parser for the Vel3dAMmpCds dataset driver
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
    INSTRUMENT = 'vel3d_a_mmp_cds_instrument'


class Vel3dAMmpCdsParserDataParticleKey(BaseEnum):

    VA = 'vel3d_a_va'
    VB = 'vel3d_a_vb'
    VC = 'vel3d_a_vc'
    VD = 'vel3d_a_vd'
    HX = 'vel3d_a_hx'
    HY = 'vel3d_a_hy'
    HZ = 'vel3d_a_hz'
    TX = 'vel3d_a_tx'
    TY = 'vel3d_a_ty'


class Vel3dAMmpCdsParserDataParticle(MmpCdsParserDataParticle):
    """
    Class for parsing data from the Vel3dAMmpCds data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def _get_mmp_cds_subclass_particle_params(self, dict_data):
        """
        This method is required to be implemented by classes that extend the MmpCdsParserDataParticle class.
        This implementation returns the particle parameters specific for Vel3dAMmpCds.
        @returns a list of particle params specific to Vel3dAMmpCds
        """

        va = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.VA,
                                dict_data['va'], float)
        vb = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.VB,
                                dict_data['vb'], float)
        vc = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.VC,
                                dict_data['vc'], float)
        vd = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.VD,
                                dict_data['vd'], float)
        hx = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.HX,
                                dict_data['hx'], float)
        hy = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.HY,
                                dict_data['hy'], float)
        hz = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.HZ,
                                dict_data['hz'], float)
        tx = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.TX,
                                dict_data['tx'], float)
        ty = self._encode_value(Vel3dAMmpCdsParserDataParticleKey.TY,
                                dict_data['ty'], float)

        return [va, vb, vc, vd, hx, hy, hz, tx, ty]
