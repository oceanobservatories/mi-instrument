#!/usr/bin/env python

"""
@package mi.dataset.parser.flord_l_wfp
@file marine-integrations/mi/dataset/parser/flord_l_wfp.py
@author Joe Padula
@brief Particle for the flord_l_wfp dataset driver
    NOTE: there is no parser class in this file. This dataset is using the parser
    in global_wfp_e_file_parser.py.
Release notes:

Initial Release
"""

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

# noinspection PyUnresolvedReferences
import ntplib
import struct

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle

##############
# There is no parser in this file, this dataset uses the parser in global_wfp_e_file_parser.py
##############


class DataParticleType(BaseEnum):
    """
    The output particle/record stream for the recovered data, as identified in the
    flord_l_wfp IDD.
    """
    INSTRUMENT = 'flord_l_wfp_instrument_recovered'


class FlordLWfpInstrumentParserDataParticleKey(BaseEnum):
    """
    The names of the instrument particle parameters in the DataParticleType.INSTRUMENT stream.
    """
    PRESSURE = 'pressure'                       # corresponds to 'pressure' from E file
    RAW_SIGNAL_CHL = 'raw_signal_chl'           # corresponds to 'chl' from E file
    RAW_SIGNAL_BETA = 'raw_signal_beta'         # corresponds to 'ntu' from E file
    RAW_INTERNAL_TEMP = 'raw_internal_temp'     # corresponds to 'temperature' from E file
    WFP_TIMESTAMP = 'wfp_timestamp'


class FlordLWfpInstrumentParserDataParticle(DataParticle):
    """
    Class for parsing data from the flord_l_wfp data set
    """

    _data_particle_type = DataParticleType.INSTRUMENT

    def _build_parsed_values(self):
        """
        Take something in the binary data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag. Note that parse_chunks() in global_wfp_e_file_parser.py
        will set the data in raw_data.
        @throws SampleException If there is a problem with sample creation
        """

        fields_prof = struct.unpack('>I f f f f f h h h', self.raw_data)
        result = [self._encode_value(FlordLWfpInstrumentParserDataParticleKey.PRESSURE, fields_prof[3], float),
                  self._encode_value(FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_CHL, fields_prof[6], int),
                  self._encode_value(FlordLWfpInstrumentParserDataParticleKey.RAW_SIGNAL_BETA, fields_prof[7], int),
                  self._encode_value(FlordLWfpInstrumentParserDataParticleKey.RAW_INTERNAL_TEMP, fields_prof[8], int),
                  self._encode_value(FlordLWfpInstrumentParserDataParticleKey.WFP_TIMESTAMP, fields_prof[0], int)]
        return result
