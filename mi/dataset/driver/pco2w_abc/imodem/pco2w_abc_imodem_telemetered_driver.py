#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2w_abc.imodem
@file mi-dataset/mi/dataset/driver/pco2w_abc/imodem/pco2w_abc_imodem_recovered_driver.py
@author Mark Worden
@brief Driver for the pco2w_abc_imodem instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.pco2w_abc_imodem import Pco2wAbcImodemParser
from mi.dataset.parser.pco2w_abc_particles import \
    Pco2wAbcParticleClassKey, \
    Pco2wAbcImodemInstrumentBlankTelemeteredDataParticle, \
    Pco2wAbcImodemInstrumentTelemeteredDataParticle, \
    Pco2wAbcImodemPowerTelemeteredDataParticle, \
    Pco2wAbcImodemControlTelemeteredDataParticle, \
    Pco2wAbcImodemMetadataTelemeteredDataParticle
from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        driver = Pco2wAbcImodemTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class Pco2wAbcImodemTelemeteredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS:
                Pco2wAbcImodemMetadataTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS:
                Pco2wAbcImodemPowerTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                Pco2wAbcImodemInstrumentTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                Pco2wAbcImodemInstrumentBlankTelemeteredDataParticle,
                Pco2wAbcParticleClassKey.CONTROL_PARTICLE_CLASS:
                Pco2wAbcImodemControlTelemeteredDataParticle,
            }
        }

        parser = Pco2wAbcImodemParser(parser_config,
                                      stream_handle,
                                      self._exception_callback)

        return parser
