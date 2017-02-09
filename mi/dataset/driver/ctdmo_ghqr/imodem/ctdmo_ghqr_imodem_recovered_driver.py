#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdmo_ghqr.imodem
@file mi-dataset/mi/dataset/driver/ctdmo_ghqr/imodem/ctdmo_ghqr_imodem_recovered_driver.py
@author Mark Worden
@brief Driver for the ctdmo_ghqr_imodem instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdmo_ghqr_imodem import CtdmoGhqrImodemParser, \
    CtdmoGhqrImodemParticleClassKey, \
    CtdmoGhqrImodemMetadataRecoveredDataParticle, \
    CtdmoGhqrImodemInstrumentRecoveredDataParticle
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

    with open(source_file_path, 'rb') as stream_handle:

        driver = CtdmoGhqrImodemrecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CtdmoGhqrImodemrecoveredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_imodem',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                CtdmoGhqrImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                    CtdmoGhqrImodemMetadataRecoveredDataParticle,
                CtdmoGhqrImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                    CtdmoGhqrImodemInstrumentRecoveredDataParticle,
            }
        }

        parser = CtdmoGhqrImodemParser(parser_config,
                                       stream_handle,
                                       self._exception_callback)

        return parser
