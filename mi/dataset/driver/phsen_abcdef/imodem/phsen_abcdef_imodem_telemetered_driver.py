"""
@package mi.dataset.driver.phsen_abcdef.imodem
@file mi-dataset/mi/dataset/driver/phsen_abcdef/imodem/phsen_abcdef_imodem_telemetered_driver.py
@author Joe Padula
@brief Telemetered driver for the phsen_abcdef_imodem instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_abcdef_imodem import \
    PhsenAbcdefImodemParser, \
    PhsenAbcdefImodemParticleClassKey
from mi.dataset.parser.phsen_abcdef_imodem_particles import \
    PhsenAbcdefImodemControlTelemeteredDataParticle, \
    PhsenAbcdefImodemInstrumentTelemeteredDataParticle,\
    PhsenAbcdefImodemMetadataTelemeteredDataParticle
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

        # create an instance of the concrete driver class defined below
        driver = PhsenAbcdefImodemTelemeteredDriver(unused, stream_handle, particle_data_handler)

        driver.processFileStream()

    return particle_data_handler


class PhsenAbcdefImodemTelemeteredDriver(SimpleDatasetDriver):
    """
    The phsen_abcdef_imodem recovered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_imodem_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                PhsenAbcdefImodemParticleClassKey.METADATA_PARTICLE_CLASS:
                PhsenAbcdefImodemMetadataTelemeteredDataParticle,
                PhsenAbcdefImodemParticleClassKey.CONTROL_PARTICLE_CLASS:
                PhsenAbcdefImodemControlTelemeteredDataParticle,
                PhsenAbcdefImodemParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                PhsenAbcdefImodemInstrumentTelemeteredDataParticle,
            }
        }

        parser = PhsenAbcdefImodemParser(parser_config,
                                         stream_handle,
                                         self._exception_callback)

        return parser

