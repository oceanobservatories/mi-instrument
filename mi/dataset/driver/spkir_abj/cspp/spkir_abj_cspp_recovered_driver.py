
"""
@package mi.dataset.driver.spkir_abj.cspp
@file mi/dataset/driver/spikr_abj/cspp/spkir_abj_cspp_recovered_driver.py
@author Mark Worden
@brief Driver for the spkir_abj_cspp instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.spkir_abj_cspp import \
    SpkirAbjCsppParser, \
    SpkirAbjCsppInstrumentRecoveredDataParticle, \
    SpkirAbjCsppMetadataRecoveredDataParticle
from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.core.versioning import version


@version("0.0.3")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = SpkirAbjCsppRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class SpkirAbjCsppRecoveredDriver(SimpleDatasetDriver):
    """
    Derived spkir_abj_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.spkir_abj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: SpkirAbjCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: SpkirAbjCsppInstrumentRecoveredDataParticle,
            }
        }

        parser = SpkirAbjCsppParser(parser_config, stream_handle,
                                    self._exception_callback)

        return parser


