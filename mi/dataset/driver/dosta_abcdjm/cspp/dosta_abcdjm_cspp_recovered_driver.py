"""
@package mi.dataset.driver.dosta_abcdjm.cspp
@file mi.dataset.driver.dosta_abcdjm.cspp.dosta_abcdjm_cspp_recovered_driver.py
@author Emily Hahn
@brief Recovered driver for the dosta series abcdjm instrument through cspp
"""

__author__ = 'ehahn'

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY, DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.dosta_abcdjm_cspp import DostaAbcdjmCsppParser, \
    DostaAbcdjmCsppMetadataRecoveredDataParticle, \
    DostaAbcdjmCsppInstrumentRecoveredDataParticle
from mi.core.versioning import version


@version("15.7.1")
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
        driver = DostaAbcdjmCsppRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DostaAbcdjmCsppRecoveredDriver(SimpleDatasetDriver):
    """
    This class just needs to create the _build_parser method of the SimpleDatasetDriver
    """

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_abcdjm_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: DostaAbcdjmCsppInstrumentRecoveredDataParticle,
            }
        }

        return DostaAbcdjmCsppParser(parser_config, stream_handle, self._exception_callback)

