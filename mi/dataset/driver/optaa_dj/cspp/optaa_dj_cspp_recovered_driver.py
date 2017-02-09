"""
@package mi.dataset.driver.optaa_dj.cspp
@file mi-dataset/mi/dataset/driver/optaa_dj/cspp/optaa_dj_cspp_recovered_driver.py
@author Joe Padula
@brief Recovered driver for the optaa_dj_cspp instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.cspp_base import \
    DATA_PARTICLE_CLASS_KEY, \
    METADATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.optaa_dj_cspp import \
    OptaaDjCsppMetadataRecoveredDataParticle, \
    OptaaDjCsppInstrumentRecoveredDataParticle, \
    OptaaDjCsppParser
from mi.core.versioning import version


@version("15.6.2")
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
        driver = OptaaDjCsppRecoveredDriver(unused, stream_handle, particle_data_handler)

        driver.processFileStream()

    return particle_data_handler


class OptaaDjCsppRecoveredDriver(SimpleDatasetDriver):
    """
    The optaa_dj_cspp recovered driver class extends the SimpleDatasetDriver.
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.optaa_dj_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: OptaaDjCsppMetadataRecoveredDataParticle,
                DATA_PARTICLE_CLASS_KEY: OptaaDjCsppInstrumentRecoveredDataParticle
            }
        }

        parser = OptaaDjCsppParser(parser_config,
                                   stream_handle,
                                   self._exception_callback)
        return parser
