"""
@package mi.dataset.driver.nutnr_n
@file marine-integrations/mi/dataset/driver/nutnr_n/nutnr_n_recovered_driver.py
@author Emily Hahn
@brief Driver for the nutnr series n instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.nutnr_n import NutnrNParser
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
    with open(source_file_path, 'rb') as stream_handle:
        driver = NutnrNRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()
    return particle_data_handler


class NutnrNRecoveredDriver(SimpleDatasetDriver):
    """
    Derived driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        return NutnrNParser(stream_handle, self._exception_callback)
