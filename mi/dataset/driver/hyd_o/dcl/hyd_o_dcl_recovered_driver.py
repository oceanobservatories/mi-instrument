"""
@package mi.dataset.driver.hyd_o.dcl
@file mi-dataset/mi/dataset/driver/hyd_o/dcl/hyd_o_dcl_recovered_driver.py
@author Emily Hahn
@brief Recovered driver for the hydrogen series o through a dcl instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.hyd_o_dcl import HydODclParser
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

        # create and instance of the concrete driver class defined below
        driver = HydODclRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class HydODclRecoveredDriver(SimpleDatasetDriver):
    """
    The hyd_o_dcl driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):
        """
        Build the parser for the input stream handle, no parser config is passed in
        :param stream_handle: The stream handle of the file to parse
        :return: The instantiation of the HydODclParser class
        """
        return HydODclParser(stream_handle, self._exception_callback, is_telemetered=False)

