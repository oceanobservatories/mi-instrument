#!/usr/bin/env python

"""
@package mi.dataset.driver.fdchp_a.dcl
@file mi/dataset/driver/fdchp_a/dcl/fdchp_a_dcl_recovered_driver.py
@author Emily Hahn
@brief Driver for the fdchp series a through dcl recovered instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.fdchp_a_dcl import FdchpADclParser
from mi.core.versioning import version


@version("15.8.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'r') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = FdchpADclRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class FdchpADclRecoveredDriver(SimpleDatasetDriver):
    """
    Derived fdchp a dcl driver class
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):
        # build the parser
        return FdchpADclParser(stream_handle, self._exception_callback, is_telemetered=False)


