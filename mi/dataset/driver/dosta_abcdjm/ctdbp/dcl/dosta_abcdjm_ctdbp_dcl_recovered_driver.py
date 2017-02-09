#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_abcdjm_ctdbp.dcl
@file mi-dataset/mi/dataset/driver/dosta_abcdjm_ctdbp/dcl_cp/dosta_abcdjm_ctdbp_dcl_recovered_driver.py
@author Jeff Roy
@brief Driver for the dosta_abcdjm_ctdbp_dcl instrument (Recovered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dosta_abcdjm_ctdbp_dcl import DostaAbcdjmCtdbpDclParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.dosta_abcdjm_ctdb_dcl'


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

        # create an instance of the concrete driver class defined below
        driver = DostaAbcdjmCtdbpDclRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DostaAbcdjmCtdbpDclRecoveredDriver(SimpleDatasetDriver):
    """
    Derived dosta_abcdjm_ctdbp_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = DostaAbcdjmCtdbpDclParser(False,
                                           stream_handle,
                                           self._exception_callback)

        return parser
