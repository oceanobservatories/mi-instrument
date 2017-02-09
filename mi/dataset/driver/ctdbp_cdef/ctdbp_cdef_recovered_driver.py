#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef
@file mi-dataset/mi/dataset/driver/ctdbp_cdef/ctdbp_cdef_recovered_driver.py
@author Jeff Roy
@brief Driver for the ctdbp_cdef instrument (Recovered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdbp_cdef import CtdbpCdefParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef'


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
        driver = CtdbpCdefRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CtdbpCdefRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_cdef driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = CtdbpCdefParser(stream_handle,
                                 self._exception_callback)

        return parser
