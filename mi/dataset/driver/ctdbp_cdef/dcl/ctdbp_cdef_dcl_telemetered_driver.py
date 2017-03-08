#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef.dcl
@file mi-dataset/mi/dataset/driver/ctdbp_cdef/dcl/ctdbp_cdef_dcl_telemetered_driver.py
@author Jeff Roy
@brief Driver for the ctdbp_cdef_dcl instrument (Telemetered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdbp_cdef_dcl import CtdbpCdefDclParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_dcl'


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
        driver = CtdbpCdefDclTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CtdbpCdefDclTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_cdef_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = CtdbpCdefDclParser(True,
                                    stream_handle,
                                    self._exception_callback)

        return parser
