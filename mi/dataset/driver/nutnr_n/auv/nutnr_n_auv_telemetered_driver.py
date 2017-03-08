#!/usr/bin/env python

"""
@package mi.dataset.driver.nutnr_n.auv
@file mi/dataset/driver/nutnr_n/auv/nutnr_n_auv_telemetered_driver.py
@author Jeff Roy
@brief Driver for the nutnr_n_auv instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.nutnr_n_auv import NutnrNAuvParser
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
        driver = NutnrNAuvTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class NutnrNAuvTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived nutnr_n_auv driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = NutnrNAuvParser(stream_handle,
                                 self._exception_callback)

        return parser


