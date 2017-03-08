#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_ln.auv
@file mi/dataset/driver/dosta_ln/auv/dosta_ln_auv_recovered_driver.py
@author Jeff Roy
@brief Driver for the dosta_ln_auv instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dosta_ln_auv import DostaLnAuvParser
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
        driver = DostaLnAuvTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DostaLnAuvTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived adcpa_n_auv driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = DostaLnAuvParser(stream_handle,
                                  self._exception_callback,
                                  is_telemetered=False)

        return parser


