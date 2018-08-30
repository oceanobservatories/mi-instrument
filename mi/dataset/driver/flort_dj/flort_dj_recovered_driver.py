#!/usr/bin/env python

"""
@package mi.dataset.driver.flort_dj
@file mi-dataset/mi/dataset/driver/flort_dj/flort_dj_recovered_driver.py
@author Rene Gelinas
@brief Driver for the flort_dj instrument (Recovered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flort_dj import FlortDjParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.flort_dj'


@version("1.0.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = FlortDjRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class FlortDjRecoveredDriver(SimpleDatasetDriver):
    """
    Derived flort_dj driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = FlortDjParser(stream_handle,
                               self._exception_callback)

        return parser
