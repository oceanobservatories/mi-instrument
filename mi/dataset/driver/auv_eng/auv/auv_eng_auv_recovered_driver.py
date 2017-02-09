#!/usr/bin/env python

"""
@package mi.dataset.driver.auv_eng.auv
@file mi/dataset/driver/auv_eng/auv/auv_eng_auv_recovered_driver.py
@author Jeff Roy
@brief Driver for the auv_eng_auv instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.auv_eng_auv import AuvEngAuvParser
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
        driver = AuvEngAuvRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class AuvEngAuvRecoveredDriver(SimpleDatasetDriver):
    """
    Derived auv_eng_auv driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = AuvEngAuvParser(stream_handle,
                                 self._exception_callback,
                                 is_telemetered=False)

        return parser


