#!/usr/bin/env python

"""
@package mi.dataset.driver.winch_cspp
@file mi-dataset/mi/dataset/driver/winch_cspp_/winch_cspp_driver.py
@author Richard Han
@brief Driver for the  Winch CSPP platform

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.winch_cspp import WinchCsppParser
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

    with open(source_file_path, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = \
            WinchCsppDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class WinchCsppDriver(SimpleDatasetDriver):
    """
    Derived WinchCspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.winch_cspp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'WinchCsppDataParticle'
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = WinchCsppParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser
