#!/usr/bin/env python

"""
@package mi.dataset.driver.flobn_cm_subcon
@file marine-integrations/mi/dataset/driver/ppsdn/flobn_m_subcon_recovered_driver.py
@author Rachel Manoni
@brief Driver for the flobn_cm_subcon instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flobn_cm_subcon import FlobnMSubconParser
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
        driver = FlobnMSubconRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()
    return particle_data_handler


class FlobnMSubconRecoveredDriver(SimpleDatasetDriver):
    """
    Derived driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flobn_cm_subcon',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = FlobnMSubconParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser
