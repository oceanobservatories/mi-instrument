#!/usr/bin/env python

"""
@package mi.dataset.driver.phsen_abcdef
@file mi-dataset/mi/dataset/driver/phsen_abcdef/phsen_abcdef_recovered_driver.py
@author Ronald Ronquillo
@brief Driver for the phsen_abcdef instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.phsen_abcdef import PhsenRecoveredParser
from mi.core.versioning import version
log = get_logger()


@version("15.7.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = PhsenAbcdefRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class PhsenAbcdefRecoveredDriver(SimpleDatasetDriver):
    """
    Derived sio_eng_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        parser = PhsenRecoveredParser(parser_config, stream_handle, self._exception_callback)

        return parser
