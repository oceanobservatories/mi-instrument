#!/usr/bin/env python

"""
@package mi.dataset.driver.phsen_gh
@file mi-dataset/mi/dataset/driver/phsen_gh/phsen_gh_driver.py
@author Samuel Dahlberg
@brief driver for the phsen_gh instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_gh import PhsenGhParser


def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        PhsenGhDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class PhsenGhDriver(SimpleDatasetDriver):
    """
        The phsen_gh telemetered driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(PhsenGhDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_gh',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'PhsenGhDataParticle'}

        parser = PhsenGhParser(parser_config, stream_handle, self._exception_callback)

        return parser
