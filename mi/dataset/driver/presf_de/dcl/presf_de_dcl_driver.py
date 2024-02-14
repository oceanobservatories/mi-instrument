#!/usr/bin/env python

"""
@package mi.dataset.driver.presf_de
@file mi-dataset/mi/dataset/driver/presf_de/dcl/presf_de_dcl_driver.py
@author Samuel Dahlberg
@brief DCL driver for the presf_de instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.presf_de_dcl import PresfDeDclParser


def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        PresfDeDclDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class PresfDeDclDriver(SimpleDatasetDriver):
    """
        The presf_de driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(PresfDeDclDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.presf_de_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'PresfDeDataParticle'}

        parser = PresfDeDclParser(parser_config, stream_handle, self._exception_callback)

        return parser
