#!/usr/bin/env python

"""
@package mi.dataset.driver.turbd_a
@file mi-dataset/mi/dataset/driver/turbd_a/dcl/turbd_a_dcl_driver.py
@author Samuel Dahlberg
@brief DCL driver for the turbd_a instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.flort_dj_dcl import FlortDjDclParser

def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        TurbdADclDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class TurbdADclDriver(SimpleDatasetDriver):
    """
        The turbd_a driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(TurbdADclDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flort_dj_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'TurbdADclDataParticle'}

        parser = FlortDjDclParser(parser_config, stream_handle, self._exception_callback)

        return parser