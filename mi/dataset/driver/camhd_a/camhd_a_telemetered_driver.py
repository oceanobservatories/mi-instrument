#!/usr/bin/env python

"""
@package mi.dataset.driver.camhd_a
@file mi-dataset/mi/dataset/driver/camhd_a/camhd_a_telemetered_driver.py
@author Ronald Ronquillo
@brief Telemetered driver for the camhd_a instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.camhd_a import CamhdAParser
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

    with open(source_file_path, 'rb') as stream_handle:

        CamhdATelemeteredDriver(unused, stream_handle,
                                particle_data_handler).processFileStream()

    return particle_data_handler


class CamhdATelemeteredDriver(SimpleDatasetDriver):
    """
    The camhd_a driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):

        super(CamhdATelemeteredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.camhd_a',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CamhdAInstrumentDataParticle'}

        parser = CamhdAParser(parser_config,
                              stream_handle,
                              self._exception_callback)

        return parser
