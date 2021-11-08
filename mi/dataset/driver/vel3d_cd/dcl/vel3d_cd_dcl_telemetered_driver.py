#!/usr/bin/env python

"""
@package mi.dataset.driver.vel3d_cd.dcl
@file mi/dataset/driver/vel3d_cd/dcl/vel3d_cd_dcl_telemetered_driver.py
@author Emily Hahn
@brief Driver for the telemetered vel3d instrument series c and d through dcl
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.vel3d_cd_dcl import Vel3dCdDclParser
from mi.core.versioning import version


@version("15.8.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = Vel3dCdDclTelemeteredDriver(unused, stream_handle, particle_data_handler, source_file_path)
        driver.processFileStream()

    return particle_data_handler


class Vel3dCdDclTelemeteredDriver(SimpleDatasetDriver):

    def __init__(self, unused, stream_handle, particle_data_handler, source_file_path):
        self.source_file_path = source_file_path

        super(Vel3dCdDclTelemeteredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    """
    Create a _build_parser method for building the vel3d cd dcl parser
    """
    def _build_parser(self, stream_handle):
        """
        Build the vel3d cd dcl parser
        :param stream_handle: The file handle to pass into the parser
        :return: The created parser class
        """
        # no config input
        return Vel3dCdDclParser(stream_handle, self._exception_callback, self.source_file_path, is_telemetered=True)
