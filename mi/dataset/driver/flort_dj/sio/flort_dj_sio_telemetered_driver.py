#!/usr/bin/env python

"""
@package mi.dataset.driver.flort_dj.sio
@file mi-dataset/mi/dataset/driver/flort_dj/sio/flort_dj_sio_telemetered_driver.py
@author Joe Padula
@brief Telemetered driver for the flort_dj_sio instrument

Release notes:

Initial Release
"""

__author__ = 'jpadula'

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.flort_dj_sio import FlortDjSioParser
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

        # create and instance of the concrete driver class defined below
        driver = FlortDjSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class FlortDjSioTelemeteredDriver(SimpleDatasetDriver):
    """
    The flort_dj_sio recovered driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flort_dj_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortdParserDataParticle'
        }

        parser = FlortDjSioParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser

