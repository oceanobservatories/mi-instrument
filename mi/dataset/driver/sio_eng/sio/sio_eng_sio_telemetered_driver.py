#!/usr/bin/env python

"""
@package mi.dataset.driver.sio_eng/sio
@file mi/dataset/driver/sio_eng/sio/sio_eng_sio_telemetered_driver.py
@author Jeff Roy
@brief Driver for the sio_eng_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.sio_eng_sio import SioEngSioParser
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
        driver = SioEngSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class SioEngSioTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived sio_eng_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.sio_eng_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'SioEngSioTelemeteredDataParticle'
        }

        parser = SioEngSioParser(parser_config, stream_handle,
                                 self._exception_callback)

        return parser


