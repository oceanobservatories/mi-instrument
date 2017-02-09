#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_ln.wfp_sio.dosta_ln_wfp_sio_telemetered_driver.py
@file mi-dataset/mi/dataset/driver/dosta_ln/wfp_sio/ctdpf_wfp_sio_telemetered_driver.py
@author Jeff Roy
@brief Driver for the dosta_ln_wfp_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dosta_ln_wfp_sio import DostaLnWfpSioParser
from mi.core.versioning import version


@version("0.0.2")
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
        driver = DostaLnWfpSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DostaLnWfpSioTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived dosta_ln_wfp_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_ln_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaLnWfpSioDataParticle'
        }

        parser = DostaLnWfpSioParser(parser_config, stream_handle,
                                     self._exception_callback)

        return parser


