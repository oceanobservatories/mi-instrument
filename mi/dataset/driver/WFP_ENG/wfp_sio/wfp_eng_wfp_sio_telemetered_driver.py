#!/usr/bin/env python

"""
@package mi.dataset.driver.WFP_ENG.wfp_sio
@file mi-dataset/mi/dataset/driver/WFP_ENG/wfp_sio/wfp_eng_wdp_sio_telemetered_driver.py
@author Mark Worden
@brief Driver for the wfp_eng_wfp_sio instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.wfp_eng_wfp_sio import WfpEngWfpSioParser
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

        driver = WfpEngWfpSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class WfpEngWfpSioTelemeteredDriver(SimpleDatasetDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        }

        parser = WfpEngWfpSioParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser


