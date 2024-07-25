#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_p.wfp
@file mi-dataset/mi/dataset/driver/ctdpf_p/wfp/ctdpf_p_wfp_telemetered_driver.py
@author Samuel Dahlberg
@brief telemetered driver for the ctdpf_p instrument, on the Prawler WFP
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdpf_p_wfp import CtdpfPWfpParser


def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        CtdpfPWfpTelemeteredDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class CtdpfPWfpTelemeteredDriver(SimpleDatasetDriver):
    """
        The ctdpf_p telemetered driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(CtdpfPWfpTelemeteredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_p_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdpfPTelemeteredDataParticle'}

        parser = CtdpfPWfpParser(parser_config, stream_handle, self._exception_callback)

        return parser
