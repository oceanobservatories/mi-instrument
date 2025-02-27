#!/usr/bin/env python

"""
@package mi.dataset.driver.dofst_p.wfp
@file mi-dataset/mi/dataset/driver/dofst_p/wfp/dofst_p_wfp_driver.py
@author Samuel Dahlberg
@brief driver for the dofst_p instrument, on the Prawler WFP
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.dofst_p_wfp import DofstPWfpParser


def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        DofstPWfpDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class DofstPWfpDriver(SimpleDatasetDriver):
    """
        The dofst_p driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(DofstPWfpDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofst_p_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'DofstPDataParticle'}

        parser = DofstPWfpParser(parser_config, stream_handle, self._exception_callback)

        return parser
