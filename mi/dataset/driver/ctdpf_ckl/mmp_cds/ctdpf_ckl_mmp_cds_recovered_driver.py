#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_ckl.mmp_cds.ctdpf_ckl_mmp_cds_recovered_driver.py
@file mi-dataset/mi/dataset/driver/ctdpf_ckl/mmp_cds/fctdpf_ckl_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the ctdpf_ckl_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.core.versioning import version


@version("0.0.3")
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
        driver = CtdpfCklMmpCdsRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CtdpfCklMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdpf_ckl_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdpfCklMmpCdsParserDataParticle'
        }

        parser = MmpCdsParser(parser_config, stream_handle, self._exception_callback)

        return parser
