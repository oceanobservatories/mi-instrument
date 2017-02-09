#!/usr/bin/env python

"""
@package mi.dataset.driver.vel3d_l.wfp
@file marine-integrations/mi/dataset/driver/vel3d_l/wfp/vel3d_l_wfp_recovered_driver.py
@author Tapana Gupta
@brief Driver for the vel3d_l_wfp instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.vel3d_l_wfp import Vel3dLWfpParser
from mi.core.versioning import version


@version("0.1.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = Vel3dlWfpRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class Vel3dlWfpRecoveredDriver(SimpleDatasetDriver):
    """
    Derived vel3d_l_wfp_recovered driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dLWfpInstrumentRecoveredParticle',
                                                     'Vel3dLWfpMetadataRecoveredParticle']
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = Vel3dLWfpParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser

