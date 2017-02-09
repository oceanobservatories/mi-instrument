#!/usr/bin/env python

"""
@package mi.dataset.driver.adcpa_n
@file mi/dataset/driver/adcpa_n/auv/adcpa_n_recovered_driver.py
@author Jeff Roy
@brief Driver for the adcpa_n instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("15.8.1")
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
        driver = AdcpaNRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class AdcpaNRecoveredDriver(SimpleDatasetDriver):
    """
    Derived adcpa_n driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'velocity': 'VelocityInst',
                'engineering': 'AuvEngineering',
                'config': 'AuvConfig',
                'bottom_track': 'InstBottom',
                'bottom_track_config': 'BottomConfig',
            }
        }

        return AdcpPd0Parser(config, stream_handle, self._exception_callback)
