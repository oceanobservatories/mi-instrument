#!/usr/bin/env python

"""
@package mi.dataset.driver.nutnr_m.glider
@file mi/dataset/driver/nutnr_m/glider/nutnr_m_glider_telemetered_driver.py
@author Emily Hahn
@brief Driver for the nutnr series m instrument on a glider
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.glider import GliderParser
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

    with open(source_file_path, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = NutnrMGliderTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class NutnrMGliderTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived nutnr_m_glider driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'NutnrMDataParticle'
        }
        return GliderParser(config, stream_handle, self._exception_callback)


