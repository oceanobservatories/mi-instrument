##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##


import os

from mi.logging import config

from mi.dataset.driver.moas.gl.adcpa.adcpa_driver_common import AdcpaDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

__author__ = "Jeff Roy"


@version("0.2.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'velocity': 'VelocityGlider',
            'engineering': 'GliderEngineering',
            'config': 'GliderConfig',
            'bottom_track': 'EarthBottom',
            'bottom_track_config': 'BottomConfig',
        }
    }

    driver = AdcpaDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
