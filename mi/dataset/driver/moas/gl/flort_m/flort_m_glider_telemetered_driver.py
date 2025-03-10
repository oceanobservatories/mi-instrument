#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Rachel Manoni'

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.moas.gl.flort_m.flort_m_glider_driver import FlortMDriver
from mi.core.versioning import version


@version("15.7.0")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortTelemeteredDataParticle'
    }

    driver = FlortMDriver(unused, source_file_path, particle_data_handler, parser_config)
    return driver.process()
