#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2017 Raytheon Co.
##
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.moas.gl.flort_o.flort_o_glider_driver import FlortODriver
from mi.core.versioning import version

__author__ = 'Rene Gelinas'


@version("1.0.0")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortODataParticle'
    }

    driver = FlortODriver(source_file_path, particle_data_handler, parser_config)
    return driver.process()
