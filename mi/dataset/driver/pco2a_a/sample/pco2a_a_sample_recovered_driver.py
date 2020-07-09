#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_recovered_driver
@file mi/dataset/driver/pco2a_a/sample/pco2a_a_sample_recovered_driver.py
@author Tim Fisher
@brief Recovered driver for pco2a_a_sample data parser.

"""

from mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_driver import process, \
    RECOVERED_PARTICLE_CLASSES
from mi.core.versioning import version


@version("0.1.0")
def parse(unused, source_file_path, particle_data_handler):
    process(source_file_path, particle_data_handler, RECOVERED_PARTICLE_CLASSES)

    return particle_data_handler
