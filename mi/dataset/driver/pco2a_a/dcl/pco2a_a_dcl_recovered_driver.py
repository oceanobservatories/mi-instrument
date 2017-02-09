#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.dcl.pco2a_a_dcl_recovered_driver
@file mi/dataset/driver/pco2a_a/dcl/pco2a_a_dcl_recovered_driver.py
@author Sung Ahn
@brief Recovered driver for pco2a_a_dcl data parser.

"""

from mi.dataset.driver.pco2a_a.dcl.pco2a_a_dcl_driver import process, \
    RECOVERED_PARTICLE_CLASSES
from mi.core.versioning import version


@version("15.6.2")
def parse(unused, source_file_path, particle_data_handler):
    process(source_file_path, particle_data_handler, RECOVERED_PARTICLE_CLASSES)

    return particle_data_handler
