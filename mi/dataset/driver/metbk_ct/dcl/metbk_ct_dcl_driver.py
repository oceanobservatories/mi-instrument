#!/usr/local/bin/python2.7

"""
@package mi.dataset.driver.metbk_ct
@file metbk_ct_recovered_driver.py
@author Mark Steiner
@brief Driver for the metbk_ct_dcl_instrument stream

The metbk_ct_dcl_instrument stream is a subset of the metbk_a_dcl_instrument stream
so invoke metbk_dcl_a_driver.process to populate the RECOVERED_HOST_CT_PARTICLE_CLASS
"""

from mi.dataset.driver.metbk_a.dcl.metbk_dcl_a_driver import process, \
    RECOVERED_HOST_CT_PARTICLE_CLASS
from mi.core.versioning import version

@version("1.0.4")
def parse(unused, source_file_path, particle_data_handler):
    process(source_file_path, particle_data_handler, RECOVERED_HOST_CT_PARTICLE_CLASS)

    return particle_data_handler
