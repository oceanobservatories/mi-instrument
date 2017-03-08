"""
@package mi.dataset.test.test_driver_ingest
@file  mi/dataset/test/test_driver_ingest.py
@author Jeff Roy
@brief This module provides a simple method to send a file to a named driver.
The purpose is to provide a quick way to assess a driver & parsers compatibility
with data files
"""

import json
import os

from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'jroy'
log = get_logger()


def parse_file(driver_path, file_path, output_dir=None):
    """
    This method will use a DAD driver to invoke a parser and produce
    the L0 parser outputs.  The Outputs will be written to a file with the same
    root name as the input file in file_path with the .json suffix

    :param driver_path: full path name to dataset driver
    :param file_path:  full path to name of data file.
    :param output_dir  full path to directory to write outputs.
    :return:
    """

    # convert driver path to module name
    # replace \ with / in case this is on windows machine
    module = driver_path.replace('\\', '/')
    # replace / with .
    module = module.replace('/', '.')
    # strip off file extension
    module = module.rstrip('.py')

    try:

        driver_module = __import__(module, fromlist='parse')

    except ImportError as e:
        log.error("could not import method parse from driver module")
        raise e

    # Split the file path up into it's components to build the output file path
    (dir_path, file_name) = os.path.split(file_path)
    (short_name, extension) = os.path.splitext(file_name)

    # construct the output path
    output_file = short_name + '.json'
    if output_dir:
        output_path = os.path.join(output_dir, output_file)
    else:
        output_path = os.path.join(dir_path, output_file)

    particle_data_handler = ParticleDataHandler()

    driver_module.parse(None, file_path, particle_data_handler)

    stream_output = particle_data_handler._samples

    errors = particle_data_handler._failure
    if errors:
        log.debug('Errors during ingest test')

    output_fid = open(output_path, 'w')
    output_fid.write(json.dumps(stream_output))

if __name__ == '__main__':

    # TEST CODE USED FOR DEBUGGING (note tested on Windows platform)

    # first test, no output_dir, Unix style names
    driver_path1 = 'mi/dataset/driver/ctdbp_cdef/dcl/ctdbp_cdef_dcl_telemetered_driver.py'
    filename1 = 'mi/dataset/driver/ctdbp_cdef/dcl/resource/20150629.ctdbp.log'

    # second test, with output _dir, use os.path.join to construct Windows style names
    filename2 = os.path.join('mi', 'dataset', 'driver', 'ctdbp_cdef', 'dcl', 'resource', '20150409.ctdbp1.log')
    driver_path2 = os.path.join('mi', 'dataset', 'driver', 'ctdbp_cdef', 'dcl', 'ctdbp_cdef_dcl_telemetered_driver.py')
    output_dir2 = os.path.join('mi', 'dataset', 'driver', 'ctdbp_cdef', 'dcl', 'resource')

    parse_file(driver_path1, filename1)
    parse_file(driver_path2, filename2, output_dir2)

