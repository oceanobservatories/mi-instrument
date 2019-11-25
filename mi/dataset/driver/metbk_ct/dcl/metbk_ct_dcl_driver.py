#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2019 Raytheon Co.
##

import os
import re

from mi.core.exceptions import DatasetParserException
from mi.core.log import get_logger
from mi.core.versioning import version

from mi.dataset.parser.metbk_ct_dcl import MetbkCtDclParser, INDUCTIVE_ID_KEY
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

__author__ = 'Tim Fisher'
log = get_logger()

FILENAME_REGEX = r'SBE37SM-RS485_(\d+)_\d{4}_\d{2}_\d{2}-\S+.hex'
FILENAME_MATCHER = re.compile(FILENAME_REGEX)
FILENAME_SERIAL_NUMBER_GROUP = 1

MODULE_NAME = 'mi.dataset.parser.metbk_ct_dcl'
PARTICLE_CLASS = 'MetbkCtDclInstrumentDataParticle'

@version("1.0.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """
    log = get_logger()

    with open(source_file_path, "r") as stream_handle:
        def exception_callback(exception):
            log.debug("Exception: %s", exception)
            particle_data_handler.setParticleDataCaptureFailure()

        # extract the serial number from the file name
        serial_num = get_serial_num_from_filepath(source_file_path)

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: [PARTICLE_CLASS],
            INDUCTIVE_ID_KEY: serial_num
        }

        parser = MetbkCtDclParser(parser_config, stream_handle, exception_callback)
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


def get_serial_num_from_filepath(filepath):
    """
    Parse the serial number from the file path
    :param filepath: The full path of the file to extract the serial number from the name
    :return: serial number
    """

    # get just the filename from the full path
    filename = os.path.basename(filepath)

    # match the filename, serial number is the first group
    filename_match = FILENAME_MATCHER.match(filename)

    # can't run parser without the serial number, raise an exception if it can't be found
    if not filename_match:
        raise DatasetParserException("Unable to parse serial number from file name %s", filename)

    # return serial number as an integer
    return int(filename_match.group(FILENAME_SERIAL_NUMBER_GROUP))
