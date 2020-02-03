#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2020 Raytheon Co.
##

from mi.core.log import get_logger
from mi.core.versioning import version

from mi.dataset.parser.metbk_ct_dcl import MetbkCtDclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

__author__ = 'Tim Fisher'
log = get_logger()

MODULE_NAME = 'mi.dataset.parser.metbk_ct_dcl'
PARTICLE_CLASS = 'MetbkCtDclInstrumentDataParticle'


@version("1.0.2")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe. Note since the instrument in use is not inductive
    there is no need to attempt to extract that value from the name of the parsed file.
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

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: [PARTICLE_CLASS]
        }

        parser = MetbkCtDclParser(parser_config, stream_handle, exception_callback)
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
