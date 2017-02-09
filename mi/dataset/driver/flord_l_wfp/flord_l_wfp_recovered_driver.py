#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Jeff Roy'

import os

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver

from mi.dataset.parser.global_wfp_e_file_parser import GlobalWfpEFileParser
from mi.core.versioning import version

log = get_logger()


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpInstrumentParserDataParticle'
    }

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particle_data_handler.setParticleDataCaptureFailure()

    with open(source_file_path, 'r') as stream_handle:
        parser = GlobalWfpEFileParser(parser_config, None,
                                      stream_handle,
                                      lambda state, ingested: None,
                                      lambda data: log.trace("Found data: %s", data),
                                      exception_callback)

        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
