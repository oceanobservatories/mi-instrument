# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##


import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.flort_dj_dcl import FlortDjDclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

__author__ = "mworden"


@version("0.0.3")
class FlortDjDclRecoveredDriver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        with open(self._source_file_path, "r") as file_handle:
            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = FlortDjDclParser(self._parser_config, file_handle, exception_callback)

            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler


def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.flort_dj_dcl",
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlortDjDclRecoveredInstrumentDataParticle'
    }

    driver = FlortDjDclRecoveredDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
