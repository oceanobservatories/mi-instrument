# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.optaa_dj_dcl import OptaaDjDclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

__author__ = "mworden"


class OptaaDjDclTelemeteredDriver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        with open(self._source_file_path, "rb") as file_handle:
            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = OptaaDjDclParser(self._parser_config,
                                      file_handle, exception_callback, self._source_file_path, True)

            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler


@version("15.7.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.optaa_dj_dcl",
        DataSetDriverConfigKeys.PARTICLE_CLASS: None
    }

    driver = OptaaDjDclTelemeteredDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
