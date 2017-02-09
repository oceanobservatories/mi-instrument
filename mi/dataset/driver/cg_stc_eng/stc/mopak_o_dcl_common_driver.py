#!/usr/local/bin/python2.7
# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
# #

from mi.core.log import get_logger

from mi.dataset.parser.mopak_o_dcl import MopakODclParser
from mi.dataset.dataset_driver import DataSetDriver


class MopakDriver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        def exception_callback(exception):
            log.debug("ERROR: %r", exception)
            self._particle_data_handler.setParticleDataCaptureFailure()

        pathList = (self._source_file_path.split('/'))
        filename = pathList[len(pathList) - 1]

        with open(self._source_file_path, 'rb') as stream_handle:
            parser = MopakODclParser(self._parser_config, stream_handle,
                                     filename, exception_callback)

            driver = DataSetDriver(parser, self._particle_data_handler)
            driver.processFileStream()

        return self._particle_data_handler
