#!/usr/local/bin/python2.7
# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
# #

from mi.core.log import get_logger

from mi.dataset.parser.cg_stc_eng_stc import CgStcEngStcParser
from mi.dataset.dataset_driver import DataSetDriver


class CgStcEngDriver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        def exception_callback(exception):
            log.debug("ERROR: %r", exception)
            self._particle_data_handler.setParticleDataCaptureFailure()

        with open(self._source_file_path, 'rb') as stream_handle:
            parser = CgStcEngStcParser(self._parser_config, None, stream_handle,
                                       lambda state, ingested: None,
                                       lambda data: None, exception_callback)

            driver = DataSetDriver(parser, self._particle_data_handler)
            driver.processFileStream()

        return self._particle_data_handler
