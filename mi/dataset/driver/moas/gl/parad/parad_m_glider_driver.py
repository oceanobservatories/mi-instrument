#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'dmergens'

import sys

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderParser


class ParadMDriver:
    def __init__(self, unused, source_file_path, particle_data_handler_object, config):
        
        self._unused = unused
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler_object
        self._config = config

    def process(self):
    
        log = get_logger()

        with open(self._source_file_path, 'rb') as file_handle:

            def exception_callback(exception):
                log.debug('Exception: %s', exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = GliderParser(self._config,
                                  file_handle,
                                  exception_callback)

            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler
