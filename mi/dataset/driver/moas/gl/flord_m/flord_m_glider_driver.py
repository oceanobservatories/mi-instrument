#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Rachel Manoni'

import sys

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderParser


class FlordMDriver:
    def __init__(self, unused, source_file_path, particle_data_handler, config):

        self._unused = unused
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._config = config

    def process(self):

        with open(self._source_file_path, 'rb') as stream_handle:
            def exp_callback(exception):
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = GliderParser(self._config,
                                  stream_handle,
                                  exp_callback)
            driver = DataSetDriver(parser, self._particle_data_handler)
            driver.processFileStream()

        return self._particle_data_handler
