#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2017 Raytheon Co.
##
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderParser

__author__ = 'Rene Gelinas'


class FlortODriver:
    def __init__(self, source_file_path, particle_data_handler, config):

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
