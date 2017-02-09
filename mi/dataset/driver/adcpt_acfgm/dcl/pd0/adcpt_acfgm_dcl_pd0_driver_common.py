#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.adcpt_acfgm_dcl_pd0 import AdcptAcfgmDclPd0Parser


__author__ = "Jeff Roy"

log = get_logger()


class AdcptAcfgmDclPd0Driver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):

        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):

        with open(self._source_file_path, "r") as file_handle:

            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = AdcptAcfgmDclPd0Parser(self._parser_config,
                                            file_handle,
                                            exception_callback,
                                            lambda state, ingested: None,
                                            lambda data: None)

            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler
