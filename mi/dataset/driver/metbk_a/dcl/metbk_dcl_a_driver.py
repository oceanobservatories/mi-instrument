#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.log import get_logger
from mi.dataset.parser.metbk_a_dcl import MetbkADclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

__author__ = 'Ronald Ronquillo'
log = get_logger()

MODULE_NAME = 'mi.dataset.parser.metbk_a_dcl'
RECOVERED_PARTICLE_CLASS = 'MetbkADclRecoveredInstrumentDataParticle'
TELEMETERED_PARTICLE_CLASS = 'MetbkADclTelemeteredInstrumentDataParticle'


def process(source_file_path, particle_data_handler, particle_class):

    with open(source_file_path, "r") as stream_handle:
        parser = MetbkADclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            stream_handle,
            lambda ex: particle_data_handler.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()
