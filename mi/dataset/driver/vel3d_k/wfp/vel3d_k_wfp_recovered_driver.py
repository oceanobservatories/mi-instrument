#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
import os

from mi.logging import config
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler
from mi.dataset.parser.vel3d_k_wfp import Vel3dKWfpParser
from mi.core.versioning import version

__author__ = 'kustert'

log = get_logger()


@version("0.2.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_k_wfp',
        DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dKWfpMetadataParticle',
                                                 'Vel3dKWfpInstrumentParticle',
                                                 'Vel3dKWfpStringParticle']
    }

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particle_data_handler.setParticleDataCaptureFailure()

    with open(source_file_path, 'rb') as stream_handle:
        parser = Vel3dKWfpParser(parser_config,
                                 stream_handle,
                                 exception_callback)

        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
