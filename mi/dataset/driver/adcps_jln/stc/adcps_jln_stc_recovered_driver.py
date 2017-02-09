#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcps_jln_stc import AdcpsJlnStcParser, \
    AdcpsJlnStcMetadataRecoveredDataParticle, \
    AdcpsJlnStcInstrumentRecoveredDataParticle, \
    AdcpsJlnStcParticleClassKey
from mi.core.versioning import version

log = get_logger()


@version("0.0.5")
def parse(unused, source_file_path, particle_data_handler):
    config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcps_jln_stc',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            AdcpsJlnStcParticleClassKey.METADATA_PARTICLE_CLASS:
                AdcpsJlnStcMetadataRecoveredDataParticle,
            AdcpsJlnStcParticleClassKey.INSTRUMENT_PARTICLE_CLASS:
                AdcpsJlnStcInstrumentRecoveredDataParticle,
        }
    }
    log.debug("My ADCPS JLN STC Config: %s", config)

    def exception_callback(exception):
        log.debug("ERROR: %r", exception)
        particle_data_handler.setParticleDataCaptureFailure()

    with open(source_file_path, 'rb') as file_handle:
        parser = AdcpsJlnStcParser(config,
                                   file_handle,
                                   exception_callback)
                
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()  
        
    return particle_data_handler
