# #
# OOIPLACEHOLDER
#
# Copyright 2019 Raytheon Co.
##

__author__ = "mworden"

import os

from mi.core.log import get_logger
from mi.logging import config

from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcInstrumentDataParticle, \
    Pco2wAbcInstrumentBlankDataParticle, Pco2wAbcPowerDataParticle, Pco2wAbcMetadataDataParticle

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.pco2w_abc import Pco2wAbcParser, Pco2wAbcParticleClassKey
from mi.core.versioning import version


class Pco2wAbcDriver:
    def __init__(self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        log = get_logger()

        with open(self._source_file_path, "r") as file_handle:
            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            parser = Pco2wAbcParser(self._parser_config,
                                    file_handle,
                                    exception_callback,
                                    None,
                                    None)

            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler


@version("15.6.2")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcMetadataDataParticle,
            Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcPowerDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcInstrumentDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS: Pco2wAbcInstrumentBlankDataParticle,
        }
    }

    driver = Pco2wAbcDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
