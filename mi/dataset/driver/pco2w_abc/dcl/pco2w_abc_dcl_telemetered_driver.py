##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.driver.pco2w_abc.dcl.driver_common import Pco2wAbcDclDriver
from mi.dataset.parser.pco2w_abc import Pco2wAbcParticleClassKey
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDclMetadataTelemeteredDataParticle, \
    Pco2wAbcDclPowerTelemeteredDataParticle, \
    Pco2wAbcDclInstrumentTelemeteredDataParticle, \
    Pco2wAbcDclInstrumentBlankTelemeteredDataParticle
from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcDclMetadataTelemeteredDataParticle,
            Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcDclPowerTelemeteredDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcDclInstrumentTelemeteredDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
                Pco2wAbcDclInstrumentBlankTelemeteredDataParticle,
        }
    }

    driver = Pco2wAbcDclDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
