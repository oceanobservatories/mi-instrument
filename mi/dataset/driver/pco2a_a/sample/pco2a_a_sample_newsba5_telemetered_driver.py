#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_newsba5_telemetered_driver
@file mi/dataset/driver/pco2a_a/sample/pco2a_a_sample_newsba5_telemetered_driver.py
@author Samuel Dahlberg
@brief Telemetered driver for pco2a_a_sample data parser with the new sba5 format.

"""

from mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_driver import process, \
    TELEMETERED_PARTICLE_CLASSES
from mi.core.versioning import version

from mi.dataset.parser.pco2a_a_sample import Pco2aADclParticleClassKey, \
    Pco2aADclTelemeteredInstrumentDataParticleAirNewSBA5, Pco2aADclTelemeteredInstrumentDataParticleWaterNewSBA5

TELEMETERED_PARTICLE_CLASSES_NEWSBA5 = {
    Pco2aADclParticleClassKey.AIR_PARTICLE_CLASS: Pco2aADclTelemeteredInstrumentDataParticleAirNewSBA5,
    Pco2aADclParticleClassKey.WATER_PARTICLE_CLASS: Pco2aADclTelemeteredInstrumentDataParticleWaterNewSBA5
}

@version("0.1.0")
def parse(unused, source_file_path, particle_data_handler):
    process(source_file_path, particle_data_handler, TELEMETERED_PARTICLE_CLASSES_NEWSBA5)

    return particle_data_handler
