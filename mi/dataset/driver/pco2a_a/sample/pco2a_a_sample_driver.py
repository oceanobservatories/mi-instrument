#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.sample.pco2a_a_sample_driver
@file mi/dataset/driver/pco2a_a/sample/pco2a_a_sample_driver.py
@author Tim Fisher
@brief For creating a pco2a_a_sample driver.

"""


from mi.core.log import log
from mi.core.versioning import version
from mi.dataset.parser.pco2a_a_sample import Pco2aADclParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.pco2a_a_sample import Pco2aADclParticleClassKey, \
    Pco2aADclInstrumentDataParticleAir, \
    Pco2aADclInstrumentDataParticleWater

MODULE_NAME = 'mi.dataset.parser.pco2a_a_sample'

PARTICLE_CLASSES = {
    Pco2aADclParticleClassKey.AIR_PARTICLE_CLASS: Pco2aADclInstrumentDataParticleAir,
    Pco2aADclParticleClassKey.WATER_PARTICLE_CLASS: Pco2aADclInstrumentDataParticleWater
}


def process(source_file_path, particle_data_handler, particle_classes):

    with open(source_file_path, "r") as stream_handle:
        parser = Pco2aADclParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: None,
             DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: particle_classes},
            stream_handle,
            lambda ex: particle_data_handler.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

@version("0.1.0")
def parse(unused, source_file_path, particle_data_handler):
    process(source_file_path, particle_data_handler, PARTICLE_CLASSES)

    return particle_data_handler
