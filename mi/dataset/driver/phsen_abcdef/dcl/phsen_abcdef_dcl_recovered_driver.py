##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.logging import config

from mi.dataset.driver.phsen_abcdef.dcl.driver_common import PhsenAbcdefDclDriver

from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclMetadataRecoveredDataParticle
from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclInstrumentRecoveredDataParticle
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

__author__ = "Nick Almonte"


@version('0.0.3')
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.driver.phsen_abcdef.dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'metadata_particle_class_key': PhsenAbcdefDclMetadataRecoveredDataParticle,
            'data_particle_class_key': PhsenAbcdefDclInstrumentRecoveredDataParticle,
        }
    }

    driver = PhsenAbcdefDclDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
