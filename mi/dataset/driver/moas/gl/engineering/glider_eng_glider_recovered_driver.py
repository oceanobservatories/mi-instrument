##
# OOIPLACEHOLDER
#
##


import os

from mi.logging import config

from mi.dataset.driver.moas.gl.engineering.driver_common import GliderEngineeringDriver
from mi.dataset.parser.glider import EngineeringClassKey

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.versioning import version

__author__ = "ehahn"


@version("15.7.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    Initialize the parser configuration and build the driver
    @param unused - python code path from Java
    @param source_file_path - source file from Java
    @param particle_data_handler - particle data handler object from Java
    """

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            EngineeringClassKey.METADATA: 'EngineeringMetadataRecoveredDataParticle',
            EngineeringClassKey.DATA: 'EngineeringRecoveredDataParticle',
            EngineeringClassKey.SCIENCE: 'EngineeringScienceRecoveredDataParticle',
            EngineeringClassKey.GPS: 'GpsPositionDataParticle'
        }
    }

    driver = GliderEngineeringDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
