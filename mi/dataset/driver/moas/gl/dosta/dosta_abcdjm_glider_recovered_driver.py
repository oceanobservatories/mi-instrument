##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

import os

from mi.logging import config

from mi.dataset.driver.moas.gl.dosta.driver_common import DostaAbcdjmGliderDriver

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaRecoveredDataParticle',
    }

    driver = DostaAbcdjmGliderDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
