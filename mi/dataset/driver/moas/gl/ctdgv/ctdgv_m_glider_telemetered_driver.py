##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.driver.moas.gl.ctdgv.driver_common import CtdgvMGliderDriver

from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.glider',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CtdgvTelemeteredDataParticle'
    }

    driver = CtdgvMGliderDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
