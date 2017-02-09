#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_cpm_eng.cpm.cg_cpm_eng_cpm_common_driver import CgCpmEngCpmDriver


@version("15.7.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_cpm_eng_cpm',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'CgCpmEngCpmRecoveredDataParticle'
    }

    driver = CgCpmEngCpmDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
