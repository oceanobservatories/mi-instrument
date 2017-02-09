#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcpt_acfgm.dcl.pd0.adcpt_acfgm_dcl_pd0_driver_common import AdcptAcfgmDclPd0Driver

__author__ = "Jeff Roy"


@version("15.8.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'velocity': 'Velocity',
            'engineering': 'Engineering',
            'config': 'Config',
        }
    }

    driver = AdcptAcfgmDclPd0Driver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
