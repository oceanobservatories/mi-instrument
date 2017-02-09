#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Ronald Ronquillo"

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.adcpt_acfgm.dcl.pd8.adcpt_acfgm_dcl_pd8_driver_common import \
    AdcptAcfgmDclPd8Driver, MODULE_NAME, ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
        DataSetDriverConfigKeys.PARTICLE_CLASS: ADCPT_ACFGM_DCL_PD8_TELEMETERED_PARTICLE_CLASS,
    }

    driver = AdcptAcfgmDclPd8Driver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
