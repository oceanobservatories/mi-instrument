#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.cg_stc_eng.stc.mopak_o_dcl_common_driver import MopakDriver
from mi.dataset.parser.mopak_o_dcl import \
    MopakODclAccelParserDataParticle, \
    MopakODclRateParserDataParticle, \
    MopakParticleClassType


@version("0.0.4")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.mopak_o_dcl',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        # particle_class configuration does nothing for multi-particle parsers
        # put the class names in specific config parameters so the parser can get them
        # use real classes as objects instead of strings to make it easier
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT:
            {MopakParticleClassType.ACCEL_PARTICLE_CLASS: MopakODclAccelParserDataParticle,
             MopakParticleClassType.RATE_PARTICLE_CLASS: MopakODclRateParserDataParticle}
    }

    driver = MopakDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
