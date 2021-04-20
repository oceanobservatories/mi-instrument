#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Jeff Roy'


from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.wfp_common.wfp_e_file_driver import WfpEFileDriver

from mi.dataset.parser.global_wfp_e_file_parser import GlobalWfpEFileParser
from mi.dataset.parser.flord_l_wfp import DataParticleType as FlordLWfpDataParticleType
from mi.dataset.parser.flord_l_wfp import FlordLWfpInstrumentParserDataParticleKey
from mi.core.versioning import version

log = get_logger()


@version("15.6.2")
def parse(unused, source_file_path, particle_data_handler):

    with open(source_file_path, 'r') as stream_handle:

        driver = FlordLWfpRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class FlordLWfpRecoveredDriver(WfpEFileDriver):

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpInstrumentParserDataParticle'
        }

        parser = GlobalWfpEFileParser(parser_config, None,
                                      stream_handle,
                                      lambda state, ingested: None,
                                      lambda data: log.trace("Found data: %s", data),
                                      self._exception_callback)

        return parser

    def pressure_containing_data_particle_stream(self):
        return FlordLWfpDataParticleType.INSTRUMENT

    def pressure_containing_data_particle_field(self):
        return FlordLWfpInstrumentParserDataParticleKey.PRESSURE
