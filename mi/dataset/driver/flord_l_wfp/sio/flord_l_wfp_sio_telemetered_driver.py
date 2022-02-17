#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Mark Worden'

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.wfp_common.wfp_e_file_driver import WfpEFileDriver

from mi.dataset.parser.flord_l_wfp_sio import FlordLWfpSioParser
from mi.dataset.parser.flord_l_wfp_sio import DataParticleType as FlordLWfpSioDataParticleType
from mi.dataset.parser.flord_l_wfp_sio import FlordLWfpSioDataParticleKey
from mi.core.versioning import version


@version("15.6.2")
def parse(unused, source_file_path, particle_data_handler):

    with open(source_file_path, 'rb') as stream_handle:

        driver = FlordLWfpSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class FlordLWfpSioTelemeteredDriver(WfpEFileDriver):

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpSioDataParticle'
        }

        parser = FlordLWfpSioParser(parser_config,
                                    stream_handle,
                                    self._exception_callback)

        return parser

    def pressure_containing_data_particle_stream(self):
        return FlordLWfpSioDataParticleType.SAMPLE

    def pressure_containing_data_particle_field(self):
        return FlordLWfpSioDataParticleKey.PRESSURE
