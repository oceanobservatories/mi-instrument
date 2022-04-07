#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
import os

from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.parser.vel3d_k_wfp import Vel3dKWfpParser, Vel3dKWfpDataParticleType
from mi.dataset.driver.wfp_common.wfp_c_file_driver import WfpAFileDriver
from mi.dataset.driver.flort_kn.stc_imodem.flort_kn__stc_imodem_driver import FlortKnStcImodemDriver
from mi.core.versioning import version

__author__ = 'kustert'

log = get_logger()


@version("0.4.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # Get the flort file name from the vel3d file name
    head, tail = os.path.split(source_file_path)
    e_tail = tail.replace('A', 'E', 1)

    if e_tail == tail:
        log.error('Could not generate e file name')
        return particle_data_handler

    flort_source_file_path = os.path.join(head, e_tail)

    #  Get a list of (time, pressure) tuples from the "E" file using the flort driver
    try:
        with open(flort_source_file_path, 'rb') as flort_stream_handle:
            driver = FlortKnStcImodemDriver(unused, flort_stream_handle, ParticleDataHandler())
            e_file_time_pressure_tuples = driver.get_time_pressure_tuples()
    except Exception as e:
        log.error(e)
        return particle_data_handler

    if not e_file_time_pressure_tuples:
        log.error('Time-Pressure tuples not extracted from %s', flort_source_file_path)
        return particle_data_handler

    # Parse the ctd file and use the e_file_time_pressure_tuples to generate
    # the internal timestamps of the particles
    with open(source_file_path, 'rb') as stream_handle:
        driver = Vel3dKWfpRecoveredDriver(
            unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)
        driver.processFileStream()

    return particle_data_handler


class Vel3dKWfpRecoveredDriver(WfpAFileDriver):
    """
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):

        # The parser is self aware of the particle classes
        # so no need to specify them in the parser_config here.
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        parser = Vel3dKWfpParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser

    def pressure_containing_data_particle_stream(self):
        return Vel3dKWfpDataParticleType.INSTRUMENT_PARTICLE

    def pressure_containing_data_particle_field(self):
        return 'vel3d_k_pressure'
