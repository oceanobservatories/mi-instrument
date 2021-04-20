#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.versioning import version
from mi.dataset.driver.wfp_common.wfp_c_file_driver import WfpCFileDriver
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdpf_ckl_wfp import CtdpfCklWfpParser, \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY
from mi.dataset.parser.ctdpf_ckl_wfp_particles import DataParticleType as CtdpfCklWfpDataParticleType
from mi.dataset.parser.ctdpf_ckl_wfp_particles import \
    CtdpfCklWfpRecoveredDataParticle, \
    CtdpfCklWfpRecoveredMetadataParticle, \
    CtdpfCklWfpDataParticleKey
from mi.dataset.driver.flord_l_wfp.flord_l_wfp_recovered_driver import FlordLWfpRecoveredDriver

from mi.core.log import get_logger

log = get_logger()


class CtdpfCklWfpRecoveredDriver(WfpCFileDriver):
    """
    Derived wc_wm_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: CtdpfCklWfpRecoveredMetadataParticle,
                DATA_PARTICLE_CLASS_KEY: CtdpfCklWfpRecoveredDataParticle
            }
        }

        file_size = os.path.getsize(stream_handle.name)

        parser = CtdpfCklWfpParser(parser_config,
                                   stream_handle,
                                   self._exception_callback,
                                   file_size)

        return parser

    def pressure_containing_data_particle_stream(self):
        return CtdpfCklWfpDataParticleType.RECOVERED_DATA

    def pressure_containing_data_particle_field(self):
        return CtdpfCklWfpDataParticleKey.PRESSURE


@version("0.0.4")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # Get the flord file name from the ctd file name
    head, tail = os.path.split(source_file_path)
    e_tail = tail.replace('C', 'E')

    if e_tail == tail:
        log.error('Could not generate e file name')
        return particle_data_handler

    flord_source_file_path = os.path.join(head, e_tail)

    #  Get a list of (time, pressure) tuples from the "E" file using the flord driver
    try:
        with open(flord_source_file_path, 'rb') as flord_stream_handle:
            driver = FlordLWfpRecoveredDriver(unused, flord_stream_handle, ParticleDataHandler())
            e_file_time_pressure_tuples = driver.get_time_pressure_tuples()
    except Exception as e:
        log.error(e)
        return particle_data_handler

    if not e_file_time_pressure_tuples:
        log.error('Time-Pressure tuples not extracted from %s', flord_source_file_path)
        return particle_data_handler

    # Parse the ctd file and use the e_file_time_pressure_tuples to generate
    # the internal timestamps of the particles
    with open(source_file_path, 'rb') as stream_handle:
        driver = CtdpfCklWfpRecoveredDriver(
            unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)
        driver.processFileStream()

    return particle_data_handler
