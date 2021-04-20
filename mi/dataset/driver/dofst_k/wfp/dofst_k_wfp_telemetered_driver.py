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
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.dofst_k_wfp_particles import \
    DofstKWfpTelemeteredDataParticle, \
    DofstKWfpTelemeteredMetadataParticle, \
    DofstKWfpDataParticleKey, \
    DataParticleType
from mi.dataset.driver.flort_kn.stc_imodem.flort_kn__stc_imodem_driver import FlortKnStcImodemDriver

from mi.core.log import get_logger

log = get_logger()

__author__ = 'jroy'


@version("0.0.4")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # Get the flort file name from the ctd file name
    head, tail = os.path.split(source_file_path)
    e_tail = tail.replace('C', 'E')

    if e_tail == tail:
        log.error('Could not generate e file name')
        return particle_data_handler

    flort_source_file_path = os.path.join(head, e_tail)

    # Parse the flort file to get a list of (time, pressure) tuples.
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
        driver = DofstKWfpTelemeteredDriver(
            unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)
        driver.processFileStream()

    return particle_data_handler


class DofstKWfpTelemeteredDriver(WfpCFileDriver):
    """
    Derived dofst_k_wfp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        filesize = os.path.getsize(stream_handle.name)

        config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofs_k_wfp_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'instrument_data_particle_class': DofstKWfpTelemeteredDataParticle,
                'metadata_particle_class': DofstKWfpTelemeteredMetadataParticle
            }
        }
        parser = DofstKWfpParser(config,
                                 None,
                                 stream_handle,
                                 lambda state, ingested: None,
                                 lambda data: None,
                                 self._exception_callback,
                                 filesize)

        return parser

    def pressure_containing_data_particle_stream(self):
        return DataParticleType.TELEMETERED_DATA

    def pressure_containing_data_particle_field(self):
        return DofstKWfpDataParticleKey.PRESSURE
