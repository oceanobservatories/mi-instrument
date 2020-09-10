#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2020 Raytheon Co.
##
import os

from mi.core.versioning import version
from mi.dataset.dataset_driver import SimpleDatasetDriver, ParticleDataHandler
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.wfp_c_file_common import WfpCFileCommonConfigKeys
from mi.dataset.parser.dofst_k_wfp_particles import \
    DofstKWfpRecoveredDataParticle, \
    DofstKWfpRecoveredMetadataParticle, \
    DofstKWfpDataParticleKey
from mi.dataset.driver.flort_kn.stc_imodem.flort_kn__stc_imodem_driver import FlortKnStcImodemDriver

from mi.core.log import get_logger

log = get_logger()

__author__ = 'msteiner'


@version("0.0.1")
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
    tail = tail.replace('C', 'E')
    flort_source_file_path = os.path.join(head, tail)

    # Parse the flort file to get a list of (time, pressure) tuples.
    try:
        flort_particle_data_handler = ParticleDataHandler()
        with open(flort_source_file_path, 'rb') as flort_stream_handle:
            driver = FlortKnStcImodemDriver(unused, flort_stream_handle, flort_particle_data_handler)
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
        driver = DofstKWfpRecoveredDriver(
            unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)
        driver.processFileStream()

    return particle_data_handler


class DofstKWfpRecoveredDriver(SimpleDatasetDriver):
    """
    Derived dofst_k_wfp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def __init__(self, unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples):
        self._e_file_time_pressure_tuples = e_file_time_pressure_tuples

        super(DofstKWfpRecoveredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):

        filesize = os.path.getsize(stream_handle.name)

        config = {
            WfpCFileCommonConfigKeys.PRESSURE_FIELD_C_FILE: DofstKWfpDataParticleKey.PRESSURE,
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dofs_k_wfp_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                'instrument_data_particle_class': DofstKWfpRecoveredDataParticle,
                'metadata_particle_class': DofstKWfpRecoveredMetadataParticle
            }
        }
        parser = DofstKWfpParser(config,
                                 None,
                                 stream_handle,
                                 lambda state, ingested: None,
                                 lambda data: None,
                                 self._exception_callback,
                                 filesize,
                                 self._e_file_time_pressure_tuples)

        return parser
