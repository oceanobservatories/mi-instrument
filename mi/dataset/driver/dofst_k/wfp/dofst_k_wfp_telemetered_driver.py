#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

import os

from mi.core.versioning import version
from mi.dataset.dataset_driver import SimpleDatasetDriver, ParticleDataHandler
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.wfp_c_file_common import WfpCFileCommonConfigKeys
from mi.dataset.parser.dofst_k_wfp_particles import \
    DofstKWfpTelemeteredDataParticle, \
    DofstKWfpTelemeteredMetadataParticle, \
    DofstKWfpDataParticleKey

from mi.core.log import get_logger

log = get_logger()

__author__ = 'jroy'


@version("0.0.3")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # Let this be None until we modify the global E file driver to get these tuples
    e_file_time_pressure_tuples = None

    # Parse the ctd file and use the e_file_time_pressure_tuples to generate
    # the internal timestamps of the particles
    with open(source_file_path, 'rb') as stream_handle:
        driver = DofstKWfpTelemeteredDriver(
            unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)
        driver.processFileStream()

    return particle_data_handler


class DofstKWfpTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived dofst_k_wfp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def __init__(self, unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples):
        self._e_file_time_pressure_tuples = e_file_time_pressure_tuples

        super(DofstKWfpTelemeteredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):

        filesize = os.path.getsize(stream_handle.name)

        config = {
            WfpCFileCommonConfigKeys.PRESSURE_FIELD_C_FILE: DofstKWfpDataParticleKey.PRESSURE,
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
                                 filesize,
                                 self._e_file_time_pressure_tuples)

        return parser
