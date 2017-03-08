#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'jroy'

import os

from mi.core.versioning import version
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.dofst_k_wfp import DofstKWfpParser
from mi.dataset.parser.dofst_k_wfp_particles import DofstKWfpTelemeteredDataParticle, \
    DofstKWfpTelemeteredMetadataParticle


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = DofstKWfpTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DofstKWfpTelemeteredDriver(SimpleDatasetDriver):
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
