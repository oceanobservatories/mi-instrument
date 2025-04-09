#!/usr/bin/env python

"""
@package mi.dataset.driver.plims_a_hdr
@file mi-dataset/mi/dataset/driver/plims_a/plims_a_hdr_driver.py
@author Joffrey Peters
@brief driver for the plims_a instrument
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.plims_a_hdr import PlimsAHdrParser
from mi.dataset.parser.plims_a_particles import PlimsAHdrClassKey


def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:
        PlimsAHdrRecoveredDriver(unused, stream_handle, particle_data_handler).processFileStream()

    return particle_data_handler


class PlimsAHdrRecoveredDriver(SimpleDatasetDriver):
    """
        The plims_a driver class extends the SimpleDatasetDriver.
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        super(PlimsAHdrRecoveredDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.plims_a_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                PlimsAHdrClassKey.ENGINEERING: 'PlimsAHdrEngineeringDataParticle',
                PlimsAHdrClassKey.INSTRUMENT: 'PlimsAHdrInstrumentDataParticle',
            }
        }

        parser = PlimsAHdrParser(parser_config, stream_handle, self._exception_callback)

        return parser
