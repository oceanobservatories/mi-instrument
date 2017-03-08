#!/usr/bin/env python

"""
@package mi.dataset.driver.velpt_j.cspp
@file mi/dataset/driver/velpt_j/cspp/velpt_j_cspp_telemetered_driver.py
@author Emily Hahn
@brief Driver for the telemetered velpt series j instrument through cspp
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.velpt_j_cspp import VelptJCsppParser, VelptJCsppMetadataTelemeteredDataParticle, \
    VelptJCsppInstrumentTelemeteredDataParticle

from mi.dataset.parser.cspp_base import METADATA_PARTICLE_CLASS_KEY, DATA_PARTICLE_CLASS_KEY
from mi.core.versioning import version


@version("15.6.2")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = VelptJCsppTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class VelptJCsppTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived velpt j cspp driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: VelptJCsppMetadataTelemeteredDataParticle,
                DATA_PARTICLE_CLASS_KEY: VelptJCsppInstrumentTelemeteredDataParticle,
            }
        }

        return VelptJCsppParser(parser_config, stream_handle, self._exception_callback)
