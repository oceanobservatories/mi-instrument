#!/usr/bin/env python

"""
@package mi.dataset.driver.velpt_ab.dcl
@file mi-dataset/mi/dataset/driver/velpt_ab/dcl/velpt_ab_dcl_telemetered_driver.py
@author Joe Padula
@brief Telemetered driver for the velpt_ab_dcl instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.velpt_ab_dcl import VelptAbDclParser, \
    VelptAbDclParticleClassKey
from mi.dataset.parser.velpt_ab_dcl_particles import VelptAbDclDiagnosticsDataParticle, \
    VelptAbDclInstrumentDataParticle, \
    VelptAbDclDiagnosticsHeaderParticle
from mi.core.versioning import version


@version("15.7.1")
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
        driver = VelptAbDclTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class VelptAbDclTelemeteredDriver(SimpleDatasetDriver):
    """
    The velpt_ab_dcl driver class extends the SimpleDatasetDriver.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.velpt_ab_dcl_particles',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                VelptAbDclParticleClassKey.METADATA_PARTICLE_CLASS: VelptAbDclDiagnosticsHeaderParticle,
                VelptAbDclParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS: VelptAbDclDiagnosticsDataParticle,
                VelptAbDclParticleClassKey.INSTRUMENT_PARTICLE_CLASS: VelptAbDclInstrumentDataParticle
            }
        }

        parser = VelptAbDclParser(parser_config,
                                  stream_handle,
                                  self._exception_callback)

        return parser
