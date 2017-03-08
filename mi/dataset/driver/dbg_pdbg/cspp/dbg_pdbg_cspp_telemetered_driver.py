#!/usr/bin/env python

"""
@package mi.dataset.driver.dbg_pdbg.cspp
@file mi/dataset/driver/dbg_pdbg/cspp/dbg_pdbg_cspp_telemetered_driver.py
@author Jeff Roy
@brief Driver for the dbg_pdbg_cspp instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver

from mi.dataset.parser.dbg_pdbg_cspp import \
    DbgPdbgCsppParser, \
    DbgPdbgTelemeteredBatteryParticle, \
    DbgPdbgTelemeteredGpsParticle, \
    DbgPdbgMetadataTelemeteredDataParticle, \
    BATTERY_STATUS_CLASS_KEY, \
    GPS_ADJUSTMENT_CLASS_KEY

from mi.dataset.parser.cspp_base import \
    METADATA_PARTICLE_CLASS_KEY
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
    with open(source_file_path, 'r') as stream_handle:
        # create and instance of the concrete driver class defined below
        driver = DbgPdbgCsppTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DbgPdbgCsppTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived dbg_pdbg_cspp driver class
    All this needs to do is create a concrete _build_parser method
    """
    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                METADATA_PARTICLE_CLASS_KEY: DbgPdbgMetadataTelemeteredDataParticle,
                BATTERY_STATUS_CLASS_KEY: DbgPdbgTelemeteredBatteryParticle,
                GPS_ADJUSTMENT_CLASS_KEY: DbgPdbgTelemeteredGpsParticle
            }
        }

        parser = DbgPdbgCsppParser(parser_config, stream_handle,
                                   self._exception_callback)

        return parser
