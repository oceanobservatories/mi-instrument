#!/usr/bin/env python

"""
@package mi.dataset.driver.pred_tide
@file mi-dataset/mi/dataset/driver/pred_tide/instr_pred_tide_driver.py
@author Mark Steiner
@brief Driver to upload instrument-specific predicted tide files

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.instr_pred_tide import InstrPredictedTideParser
from mi.core.versioning import version
from mi.dataset.dataset_driver import ProcessingInfoKey

__author__ = 'Mark Steiner'
__license__ = 'Apache 2.0'

@version("0.1.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """
    with open(source_file_path, 'rb') as stream_handle:

        driver = InstrPredTideDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    # The raw datafiles have already been determined to be correct so tell the caller that
    # the timestamps have already been validated and further validation is not needed.
    particle_data_handler.setProcessingInfo(ProcessingInfoKey.TIMESTAMPS_VALIDATED, True)

    return particle_data_handler


class InstrPredTideDriver(SimpleDatasetDriver):
    """
    Derived class to instantiate the actual file parser
    """
    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.instr_pred_tide',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'InstrPredictedTideDataParticle'
        }

        parser = InstrPredictedTideParser(parser_config, stream_handle, self._exception_callback)
        return parser
