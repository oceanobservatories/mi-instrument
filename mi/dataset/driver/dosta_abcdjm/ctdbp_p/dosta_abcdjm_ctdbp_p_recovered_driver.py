#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_abcdjm.ctdbp_p
@file mi-dataset/mi/dataset/driver/dosta_abcdjm/ctdbp_p/dosta_abcdjm_ctdbp_p_recovered_driver.py
@author Jeff Roy, Rene Gelinas
@brief Driver for the dosta_abcdjm attached to the ctdbp_p instrument (Recovered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_p import CtdbpPCommonParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.ctdbp_p'

DOSTA_RECOV_CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaAbcdjmCtdbpPRecoveredDataParticle'
}


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path  This is the full path and filename of the file to be parsed
    :param particle_data_handler  Consumes the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = DostaAbcdjmCtdbpPRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class DostaAbcdjmCtdbpPRecoveredDriver(SimpleDatasetDriver):
    """
    Derived ctdbp_p driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = CtdbpPCommonParser(DOSTA_RECOV_CONFIG,
                                    stream_handle,
                                    self._exception_callback)

        return parser
