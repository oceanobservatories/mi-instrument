#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdav_nbois.auv
@file mi/dataset/driver/ctdav_nbois/auv/ctdav_nbois_auv_driver.py
@author Rene Gelinas
@brief Driver for the ctdav_nbois_auv instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.ctdav_nbosi_auv import CtdavNbosiAuvParser
from mi.core.versioning import version


@version("0.1.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by uFrame
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = CtdavNboisAuvDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CtdavNboisAuvDriver(SimpleDatasetDriver):
    """
    Create a concrete _build_parser method for the adcpa_n_auv driver.
    """

    def _build_parser(self, stream_handle):

        parser = CtdavNbosiAuvParser(stream_handle,
                                     self._exception_callback)

        return parser
