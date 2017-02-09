#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

"""
@package mi.dataset.driver.vel3d_a.mmp_cds.vel3d_a_mmp_cds_recovered_driver
@file mi-dataset/mi/dataset/driver/vel3d_a/mmp_cds/vel3d_a_mmp_cds_recovered_driver.py
@author Jeff Roy
@brief Driver for the vel3d_a_mmp_cds instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.mmp_cds_base import MmpCdsParser
from mi.core.versioning import version


@version("0.0.3")
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
        driver = Vel3dAMmpCdsRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class Vel3dAMmpCdsRecoveredDriver(SimpleDatasetDriver):
    """
    Derived vel3d_a_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_a_mmp_cds',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'Vel3dAMmpCdsParserDataParticle'
        }

        parser = MmpCdsParser(parser_config, stream_handle, self._exception_callback)

        return parser


