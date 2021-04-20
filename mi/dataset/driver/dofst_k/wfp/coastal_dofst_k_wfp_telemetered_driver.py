#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2020 Raytheon Co.
##

from mi.core.versioning import version

from mi.dataset.driver.dofst_k.wfp.dofst_k_wfp_telemetered_driver import parse as parse_impl

from mi.core.log import get_logger

log = get_logger()

__author__ = 'msteiner'


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # This "coastal" driver file was really not necessary since all dofst are costal.
    # So just point to the implementation in the original driver file.
    # This file cam probably be removed after coordinating with those who ingest
    # using this driver.
    return parse_impl(unused, source_file_path, particle_data_handler)
