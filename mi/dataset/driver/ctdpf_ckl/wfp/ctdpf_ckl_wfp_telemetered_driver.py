#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

from mi.core.versioning import version

from mi.dataset.driver.ctdpf_ckl.wfp.coastal_ctdpf_ckl_wfp_telemetered_driver import parse as parse_impl

@version("0.0.3")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    # This driver should only be called for coastals so just use the implementation
    # of the parse function from the coastal driver file
    return parse_impl(unused, source_file_path, particle_data_handler)
