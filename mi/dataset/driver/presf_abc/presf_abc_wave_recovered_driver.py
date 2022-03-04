#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2016 Raytheon Co.
##
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.presf_abc import PresfAbcParser
from mi.core.versioning import version

__author__ = 'msteiner'


@version("1.0.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to
    be parsed
    :param particle_data_handler Java Object to consume the output of the
    parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = PresfAbcWaveRecoveredDriver(unused,
                                         stream_handle,
                                         particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class PresfAbcWaveRecoveredDriver(SimpleDatasetDriver):
    """
    Derived presf_abc_wave driver class
    The method below passes "True" as the last arg to the parser constructor to instruct the parser to
    not only generate the tide data, but also generate the wave data.
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser = PresfAbcParser(stream_handle, self._exception_callback, True)

        return parser
