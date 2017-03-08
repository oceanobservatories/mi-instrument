#!/usr/bin/env python

"""
@package mi.dataset.driver.camds
@file mi/dataset/driver/camds/camds_abc_driver.py
@author Dan Mergens
@brief Dataset driver for parsing CAMDS HTML image metadata.

Release notes:

- Initial Release

Subject Matter Experts:
- Linda Fayler (OSU)
- Jonathan Howland (WHOI)
- Jonathan Fram (OSU)
- Michael Vardaro (RU)
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.camds import CamdsHtmlParser
from mi.core.versioning import version


@version('1.0.0')
def parse(unused, source_file_path, particle_data_handler):
    """
    Wrapper function for calls from uFrame
    :param unused:
    :param source_file_path:  Full path and filename of the html file to be parsed.
    :param particle_data_handler:  Java object to consume the output of the parser.
    :return:  particleDataHdlrObj
    """

    with open(source_file_path, 'rU') as stream_handle:

        # create an instance of the concrete driver class
        driver = CamdsHtmlDriver(None, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CamdsHtmlDriver(SimpleDatasetDriver):
    """
    Implements _build_parser to handle input file from uFrame.
    """
    def _build_parser(self, stream_handle):

        parser = CamdsHtmlParser(stream_handle, self._exception_callback)

        return parser
