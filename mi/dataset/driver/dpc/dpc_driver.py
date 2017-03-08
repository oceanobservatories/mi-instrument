#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
from mi.dataset.dataset_driver import DataSetDriver

from mi.dataset.parser.dpc import DeepProfilerParser
from mi.core.log import get_logger
from mi.logging import config
from mi.core.versioning import version
import os

log = get_logger()


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):

    with open(source_file_path, "r") as stream_handle:

        def exception_callback(exception):
                log.debug("Exception: %s", exception)
                particle_data_handler.setParticleDataCaptureFailure()

        parser = DeepProfilerParser({}, stream_handle, exception_callback)
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
