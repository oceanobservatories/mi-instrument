#!/usr/bin/env python

import os

from mi.logging import log
from mi.dataset.parser.zplsc_c import ZplscCParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.zplsc_c.resource import RESOURCE_PATH

__author__ = 'Rene Gelinas'

MODULE_NAME = 'mi.dataset.parser.zplsc_c'
CLASS_NAME = 'ZplscCRecoveredDataParticle'
config = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
}


def create_zplsc_c_parser(file_handle):
    """
    This function creates a zplsc-c parser for recovered data.
    @param file_handle - File handle of the ZPLSC_C raw data.
    """
    return ZplscCParser(config, file_handle, rec_exception_callback)


def file_path(filename):
    log.debug('resource path = %s, file name = %s', RESOURCE_PATH, filename)
    return os.path.join(RESOURCE_PATH, filename)


def rec_exception_callback(exception):
    """
    Call back method to for exceptions
    @param exception - Exception that occurred
    """
    log.info("Exception occurred: %s", exception.message)


def zplsc_c_echogram_test():
    with open(file_path('160501.01A')) as in_file:
        parser = create_zplsc_c_parser(in_file)
        parser.create_echogram()


if __name__ == '__main__':
    zplsc_c_echogram_test()
