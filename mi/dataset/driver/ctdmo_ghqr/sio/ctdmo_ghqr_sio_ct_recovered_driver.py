#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdmo_ghqr.sio
@file mi-dataset/mi/dataset/driver/ctdmo_ghqr/sio/ctdmo_ghqr_sio_ct_recovered_driver.py
@author Vipul Lakhani
@brief Driver for the ctdmo_ghqr_sio instrument ct recovered data
"""
from mi.core.log import get_logger

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.ctdmo_ghqr_sio import CtdmoGhqrSioRecoveredCoAndCtParser
from mi.core.versioning import version


@version("15.7.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """
    log = get_logger()

    with open(source_file_path, 'rb') as stream_handle:

        def exception_callback(exception):
                log.debug("Exception: %s", exception)
                particle_data_handler.setParticleDataCaptureFailure()

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdmo_ghqr_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['CtdmoGhqrRecoveredHostInstrumentDataParticle']
        }

        parser = CtdmoGhqrSioRecoveredCoAndCtParser(parser_config, stream_handle, exception_callback)

        # create and instance of the concrete driver class defined below
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
