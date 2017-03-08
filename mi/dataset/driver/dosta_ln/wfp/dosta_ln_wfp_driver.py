#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_ln.wfp
@file marine-integrations/mi/dataset/driver/dosta_ln/wfp/dosta_ln_wfp.py
@author Tapana Gupta
@brief Driver for the dosta_ln_wfp instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger

from mi.dataset.parser.WFP_E_file_common import StateKey
from mi.dataset.parser.dosta_ln_wfp import DostaLnWfpParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):

    log = get_logger()

    with open(source_file_path, "r") as stream_handle:

        def exception_callback(exception):
                log.debug("Exception: %s", exception)
                particle_data_handler.setParticleDataCaptureFailure()

        parser = DostaLnWfpParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_ln_wfp',
             DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaLnWfpInstrumentParserDataParticle'},
             {StateKey.POSITION: 0},
             stream_handle,
             lambda state, ingested: None,
             lambda data: None,
             exception_callback
        )
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()
    return particle_data_handler

