##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.wfp_eng__stc_imodem import WfpEngStcImodemParser


class WfpEngStcImodemDriver:

    def __init__(self, source_file_path, particle_data_handler, parser_config):
        
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        
        log = get_logger()

        with open(self._source_file_path,"rb") as file_handle:

            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()
                    
            parser = WfpEngStcImodemParser(self._parser_config,
                                           None, file_handle,
                                           lambda state, ingested: None,
                                           lambda data: None,
                                           exception_callback)
    
            driver = DataSetDriver(parser, self._particle_data_handler)

            driver.processFileStream()

        return self._particle_data_handler

