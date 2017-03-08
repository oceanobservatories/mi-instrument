#!/usr/local/bin/python2.7
# #
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
# #

from mi.core.log import get_logger

from mi.dataset.parser.cg_cpm_eng_cpm import CgCpmEngCpmParser
from mi.dataset.dataset_driver import DataSetDriver


class CgCpmEngCpmDriver:
    def __init__ (self, source_file_path, particle_data_handler, parser_config):
        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config
    
    def process(self):
        
        log = get_logger()
        
        def exception_callback(exception):
            log.debug("ERROR: %r", exception)
            self._particle_data_handler.setParticleDataCaptureFailure()

        with open(self._source_file_path, 'r') as stream_handle:
            parser = CgCpmEngCpmParser(self._parser_config, stream_handle,
                                       exception_callback)
            
            driver = DataSetDriver(parser, self._particle_data_handler)
            driver.processFileStream()    
        
        return self._particle_data_handler
  
