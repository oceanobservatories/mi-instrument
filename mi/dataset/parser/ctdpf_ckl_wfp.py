#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdpf_ckl_wfp
@file marine-integrations/mi/dataset/parser/ctdpf_ckl_wfp.py
@author cgoodrich
@brief Parser for the ctdpf_ckl_wfp dataset driver
Release notes:

Initial Release
"""

__author__ = 'cgoodrich'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()


from mi.dataset.parser.wfp_c_file_common import WfpCFileCommonParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the metadata particle class
METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'instrument_data_particle_class'


class CtdpfCklWfpParser(WfpCFileCommonParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 file_size):

        log.info(config)
        particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
        self._instrument_data_particle_class = particle_classes_dict.get('instrument_data_particle_class')
        self._metadata_particle_class = particle_classes_dict.get('metadata_particle_class')

        super(CtdpfCklWfpParser, self).__init__(config,
                                                None,
                                                stream_handle,
                                                lambda state, ingested: None,
                                                lambda data: None,
                                                exception_callback,
                                                file_size)

    def extract_metadata_particle(self, raw_data, timestamp):
        """
        Class for extracting the metadata data particle
        @param raw_data raw data to parse, in this case a tuple of the time string to parse and the number of records
        @param timestamp timestamp in NTP64
        """
        sample = self._extract_sample(self._metadata_particle_class, None, raw_data, internal_timestamp=timestamp)
        return sample

    def extract_data_particle(self, raw_data, timestamp):
        """
        Class for extracting the data sample data particle
        @param raw_data the raw data to parse
        @param timestamp the timestamp in NTP64
        """
        sample = self._extract_sample(self._instrument_data_particle_class, None, raw_data, internal_timestamp=timestamp)
        return sample
