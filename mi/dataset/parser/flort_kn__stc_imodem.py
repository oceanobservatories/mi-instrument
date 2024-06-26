#!/usr/bin/env python

"""
@package mi.dataset.parser.flort_kn__stc_imodem
@file marine-integrations/mi/dataset/parser/flort_kn__stc_imodem.py
@author Emily Hahn
@brief Parser for the FLORT_KN__STC_IMODEM dataset driver (both recovered and telemetered)
Release notes:

initial release
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import copy
import ntplib
import struct

from mi.core.log import get_logger ; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.dataset.parser.WFP_E_file_common import WfpEFileParser, StateKey, SAMPLE_BYTES


class DataParticleType(BaseEnum):
    FLORT_KN_INSTRUMENT_TELEMETERED = 'flort_sample'
    FLORT_KN_INSTRUMENT_RECOVERED = 'flort_sample'
    FLORT_KN_INSTRUMENT = 'flort_sample'


class Flort_kn__stc_imodemParserDataParticleKey(BaseEnum):
    TIMESTAMP = 'wfp_timestamp'
    PRESSURE_DEPTH = 'pressure_depth'
    RAW_SIGNAL_BETA = 'raw_signal_beta'
    RAW_SIGNAL_CHL = 'raw_signal_chl'
    RAW_SIGNAL_CDOM = 'raw_signal_cdom'


class Flort_kn_stc_imodemParserDataParticleAbstract(DataParticle):
    """
    Parent class for the recovered and instrument particles (Flort_kn__stc_imodemParserDataParticleRecovered and
    Flort_kn__stc_imodemParserDataParticle respectively)
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        fields_prof = struct.unpack('>I f f f f h h h', self.raw_data)

        result = [self._encode_value(Flort_kn__stc_imodemParserDataParticleKey.PRESSURE_DEPTH, fields_prof[3], float),
                  self._encode_value(Flort_kn__stc_imodemParserDataParticleKey.RAW_SIGNAL_BETA, fields_prof[5], int),
                  self._encode_value(Flort_kn__stc_imodemParserDataParticleKey.RAW_SIGNAL_CHL, fields_prof[6], int),
                  self._encode_value(Flort_kn__stc_imodemParserDataParticleKey.RAW_SIGNAL_CDOM, fields_prof[7], int)]

        log.debug('Flort_kn__stc_imodemParserDataParticleKey: particle=%s', result)
        return result


class Flort_kn_stc_imodemParserDataParticleRecovered(Flort_kn_stc_imodemParserDataParticleAbstract):
    """
     The FLORT_KN__STC_IMODEM data set recovered particle
    """

    _data_particle_type = DataParticleType.FLORT_KN_INSTRUMENT_RECOVERED


class Flort_kn_stc_imodemParserDataParticleTelemetered(Flort_kn_stc_imodemParserDataParticleAbstract):
    """
     The FLORT_KN__STC_IMODEM data set telemetered particle
    """
    _data_particle_type = DataParticleType.FLORT_KN_INSTRUMENT_TELEMETERED


class Flort_kn_stc_imodemParserDataParticle(Flort_kn_stc_imodemParserDataParticleAbstract):
    """
     The FLORT_KN__STC_IMODEM data set telemetered particle
    """
    _data_particle_type = DataParticleType.FLORT_KN_INSTRUMENT


class Flort_kn_stc_imodemParser(WfpEFileParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 *args, **kwargs):

        # Do not accept samples until we find the first non-zero pressure in the file
        self._accept_samples = False
        self._pressure_precision = 0.001

        super(Flort_kn_stc_imodemParser, self).__init__(config,
                                                        state,
                                                        stream_handle,
                                                        state_callback,
                                                        publish_callback,
                                                        *args, **kwargs)

    def parse_record(self, record):
        """
        parse a FLORT_KN data sample into data particle from the input record
        """
        result_particle = []
        if len(record) >= SAMPLE_BYTES:
            # pull out the timestamp for this record

            fields = struct.unpack('>I', record[:4])
            timestamp = int(fields[0])
            self._timestamp = float(ntplib.system_to_ntp_time(timestamp))
            log.debug("Converting record timestamp %f to ntp timestamp %f", timestamp, self._timestamp)
            sample = self._extract_sample(self._particle_class, None, record, internal_timestamp=self._timestamp)
            if sample:
                # create particle
                log.trace("Extracting sample %s with read_state: %s", sample, self._read_state)
                self._increment_state(SAMPLE_BYTES)

                if not self._accept_samples:
                    # Get the pressure from the list of "values" in the sample
                    pressure_name_value_pair = [i for i in sample._values
                        if i[DataParticleKey.VALUE_ID] == Flort_kn__stc_imodemParserDataParticleKey.PRESSURE_DEPTH]
                    # Accept this and subsequent samples when the first non-zero pressure is encountered in the file
                    if pressure_name_value_pair and \
                            pressure_name_value_pair[0][DataParticleKey.VALUE] > self._pressure_precision:
                        self._accept_samples = True

                if self._accept_samples:
                    result_particle = (sample, copy.copy(self._read_state))

        return result_particle
