#!/usr/bin/env python

"""
@package mi.dataset.parser.dosta_abcdjm_sio
@file mi/dataset/parser/dosta_abcdjm_sio.py
@author Emily Hahn
@brief An dosta series a,b,c,d,j,m through sio specific dataset agent parser
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


class DataParticleType(BaseEnum):
    SAMPLE_TELEMETERED = 'dosta_abcdjm_sio_instrument'
    METADATA_TELEMETERED = 'dosta_abcdjm_sio_metadata'
    SAMPLE_RECOVERED = 'dosta_abcdjm_sio_instrument_recovered'
    METADATA_RECOVERED = 'dosta_abcdjm_sio_metadata_recovered'


class DostaAbcdjmSioDataParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = 'sio_controller_timestamp'
    ESTIMATED_OXYGEN = 'estimated_oxygen_concentration'
    ESTIMATED_SATURATION = 'estimated_oxygen_saturation'
    OPTODE_TEMPERATURE = 'optode_temperature'
    CALIBRATED_PHASE = 'calibrated_phase'
    TEMP_COMPENSATED_PHASE = 'temp_compensated_phase'
    BLUE_PHASE = 'blue_phase'
    RED_PHASE = 'red_phase'
    BLUE_AMPLITUDE = 'blue_amplitude'
    RED_AMPLITUDE = 'red_amplitude'
    RAW_TEMP = 'raw_temperature'


class DostaAbcdjmSioMetadataDataParticleKey(BaseEnum):
    PRODUCT_NUMBER = 'product_number'
    SERIAL_NUMBER = 'serial_number'

# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the metadata particle class
METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'data_particle_class'

# regex to match the dosta data, random bytes, 2 integers for product (4831) and serial number,
# followed by 10 floating point numbers all separated by tabs

FLOAT_REGEX_NON_CAPTURE = r'[+-]?[0-9]*\.[0-9]+'
FLOAT_TAB_REGEX = FLOAT_REGEX_NON_CAPTURE + '\t'

DATA_REGEX = b'[x00-xFF]*'   # Any number of random hex values
DATA_REGEX += '(4831)\t'  # product number
DATA_REGEX += '(\d+)\t'  # serial number
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # oxygen content
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # relative air saturation
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # ambient temperature
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # calibrated phase
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # temperature compensated phase
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # phase measurement with blue excitation
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # phase measurement with red excitation
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # amplitude measurement with blue excitation
DATA_REGEX += '(' + FLOAT_TAB_REGEX + ')'  # amplitude measurement with red excitation
DATA_REGEX += '(' + FLOAT_REGEX_NON_CAPTURE + ')'  # raw temperature, voltage from thermistor ( no following tab )
DATA_REGEX += '\x0d\x0a'
DATA_MATCHER = re.compile(DATA_REGEX)

# regex to match the timestamp from the sio header
TIMESTAMP_REGEX = b'[0-9A-Fa-f]{8}'
TIMESTAMP_MATCHER = re.compile(TIMESTAMP_REGEX)


class DostaAbcdjmSioDataParticle(DataParticle):
    """
    Class for parsing data from the DOSTA series a,b,c,d,j,m instrument
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(DostaAbcdjmSioDataParticle, self).__init__(raw_data,
                                                         port_timestamp,
                                                         internal_timestamp,
                                                         preferred_timestamp,
                                                         quality_flag,
                                                         new_sequence)

        posix_time = int(self.raw_data[0], 16)
        self.set_internal_timestamp(unix_time=float(posix_time))

    def _build_parsed_values(self):
        """
        Take in the raw data and turn it into a particle with the appropriate tag.
        """
        # 1st item in raw data tuple is controller timestamp string, 2nd is data_match regex match object
        result = [self._encode_value(DostaAbcdjmSioDataParticleKey.CONTROLLER_TIMESTAMP,
                                     self.raw_data[0],
                                     DostaAbcdjmSioDataParticle.encode_int_16),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.ESTIMATED_OXYGEN,
                                     self.raw_data[1].group(3), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.ESTIMATED_SATURATION,
                                     self.raw_data[1].group(4), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.OPTODE_TEMPERATURE,
                                     self.raw_data[1].group(5), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.CALIBRATED_PHASE,
                                     self.raw_data[1].group(6), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.TEMP_COMPENSATED_PHASE,
                                     self.raw_data[1].group(7), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.BLUE_PHASE,
                                     self.raw_data[1].group(8), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.RED_PHASE,
                                     self.raw_data[1].group(9), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.BLUE_AMPLITUDE,
                                     self.raw_data[1].group(10), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.RED_AMPLITUDE,
                                     self.raw_data[1].group(11), float),
                  self._encode_value(DostaAbcdjmSioDataParticleKey.RAW_TEMP,
                                     self.raw_data[1].group(12), float)]

        return result

    @staticmethod
    def encode_int_16(hex_str):
        return int(hex_str, 16)


class DostaAbcdjmSioMetadataDataParticle(DataParticle):
    """
    Class for parsing data from the DOSTA series a,b,c,d,j,m instrument
    """

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(DostaAbcdjmSioMetadataDataParticle, self).__init__(raw_data,
                                                                 port_timestamp,
                                                                 internal_timestamp,
                                                                 preferred_timestamp,
                                                                 quality_flag,
                                                                 new_sequence)

        # raw data is a tuple, first item contains timestamp string as hex ascii
        posix_time = int(self.raw_data[0], 16)
        self.set_internal_timestamp(unix_time=float(posix_time))

    def _build_parsed_values(self):
        """
        Take in the raw data and turn it into a particle with the appropriate tag.
        """
        # 2nd tuple in raw data contains data match object
        result = [self._encode_value(DostaAbcdjmSioMetadataDataParticleKey.PRODUCT_NUMBER,
                                     self.raw_data[1].group(1), int),
                  self._encode_value(DostaAbcdjmSioMetadataDataParticleKey.SERIAL_NUMBER,
                                     self.raw_data[1].group(2), str)]
        return result


class DostaAbcdjmSioRecoveredDataParticle(DostaAbcdjmSioDataParticle):
    """
    Class for building a DostadParser recovered instrument data particle
    """

    _data_particle_type = DataParticleType.SAMPLE_RECOVERED


class DostaAbcdjmSioTelemeteredDataParticle(DostaAbcdjmSioDataParticle):
    """
    Class for building a DostadParser telemetered instrument data particle
    """

    _data_particle_type = DataParticleType.SAMPLE_TELEMETERED


class DostaAbcdjmSioRecoveredMetadataDataParticle(DostaAbcdjmSioMetadataDataParticle):
    """
    Class for building a DostadParser recovered instrument data particle
    """

    _data_particle_type = DataParticleType.METADATA_RECOVERED


class DostaAbcdjmSioTelemeteredMetadataDataParticle(DostaAbcdjmSioMetadataDataParticle):
    """
    Class for building a DostadParser telemetered instrument data particle
    """

    _data_particle_type = DataParticleType.METADATA_TELEMETERED


class DostaAbcdjmSioParser(SioParser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 *args, **kwargs):
        super(DostaAbcdjmSioParser, self).__init__(config,
                                                   stream_handle,
                                                   exception_callback,
                                                   *args,
                                                   **kwargs)

        self.metadata_sent = False

        # Obtain the particle classes dictionary from the config data
        particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
        # Set the metadata and data particle classes to be used later
        self._metadata_particle_class = particle_classes_dict.get(METADATA_PARTICLE_CLASS_KEY)
        self._data_particle_class = particle_classes_dict.get(DATA_PARTICLE_CLASS_KEY)

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        while chunk is not None:
            header_match = SIO_HEADER_MATCHER.match(chunk)

            if header_match.group(1) == 'DO':

                data_match = DATA_MATCHER.search(chunk)
                if data_match:
                    log.debug('Found data match in chunk %s', chunk[1:32])

                    if not self.metadata_sent:
                        # create the metadata particle
                        # prepend the timestamp from sio mule header to the dosta raw data,
                        # which is stored in header_match.group(3)
                        metadata_sample = self._extract_sample(self._metadata_particle_class,
                                                               None,
                                                               (header_match.group(3), data_match),
                                                               None)
                        if metadata_sample:
                            result_particles.append(metadata_sample)
                            self.metadata_sent = True

                    # create the dosta data particle
                    # prepend the timestamp from sio mule header to the dosta raw data ,
                    # which is stored in header_match.group(3)
                    sample = self._extract_sample(self._data_particle_class, None,
                                                  (header_match.group(3), data_match),
                                                  None)
                    if sample:
                        # create particle
                        result_particles.append(sample)

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        return result_particles
