"""
@package mi.dataset.parser.wavss_a_dcl
@file mi/dataset/parser/wavss_a_dcl.py
@author Emily Hahn
@brief A parser for the wavss series a instrument through a DCL
"""

import numpy as np
import operator

from mi.core.common import BaseEnum
from mi.core.log import get_logger
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.exceptions import RecoverableSampleException, SampleEncodingException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.utilities import \
    dcl_time_to_utc, \
    timestamp_yyyymmddhhmmss_to_ntp

log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'


class DataParticleType(BaseEnum):
    WAVSS_A_DCL_STATISTICS = "wavss_a_dcl_statistics"
    WAVSS_A_DCL_STATISTICS_RECOVERED = "wavss_a_dcl_statistics_recovered"
    WAVSS_A_DCL_NON_DIRECTIONAL = "wavss_a_dcl_non_directional"
    WAVSS_A_DCL_NON_DIRECTIONAL_RECOVERED = "wavss_a_dcl_non_directional_recovered"
    WAVSS_A_DCL_MEAN_DIRECTIONAL = "wavss_a_dcl_mean_directional"
    WAVSS_A_DCL_MEAN_DIRECTIONAL_RECOVERED = "wavss_a_dcl_mean_directional_recovered"
    WAVSS_A_DCL_MOTION = "wavss_a_dcl_motion"
    WAVSS_A_DCL_MOTION_RECOVERED = "wavss_a_dcl_motion_recovered"
    WAVSS_A_DCL_FOURIER = "wavss_a_dcl_fourier"
    WAVSS_A_DCL_FOURIER_RECOVERED = "wavss_a_dcl_fourier_recovered"


def list_encoder_factory(type_callable):
    """
    Creates a function encoder that iterates on the elements of a list to apply the specified type_callable format.
    :param type_callable:  type to apply to data
    :return:  function that applies type_callable to a supplied list of data
    """

    def inner(data):
        return [type_callable(x) for x in data]

    return inner


float_list_encoder = list_encoder_factory(float)


# the arrays are not defined in groups, so make keys instead of maps
class ArrayParticleKeys(BaseEnum):
    PSD_NON_DIRECTIONAL = "psd_non_directional"
    PSD_MEAN_DIRECTIONAL = "psd_mean_directional"
    MEAN_DIRECTION_ARRAY = "mean_direction_array"
    DIRECTIONAL_SPREAD_ARRAY = "directional_spread_array"
    HEAVE_OFFSET_ARRAY = "heave_offset_array"
    NORTH_OFFSET_ARRAY = "north_offset_array"
    EAST_OFFSET_ARRAY = "east_offset_array"
    FOURIER_COEFFICIENT_2D_ARRAY = "fourier_coefficient_2d_array"


class WavssADclCommonDataParticle(DataParticle):
    """
    WAVSS common DCL particle class - basis for each WAVSS particle
    """

    # particle maps consists of tuples containing the name of parameter and encoding type
    # common particle map used by non-directional, directional, and fourier
    band_parameter_types = [
        ('number_bands', int),
        ('initial_frequency', float),
        ('frequency_spacing', float)
    ]

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        self.dcl_data = None

        super(WavssADclCommonDataParticle, self).__init__(
            raw_data, port_timestamp, internal_timestamp, preferred_timestamp, quality_flag, new_sequence)

    @staticmethod
    def extract_dcl_parts(line):
        """
        Dissect the DCL into it's constituent parts: DCL timestamp, instrument data (payload), and checksum.
        
        Must have initialized the particle with data string.
        :return:  
          timestamp - DCL timestamp in NTP time format (int)
          data - instrument data (string)
          checksum - checksum value (int)
        """
        timestamp = None
        data = None
        checksum = None

        parts = line.split(None, 2)
        if len(parts) == 3:  # standard format with date and time leaders
            dcl_date, dcl_time, parts = parts
            dcl_timestamp = " ".join([dcl_date, dcl_time])
            timestamp = dcl_time_to_utc(dcl_timestamp)
        else:
            parts = line
        if parts[0] == '$':  # data segment must begin with $ leader
            parts = parts.rsplit('*', 1)
            if len(parts) == 2:
                data, checksum = parts
                checksum = int(checksum, 16)
        return timestamp, data, checksum

    @staticmethod
    def compute_checksum(data):
        """
        Perform DCL checksum (XOR) on the data line
        :param data:  string to sum (starting with '$' which is not included in sum)
        :return:  checksum value
        """
        return reduce(operator.xor, bytearray(data[1:]))

    def _build_parsed_values(self):
        """
        Set the timestamp and encode the common particles from the raw data using COMMON_PARTICLE_MAP
        """
        utc_time, self.dcl_data, checksum = self.extract_dcl_parts(self.raw_data)
        if utc_time:
            # DCL controller timestamp  is the port_timestamp
            self.set_port_timestamp(unix_time=utc_time)

        if not self.dcl_data:
            raise RecoverableSampleException('Missing DCL data segment')

        if not checksum or checksum != self.compute_checksum(self.dcl_data):
            self.contents[DataParticleKey.QUALITY_FLAG] = DataParticleValue.CHECKSUM_FAILED

        csv = self.dcl_data.split(',')
        if len(csv) < 7:
            raise RecoverableSampleException('DCL format error: missing items from common wavss header')
        self.marker, self.date, self.time, self.serial_number, self.buoy_id, self.latitude, self.longitude = csv[:7]

        # Instrument timestamp  is the internal_timestamp
        instrument_timestamp = timestamp_yyyymmddhhmmss_to_ntp(self.date + self.time)
        self.set_internal_timestamp(instrument_timestamp)

        self.payload = csv[7:]

        return [self._encode_value('serial_number', self.serial_number, str)]


class WavssADclStatisticsDataParticle(WavssADclCommonDataParticle):
    """
    Wave Statistics Particle
    
    Sample data:
    2014/08/25 15:09:10.100 $TSPWA,20140825,150910,05781,buoyID,,,29,0.00,8.4,0.00,0.00,14.7,0.00,22.8,8.6,28.6,...
    """

    parameter_types = [
        ('number_zero_crossings', int),
        ('average_wave_height', float),
        ('mean_spectral_period', float),
        ('max_wave_height', float),
        ('significant_wave_height', float),
        ('significant_period', float),
        ('wave_height_10', float),
        ('wave_period_10', float),
        ('mean_wave_period', float),
        ('peak_wave_period', float),
        ('wave_period_tp5', float),
        ('wave_height_hmo', float),
        ('mean_direction', float),
        ('mean_spread', float)
    ]

    def _build_parsed_values(self):
        """
        Encode the common parameters and the statistics parameters from the raw data using the particle maps
        """
        particle_parameters = super(WavssADclStatisticsDataParticle, self)._build_parsed_values()

        if len(self.payload) != len(self.parameter_types):
            raise RecoverableSampleException('unexpected number of statistic parameters (got %d, expected %d)' %
                                             (len(self.payload), len(self.parameter_types)))

        # append the statistics specific parameters
        for value, (name, ptype) in zip(self.payload, self.parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        return particle_parameters


class WavssADclStatisticsTelemeteredDataParticle(WavssADclStatisticsDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_STATISTICS


class WavssADclStatisticsRecoveredDataParticle(WavssADclStatisticsDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_STATISTICS_RECOVERED


class WavssADclNonDirectionalDataParticle(WavssADclCommonDataParticle):
    """
    Non Directional Data Particles
    
    Sample Data:
    2014/08/25 15:16:42.432 $TSPNA,20140825,151642,05781,buoyID,,,123,0.030,0.005,7.459E-07,...
    """

    def _build_parsed_values(self):
        """
        Encode the common and bands parameters from the raw data using the particle maps, and extract the non
        directional psd array
        """
        particle_parameters = super(WavssADclNonDirectionalDataParticle, self)._build_parsed_values()

        band_len = len(self.band_parameter_types)
        if len(self.payload) < (band_len + 2):
            raise RecoverableSampleException('missing bands particle map header data')

        bands_header = self.payload[:band_len]
        psd_payload = self.payload[band_len:]
        num_bands = int(self.payload[0])

        expected_payload_len = band_len + num_bands
        if len(self.payload) != expected_payload_len:
            raise RecoverableSampleException('unexpected number of non-directional parameters (got %d, expected %d)' %
                                             (len(self.payload), expected_payload_len))

        # append the band description parameters
        for value, (name, ptype) in zip(bands_header, self.band_parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        # append the non-directional PSD array, from the end of the frequency spacing group to the last floating
        # point match
        particle_parameters.append(self._encode_value(ArrayParticleKeys.PSD_NON_DIRECTIONAL,
                                                      psd_payload, list_encoder_factory(float)))

        return particle_parameters


class WavssADclNonDirectionalTelemeteredDataParticle(WavssADclNonDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_NON_DIRECTIONAL


class WavssADclNonDirectionalRecoveredDataParticle(WavssADclNonDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_NON_DIRECTIONAL_RECOVERED


# the required number of bands for the 3 mean directional arrays, padding is added if not enough bands are sent
MEAN_DIR_NUMBER_BANDS = 123


class WavssADclMeanDirectionalDataParticle(WavssADclCommonDataParticle):
    """
    Mean Directional Spectra Message particle
    
    Sample data:
    2014/08/25 15:16:42.654 $TSPMA,20140825,151642,05781,buoyID,,,86,0.030,0.005,214.05,60.54,7.459E-07,197.1,59.5,...
    """

    parameter_types = [
        ('mean_direction', float),
        ('spread_direction', float)
    ]

    def _build_parsed_values(self):
        """
        Encode the common and bands parameters from the raw data using the particle maps, and extract the 3 mean
        directional arrays
        """
        particle_parameters = super(WavssADclMeanDirectionalDataParticle, self)._build_parsed_values()

        band_len = len(self.band_parameter_types)
        if len(self.payload) < (band_len + 2):
            raise RecoverableSampleException('missing bands particle map header data')

        bands_header = self.payload[:band_len]
        num_bands = int(self.payload[0])

        expected_payload_len = band_len + num_bands * 3 + 2
        if len(self.payload) != expected_payload_len:
            raise RecoverableSampleException('unexpected number of mean-directional parameters (got %d, expected %d)' %
                                             (len(self.payload), expected_payload_len))

        # append the band description parameters
        for value, (name, ptype) in zip(bands_header, self.band_parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        # append the mean directional specific parameters
        mean_header = self.payload[band_len:]

        for value, (name, ptype) in zip(mean_header, self.parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        # split up the array into 3 arrays each number of bands in length, taking each 3rd item, size of array
        # checked in wavss parser
        spectra_payload = self.payload[band_len + 2:]
        psd = spectra_payload[0:num_bands * 3:3]
        mean_dir = spectra_payload[1:num_bands * 3:3]
        dir_spread = spectra_payload[2:num_bands * 3:3]

        # to match with non-directional data, the mean directional arrays must be padded with NaNs so they are
        # the same size
        for i in xrange(num_bands, MEAN_DIR_NUMBER_BANDS):
            psd.append(np.nan)
            mean_dir.append(np.nan)
            dir_spread.append(np.nan)

        # append and encode the particle mean directional arrays
        particle_parameters.extend((
            self._encode_value(ArrayParticleKeys.PSD_MEAN_DIRECTIONAL, psd, float_list_encoder),
            self._encode_value(ArrayParticleKeys.MEAN_DIRECTION_ARRAY, mean_dir, float_list_encoder),
            self._encode_value(ArrayParticleKeys.DIRECTIONAL_SPREAD_ARRAY, dir_spread, float_list_encoder)))

        return particle_parameters


class WavssADclMeanDirectionalTelemeteredDataParticle(WavssADclMeanDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MEAN_DIRECTIONAL


class WavssADclMeanDirectionalRecoveredDataParticle(WavssADclMeanDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MEAN_DIRECTIONAL_RECOVERED


class WavssADclMotionDataParticle(WavssADclCommonDataParticle):
    """
    Motion Data Particle

    Sample data:
    2014/08/25 15:16:42.765 $TSPHA,20140825,151642,05781,buoyID,,,344,15.659,0.783,0,0.00,0.00,0.00,0.00, ...
    """

    parameter_types = [
        ('number_time_samples', int),
        ('initial_time', float),
        ('time_spacing', float),
        ('solution_found', int),
    ]

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(WavssADclCommonDataParticle, self).__init__(
            raw_data, port_timestamp, internal_timestamp, preferred_timestamp, quality_flag, new_sequence)

    def _build_parsed_values(self):
        """
        Encode the common and motion parameters from the raw data using the particle maps, and extract the 3 motion
        offset arrays
        """
        particle_parameters = super(WavssADclMotionDataParticle, self)._build_parsed_values()

        band_len = len(self.parameter_types)
        if len(self.payload) < (band_len + 2):
            raise RecoverableSampleException('missing bands particle map header data')

        num_bands = int(self.payload[0])

        expected_payload_len = band_len + num_bands * 3
        if len(self.payload) != expected_payload_len:
            raise RecoverableSampleException('unexpected number of motion parameters (got %d, expected %d)' %
                                             (len(self.payload), expected_payload_len))

        # append the motion description parameters
        motion_header = self.payload[:band_len]
        for value, (name, ptype) in zip(motion_header, self.parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        # split up the large array into 3 smaller arrays, heave1, north1, east1, heave2, north2, east2, etc.
        # size of array is pre-checked in wavss parser
        motion_payload = self.payload[band_len:]
        heave = motion_payload[0:num_bands * 3:3]
        north = motion_payload[1:num_bands * 3:3]
        east = motion_payload[2:num_bands * 3:3]

        # append and encode the motion offset arrays
        particle_parameters.extend((
            self._encode_value(ArrayParticleKeys.HEAVE_OFFSET_ARRAY, heave, float_list_encoder),
            self._encode_value(ArrayParticleKeys.NORTH_OFFSET_ARRAY, north, float_list_encoder),
            self._encode_value(ArrayParticleKeys.EAST_OFFSET_ARRAY, east, float_list_encoder)))

        return particle_parameters


class WavssADclMotionTelemeteredDataParticle(WavssADclMotionDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MOTION


class WavssADclMotionRecoveredDataParticle(WavssADclMotionDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MOTION_RECOVERED


class WavssADclFourierDataParticle(WavssADclCommonDataParticle):
    """
    Fourier Data Particle

    Sample data:
    2014/08/25 15:16:42.543 $TSPFB,20140825,151642,05781,buoyID,,,123,0.030,0.005,86,0.030,0.005,-0.43981,-0.13496, ...
    """

    parameter_types = [
        ('number_directional_bands', int),
        ('initial_directional_frequency', float),
        ('directional_frequency_spacing', float)
    ]

    def _build_parsed_values(self):
        """
        Encode the common, bands, and fourier parameters from the raw data using the particle maps, then extract and
        shape the fourier array
        """
        particle_parameters = super(WavssADclFourierDataParticle, self)._build_parsed_values()

        band_len = len(self.band_parameter_types)
        if len(self.payload) < (band_len + 2):
            raise RecoverableSampleException('missing bands particle map header data')

        bands_header = self.payload[:band_len]
        num_bands = int(bands_header[0])

        expected_payload_len = len(self.band_parameter_types) + len(self.parameter_types) + 4 * (num_bands - 2)
        if len(self.payload) != expected_payload_len:
            raise RecoverableSampleException('unexpected number of fourier parameters (got %d, expected %d)' %
                                             (len(self.payload), expected_payload_len))

        # append the band description parameters
        for value, (name, ptype) in zip(bands_header, self.band_parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        # append the mean directional specific parameters
        fourier_payload = self.payload[band_len:]
        fourier_header = fourier_payload[:len(self.parameter_types)]
        fourier_data = fourier_payload[len(self.parameter_types):]
        for value, (name, ptype) in zip(fourier_header, self.parameter_types):
            particle_parameters.append(self._encode_value(name, value, ptype))

        flt_array = float_list_encoder(fourier_data)

        # reshape the fourier array to 4 x number_bands-2, size of array is checked in wavss parser
        data = np.array(flt_array).reshape((num_bands - 2), 4).tolist()

        # append and encode the fourier coefficients array
        particle_parameters.append(self._encode_value(ArrayParticleKeys.FOURIER_COEFFICIENT_2D_ARRAY,
                                                      data, list))

        return particle_parameters


class WavssADclFourierTelemeteredDataParticle(WavssADclFourierDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_FOURIER


class WavssADclFourierRecoveredDataParticle(WavssADclFourierDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_FOURIER_RECOVERED


class WavssADclParser(SimpleParser):
    def __init__(self,
                 stream_handle,
                 exception_callback,
                 is_telemetered):

        if is_telemetered:
            # this is a telemetered parser
            self.statistics_particle_class = WavssADclStatisticsTelemeteredDataParticle
            self.non_directional_particle_class = WavssADclNonDirectionalTelemeteredDataParticle
            self.mean_directional_particle_class = WavssADclMeanDirectionalTelemeteredDataParticle
            self.motion_particle_class = WavssADclMotionTelemeteredDataParticle
            self.fourier_particle_class = WavssADclFourierTelemeteredDataParticle
        else:
            # this is a recovered parser
            self.statistics_particle_class = WavssADclStatisticsRecoveredDataParticle
            self.non_directional_particle_class = WavssADclNonDirectionalRecoveredDataParticle
            self.mean_directional_particle_class = WavssADclMeanDirectionalRecoveredDataParticle
            self.motion_particle_class = WavssADclMotionRecoveredDataParticle
            self.fourier_particle_class = WavssADclFourierRecoveredDataParticle

        # no config for this parser, pass in empty dict
        super(WavssADclParser, self).__init__({},
                                              stream_handle,
                                              exception_callback)

    def parse_file(self):

        for line in self._stream_handle:
            if '$TSPWA' in line:
                self.extract_particle(self.statistics_particle_class, line)
            elif '$TSPMA' in line:
                self.extract_particle(self.mean_directional_particle_class, line)
            elif '$TSPNA' in line:
                self.extract_particle(self.non_directional_particle_class, line)
            elif '$TSPHA' in line:
                self.extract_particle(self.motion_particle_class, line)
            elif '$TSPFB' in line:
                self.extract_particle(self.fourier_particle_class, line)

    def extract_particle(self, particle_class, line):
        """
        Extract a particle of the specified class and append it to the record buffer
        @param particle_class: particle class to extract
        @param line: raw data input line
        """
        particle = self._extract_sample(particle_class, None, line)
        if particle:
            self._record_buffer.append(particle)

    def _extract_sample(self, particle_class, regex, raw_data, port_timestamp=None, internal_timestamp=None,
                        preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP):
        """
        Extract sample from a response line if present and publish
        parsed particle

        @param particle_class  The class to instantiate for this specific
            data particle. Parameterizing this allows for simple, standard
            behavior from this routine
        @param regex  The regular expression that matches a data sample if regex
                      is none then process every line
        @param raw_data  data to input into this particle.
        @param port_timestamp
        @param preferred_ts  the preferred timestamp (default: INTERNAL_TIMESTAMP)
        @return  raw particle if a sample was found, else None

        Changed to not return a particle in the case where an exception occurs and there is an exception callback
        defined. (c.f. 12252)
        """
        try:
            if regex is None or regex.match(raw_data):
                particle = particle_class(raw_data, port_timestamp=port_timestamp,
                                          preferred_timestamp=preferred_ts)

                # need to actually parse the particle fields to find out of there are errors
                particle.generate_dict()
                encoding_errors = particle.get_encoding_errors()
                if encoding_errors:
                    log.warn("Failed to encode: %s", encoding_errors)
                    raise SampleEncodingException("Failed to encode: %s" % encoding_errors)
                return particle

        except (RecoverableSampleException, SampleEncodingException) as e:
            log.error("Sample exception detected: %s raw data: %s", e, raw_data)
            if self._exception_callback:
                self._exception_callback(e)
            else:
                raise e
        return

    def recov_exception(self, error_message):
        """
        Add a warning log message and use the exception callback to pass a recoverable exception
        @param error_message: The error message to use in the log and callback
        """
        log.warn(error_message)
        self._exception_callback(RecoverableSampleException(error_message))
