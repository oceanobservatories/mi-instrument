"""
@package mi.dataset.parser.wavss_a_dcl
@file mi/dataset/parser/wavss_a_dcl.py
@author Emily Hahn
@brief A parser for the wavss series a instrument through a DCL
"""


import re
import numpy as np

from mi.core.common import BaseEnum
from mi.core.log import get_logger
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import SampleException, RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.dataset.parser.common_regexes import INT_REGEX
from mi.dataset.parser.utilities import dcl_controller_timestamp_to_utc_time
log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

FLOAT_REGEX = r'(?:[+-]?\d*\.\d*(?:[Ee][+-]?\d+)?|[+-]?[nN][aA][nN])'  # includes scientific notation & Nans
FLOAT_GROUP_REGEX = r'(' + FLOAT_REGEX + ')'
INT_GROUP_REGEX = r'(' + INT_REGEX + ')'
END_OF_LINE_REGEX = r'(?:\r\n|\n)?'  # end of file might be missing terminator so make optional
END_OF_SAMPLE_REGEX = r'\*[A-Fa-f0-9]{2}' + END_OF_LINE_REGEX
DCL_TIMESTAMP_REGEX = r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d{3})'

DATE_REGEX = '(\d{8})'
TIME_REGEX = '(\d{6})'
SERIAL_REGEX = '(\d+)'
DATE_TIME_SERIAL_REGEX = DATE_REGEX + ',' + TIME_REGEX + ',' + SERIAL_REGEX + \
                         r',buoyID,,,'  # includes ignored buoyID text and empty lat lon

# wave statistics sample regex
TSPWA_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPWA,' +
               DATE_TIME_SERIAL_REGEX +
               INT_GROUP_REGEX + ',' +     # number of zero crossings
               FLOAT_GROUP_REGEX + ',' +   # average wave height
               FLOAT_GROUP_REGEX + ',' +   # mean spectral period
               FLOAT_GROUP_REGEX + ',' +   # maximum wave height
               FLOAT_GROUP_REGEX + ',' +   # significant wave height
               FLOAT_GROUP_REGEX + ',' +   # significant period
               FLOAT_GROUP_REGEX + ',' +   # h10
               FLOAT_GROUP_REGEX + ',' +   # t10
               FLOAT_GROUP_REGEX + ',' +   # mean wave period
               FLOAT_GROUP_REGEX + ',' +   # peak period
               FLOAT_GROUP_REGEX + ',' +   # tp5
               FLOAT_GROUP_REGEX + ',' +   # hmo
               FLOAT_GROUP_REGEX + ',' +   # mean direction
               FLOAT_GROUP_REGEX +         # mean spread
               END_OF_SAMPLE_REGEX)

TSPWA_MATCHER = re.compile(TSPWA_REGEX)

# non-directional spectral sample regex
TSPNA_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPNA,' +
               DATE_TIME_SERIAL_REGEX +
               INT_GROUP_REGEX + ',' +      # number of bands
               FLOAT_GROUP_REGEX + ',' +    # initial frequency
               FLOAT_GROUP_REGEX + ',' +    # frequency spacing
               '(' + FLOAT_REGEX + ',)*' +  # match a varying number of comma separated floats
               FLOAT_GROUP_REGEX +          # match the last float without a comma
               END_OF_SAMPLE_REGEX)
TSPNA_MATCHER = re.compile(TSPNA_REGEX)

# mean directional spectra sample regex
TSPMA_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPMA,' +
               DATE_TIME_SERIAL_REGEX +
               INT_GROUP_REGEX + ',' +        # number of bands
               FLOAT_GROUP_REGEX + ',' +      # initial frequency
               FLOAT_GROUP_REGEX + ',' +      # frequency spacing
               FLOAT_GROUP_REGEX + ',' +      # mean average direction
               FLOAT_GROUP_REGEX + ',' +      # spread direction
               '(' + FLOAT_REGEX + ',)*' +    # match a varying number of comma separated floats
               FLOAT_GROUP_REGEX +            # match the last float without a comma
               END_OF_SAMPLE_REGEX)
TSPMA_MATCHER = re.compile(TSPMA_REGEX)

# heave north east motion sample regex
TSPHA_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPHA,' +
               DATE_TIME_SERIAL_REGEX +
               INT_GROUP_REGEX + ',' +        # number of time samples
               FLOAT_GROUP_REGEX + ',' +      # initial time
               FLOAT_GROUP_REGEX + ',' +      # time spacing
               INT_GROUP_REGEX + ',' +        # solution found
               '(' + FLOAT_REGEX + ',)*' +    # match a varying number of comma separated floats
               FLOAT_GROUP_REGEX +            # match the last float without a comma
               END_OF_SAMPLE_REGEX)
TSPHA_MATCHER = re.compile(TSPHA_REGEX)

# fourier sample regex
TSPFB_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPFB,' +
               DATE_TIME_SERIAL_REGEX +
               INT_GROUP_REGEX + ',' +        # number of bands
               FLOAT_GROUP_REGEX + ',' +      # initial frequency
               FLOAT_GROUP_REGEX + ',' +      # frequency spacing
               INT_GROUP_REGEX + ',' +        # number of directional bands
               FLOAT_GROUP_REGEX + ',' +      # directional initial frequency
               FLOAT_GROUP_REGEX + ',' +      # directional frequency spacing
               '((?:' + FLOAT_REGEX + ',)*' + FLOAT_REGEX + ')' +  # match a varying number of comma separated floats
               END_OF_SAMPLE_REGEX)
TSPFB_MATCHER = re.compile(TSPFB_REGEX)

# status message, this is ignored
TSPSA_REGEX = (DCL_TIMESTAMP_REGEX + ' \$TSPSA,' +
               DATE_TIME_SERIAL_REGEX +
               r'([+\-0-9.Ee,]+)' +           # match a varying number of comma separated floats and ints
               END_OF_SAMPLE_REGEX)
TSPSA_MATCHER = re.compile(TSPSA_REGEX)

# log status message, this is ignored
LOG_STATUS_REGEX = DCL_TIMESTAMP_REGEX + ' \[wavss:DLOGP\d+\]:.*' + END_OF_LINE_REGEX
LOG_STATUS_MATCHER = re.compile(LOG_STATUS_REGEX)


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


# particle maps consists of tuples containing the name of parameter, index into regex group, encoding type
DCL_TIMESTAMP_GROUP = 1
# the first 4 parameters in all samples are the same
COMMON_PARTICLE_MAP = [
    ('dcl_controller_timestamp', DCL_TIMESTAMP_GROUP, str),
    ('date_string', 2, str),
    ('time_string', 3, str),
    ('serial_number', 4, str)
]

# common particle map used by non-directional, directional, and fourier
FREQ_SPACING_GROUP = 7
BANDS_PARTICLE_MAP = [
    ('number_bands', 5, int),
    ('initial_frequency', 6, float),
    ('frequency_spacing', FREQ_SPACING_GROUP, float)
]

NUMBER_COUNT_GROUP = 5  # number of bands and time samples


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

    def _build_parsed_values(self):
        """
        Set the timestamp and encode the common particles from the raw data using COMMON_PARTICLE_MAP
        """
        # the timestamp comes from the DCL logger timestamp, parse the string into a datetime
        utc_time = dcl_controller_timestamp_to_utc_time(self.raw_data.group(DCL_TIMESTAMP_GROUP))
        self.set_internal_timestamp(unix_time=utc_time)

        return [self._encode_value(name, self.raw_data.group(group), function)
                for name, group, function in COMMON_PARTICLE_MAP]

    @staticmethod
    def string_to_float_array(input_string):
        """
        Convert a string of comma separated floats to an array of floating point values
        @param input_string a string containing a set of comma separated floats
        @return returns an array of floating point values
        """
        string_array = input_string.split(',')
        return map(float, string_array)


# --------------- Statistics Data Particles -------------------------------------------------------------

STATISTICS_PARTICLE_MAP = [
    ('number_zero_crossings', 5, int),
    ('average_wave_height', 6, float),
    ('mean_spectral_period', 7, float),
    ('max_wave_height', 8, float),
    ('significant_wave_height', 9, float),
    ('significant_period', 10, float),
    ('wave_height_10', 11, float),
    ('wave_period_10', 12, float),
    ('mean_wave_period', 13, float),
    ('peak_wave_period', 14, float),
    ('wave_period_tp5', 15, float),
    ('wave_height_hmo', 16, float),
    ('mean_direction', 17, float),
    ('mean_spread', 18, float)
]


class WavssADclStatisticsDataParticle(WavssADclCommonDataParticle):

    def _build_parsed_values(self):
        """
        Encode the common parameters and the statistics parameters from the raw data using the particle maps
        """
        particle_parameters = super(WavssADclStatisticsDataParticle, self)._build_parsed_values()

        # append the statistics specific parameters
        for name, group, function in STATISTICS_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        return particle_parameters


class WavssADclStatisticsTelemeteredDataParticle(WavssADclStatisticsDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_STATISTICS


class WavssADclStatisticsRecoveredDataParticle(WavssADclStatisticsDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_STATISTICS_RECOVERED


# --------------- Non Directional Data Particles -------------------------------------------------------------
END_NON_DIR_ARRAY_GROUP = 9


class WavssADclNonDirectionalDataParticle(WavssADclCommonDataParticle):

    def _build_parsed_values(self):
        """
        Encode the common and bands parameters from the raw data using the particle maps, and extract the non
        directional psd array
        """
        particle_parameters = super(WavssADclNonDirectionalDataParticle, self)._build_parsed_values()

        # append the band description parameters
        for name, group, function in BANDS_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        # append the non-directional PSD array, from the end of the frequency spacing group to the last floating point
        #  match
        non_dir_data = self.raw_data.group(0)[self.raw_data.end(FREQ_SPACING_GROUP) + 1:
                                              self.raw_data.end(END_NON_DIR_ARRAY_GROUP)]
        particle_parameters.append(self._encode_value(ArrayParticleKeys.PSD_NON_DIRECTIONAL,
                                                      non_dir_data,
                                                      WavssADclCommonDataParticle.string_to_float_array))

        return particle_parameters


class WavssADclNonDirectionalTelemeteredDataParticle(WavssADclNonDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_NON_DIRECTIONAL


class WavssADclNonDirectionalRecoveredDataParticle(WavssADclNonDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_NON_DIRECTIONAL_RECOVERED


# --------------- Mean Directional Data Particles -------------------------------------------------------------

# the required number of bands for the 3 mean directional arrays, padding is added if not enough bands are sent
MEAN_DIR_NUMBER_BANDS = 123
SPREAD_DIR_GROUP = 9
END_MEAN_DIR_ARRAY_GROUP = 11

MEAN_DIRECTION_PARTICLE_MAP = [
    ('mean_direction', 8, float),
    ('spread_direction', SPREAD_DIR_GROUP, float)
]


class WavssADclMeanDirectionalDataParticle(WavssADclCommonDataParticle):

    def _build_parsed_values(self):
        """
        Encode the common and bands parameters from the raw data using the particle maps, and extract the 3 mean
        directional arrays
        """
        particle_parameters = super(WavssADclMeanDirectionalDataParticle, self)._build_parsed_values()

        # append the band description parameters
        for name, group, function in BANDS_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        # append the mean directional specific parameters
        for name, group, function in MEAN_DIRECTION_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        number_bands = int(self.raw_data.group(NUMBER_COUNT_GROUP))

        # split the array of floats
        data_str = self.raw_data.group(0)[self.raw_data.end(SPREAD_DIR_GROUP) + 1:
                                          self.raw_data.end(END_MEAN_DIR_ARRAY_GROUP)]
        flt_array = WavssADclCommonDataParticle.string_to_float_array(data_str)

        # split up the array into 3 arrays each number of bands in length, taking each 3rd item, size of array
        # checked in wavss parser
        psd = flt_array[0:number_bands*3:3]
        mean_dir = flt_array[1:number_bands*3:3]
        dir_spread = flt_array[2:number_bands*3:3]

        # to match with non-directional data, the mean directional arrays must be padded with NaNs so they are
        # the same size
        for i in range(number_bands, MEAN_DIR_NUMBER_BANDS):
            psd.append(np.nan)
            mean_dir.append(np.nan)
            dir_spread.append(np.nan)

        # append and encode the particle mean directional arrays
        particle_parameters.append(self._encode_value(ArrayParticleKeys.PSD_MEAN_DIRECTIONAL, psd, list))
        particle_parameters.append(self._encode_value(ArrayParticleKeys.MEAN_DIRECTION_ARRAY, mean_dir, list))
        particle_parameters.append(self._encode_value(ArrayParticleKeys.DIRECTIONAL_SPREAD_ARRAY, dir_spread,
                                                      list))

        return particle_parameters


class WavssADclMeanDirectionalTelemeteredDataParticle(WavssADclMeanDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MEAN_DIRECTIONAL


class WavssADclMeanDirectionalRecoveredDataParticle(WavssADclMeanDirectionalDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MEAN_DIRECTIONAL_RECOVERED


# --------------- Motion Data Particles -------------------------------------------------------------
SOLUTION_FOUND_GROUP = 8
END_MOTION_ARRAY_GROUP = 10

MOTION_PARTICLE_MAP = [
    ('number_time_samples', 5, int),
    ('initial_time', 6, float),
    ('time_spacing', 7, float),
    ('solution_found', SOLUTION_FOUND_GROUP, int),
]


class WavssADclMotionDataParticle(WavssADclCommonDataParticle):

    def _build_parsed_values(self):
        """
        Encode the common and motion parameters from the raw data using the particle maps, and extract the 3 motion
        offset arrays
        """
        particle_parameters = super(WavssADclMotionDataParticle, self)._build_parsed_values()

        # append the motion description parameters
        for name, group, function in MOTION_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        number_samples = int(self.raw_data.group(NUMBER_COUNT_GROUP))

        data_array = self.raw_data.group(0)[self.raw_data.end(SOLUTION_FOUND_GROUP) + 1:
                                            self.raw_data.end(END_MOTION_ARRAY_GROUP)]
        flt_array = WavssADclCommonDataParticle.string_to_float_array(data_array)

        # split up the large array into 3 smaller arrays, heave1, north1, east1, heave2, north2, east2, etc.
        # size of array is pre-checked in wavss parser
        heave = flt_array[0:number_samples*3:3]
        north = flt_array[1:number_samples*3:3]
        east = flt_array[2:number_samples*3:3]

        # append and encode the motion offset arrays
        particle_parameters.append(self._encode_value(ArrayParticleKeys.HEAVE_OFFSET_ARRAY, heave, list))
        particle_parameters.append(self._encode_value(ArrayParticleKeys.NORTH_OFFSET_ARRAY, north, list))
        particle_parameters.append(self._encode_value(ArrayParticleKeys.EAST_OFFSET_ARRAY, east, list))

        return particle_parameters


class WavssADclMotionTelemeteredDataParticle(WavssADclMotionDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MOTION


class WavssADclMotionRecoveredDataParticle(WavssADclMotionDataParticle):
    _data_particle_type = DataParticleType.WAVSS_A_DCL_MOTION_RECOVERED


# --------------- Fourier Data Particles -------------------------------------------------------------

DIR_FREQ_SPACING_GROUP = 10
END_FOURIER_ARRAY_GROUP = 11
FOURIER_PARTICLE_MAP = [
    ('number_directional_bands', 8, int),
    ('initial_directional_frequency', 9, float),
    ('directional_frequency_spacing', DIR_FREQ_SPACING_GROUP, float)
]


class WavssADclFourierDataParticle(WavssADclCommonDataParticle):

    def _build_parsed_values(self):
        """
        Encode the common, bands, and fourier parameters from the raw data using the particle maps, then extract and
        shape the fourier array
        """
        particle_parameters = super(WavssADclFourierDataParticle, self)._build_parsed_values()

        # append the band description parameters
        for name, group, function in BANDS_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        # append the mean directional specific parameters
        for name, group, function in FOURIER_PARTICLE_MAP:
            particle_parameters.append(self._encode_value(name, self.raw_data.group(group), function))

        number_bands = int(self.raw_data.group(NUMBER_COUNT_GROUP))

        data_array = self.raw_data.group(0)[self.raw_data.end(DIR_FREQ_SPACING_GROUP) + 1:
                                            self.raw_data.end(END_FOURIER_ARRAY_GROUP)]
        flt_array = WavssADclCommonDataParticle.string_to_float_array(data_array)

        # reshape the fourier array to 4 x number_bands-2, size of array is checked in wavss parser
        np_flt_array = np.vstack(flt_array)
        flt_array = np_flt_array.reshape((number_bands - 2), 4)
        # convert each array back to a list for json since it will not recognize numpy arrays
        list_flt_array = []
        for i in range(0, len(flt_array)):
            list_flt_array.append(list(flt_array[i]))

        # append and encode the fourier coefficients array
        particle_parameters.append(self._encode_value(ArrayParticleKeys.FOURIER_COEFFICIENT_2D_ARRAY,
                                                      list_flt_array, list))

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

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:

            tspwa_match = TSPWA_MATCHER.match(line)
            tspma_match = TSPMA_MATCHER.match(line)
            tspna_match = TSPNA_MATCHER.match(line)
            tspha_match = TSPHA_MATCHER.match(line)
            tspfb_match = TSPFB_MATCHER.match(line)
            num_csv = len(line.split(','))

            if tspwa_match:
                # this is a wave statistics sample
                self.extract_particle(self.statistics_particle_class, tspwa_match)

            elif tspma_match:
                # this is a mean directional sample
                num_bands = int(tspma_match.group(NUMBER_COUNT_GROUP))

                if num_csv != (12 + 3*num_bands):
                    self.recov_exception("TSPMA does not contain 12 + 3*%d comma separated values, has %d" %
                                         (num_bands, num_csv))

                else:
                    self.extract_particle(self.mean_directional_particle_class, tspma_match)

            elif tspna_match:
                # this is a non directional sample
                num_bands = int(tspna_match.group(NUMBER_COUNT_GROUP))

                if num_csv != (10 + num_bands):
                    self.recov_exception("TSPNA does not contain 10 + %d comma separated values, has %d" %
                                         (num_bands, num_csv))

                else:
                    self.extract_particle(self.non_directional_particle_class, tspna_match)

            elif tspha_match:
                # this is a heave north east / motion sample
                num_time = int(tspha_match.group(NUMBER_COUNT_GROUP))

                if num_csv != (11 + 3*num_time):
                    self.recov_exception("TSPHA doesn't contain 11 + 3*%d comma separated values, has %d" %
                                         (num_time, num_csv))

                else:
                    self.extract_particle(self.motion_particle_class, tspha_match)

            elif tspfb_match:
                # this is a fourier sample
                num_bands = int(tspfb_match.group(NUMBER_COUNT_GROUP))

                if num_csv != (13 + 4*(num_bands - 2)):
                    self.recov_exception("TSPFB doesn't contain 13 + 4*(%d - 2) comma separated values, has %d" %
                                         (num_bands, num_csv))

                else:
                    self.extract_particle(self.fourier_particle_class, tspfb_match)

            else:
                log_match = LOG_STATUS_MATCHER.match(line)
                tspsa_match = TSPSA_MATCHER.match(line)

                if not (log_match or tspsa_match):
                    self.recov_exception("Wavss encountered unexpected data line '%s'" % line)

            # read the next line in the file
            line = self._stream_handle.readline()

    def extract_particle(self, particle_class, match):
        """
        Extract a particle of the specified class and append it to the record buffer
        @param particle_class: particle class to extract
        @param match: regex match to pass in as raw data
        """
        particle = self._extract_sample(particle_class, None, match, None)
        self._record_buffer.append(particle)

    def recov_exception(self, error_message):
        """
        Add a warning log message and use the exception callback to pass a recoverable exception
        @param error_message: The error message to use in the log and callback
        """
        log.warn(error_message)
        self._exception_callback(RecoverableSampleException(error_message))
