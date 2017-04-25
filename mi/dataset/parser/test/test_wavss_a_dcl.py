"""
@package mi.dataset.parser.test.test_wavss_a_dcl
@file mi/dataset/parser/test/test_wavss_a_dcl.py
@author Emily Hahn
@brief A test parser for the wavss series a instrument through a DCL
"""

import os
from nose.plugins.attrib import attr

from mi.core.instrument.data_particle import DataParticleValue
from mi.core.log import get_logger
from mi.core.exceptions import RecoverableSampleException
from mi.dataset.driver.wavss_a.dcl.resource import RESOURCE_PATH
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.parser.wavss_a_dcl import WavssADclParser, WavssADclFourierDataParticle, WavssADclMotionDataParticle, \
    WavssADclMeanDirectionalDataParticle
from mi.dataset.parser.wavss_a_dcl import WavssADclStatisticsDataParticle, WavssADclNonDirectionalDataParticle
from mi.dataset.parser.wavss_a_dcl import MEAN_DIR_NUMBER_BANDS

log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'


@attr('UNIT', group='mi')
class WavssADclParserUnitTestCase(ParserUnitTestCase):
    def test_checksum_valid(self):
        line = '2014/08/25 15:09:10.100 $TSPWA,20140825,150910,05781,buoyID,,,29,0.00,8.4,0.00,0.00,14.7,0.00,22.8,' \
               '8.6,28.6,28.6,0.00,203.3,66.6*5B'
        statistics = WavssADclStatisticsDataParticle(line, None, None, None)
        _, data, checksum = statistics.extract_dcl_parts(line)
        computed_checksum = statistics.compute_checksum(data)
        self.assertEqual(checksum, computed_checksum, 'valid string failed checksum')

    def test_checksum_missing_date(self):
        line = '$TSPWA,20140825,150910,05781,buoyID,,,29,0.00,8.4,0.00,0.00,14.7,0.00,22.8,' \
               '8.6,28.6,28.6,0.00,203.3,66.6*5B'
        statistics = WavssADclStatisticsDataParticle(line, None, None, None)
        _, data, checksum = statistics.extract_dcl_parts(line)
        computed_checksum = statistics.compute_checksum(data)
        self.assertEqual(checksum, computed_checksum, 'missing date failed checksum')

    def test_checksum_truncated_data(self):
        line = '2014/08/25 15:09:10.100 $TSPWA,20140825,'

        statistics = WavssADclStatisticsDataParticle(line, None, None, None)
        _, data, checksum = statistics.extract_dcl_parts(line)
        self.assertIsNone(checksum, 'missing checksum value matched')

    def test_checksum_incorrect(self):
        line = '2014/08/25 15:09:10.100 $TSPWA,20140825,150910,05781,buoyID,,,29,0.00,8.4,0.00,0.00,14.7,0.00,' \
               '22.8,8.6,28.6,28.6,0.00,203.3,66.6*5C'

        statistics = WavssADclStatisticsDataParticle(line, None, None, None)
        _, data, checksum = statistics.extract_dcl_parts(line)
        computed_checksum = statistics.compute_checksum(data)
        self.assertNotEqual(checksum, computed_checksum, 'invalid checksum value matched')

    def test_parse_tspwa(self):
        line = '2014/08/25 15:09:10.100 $TSPWA,20140825,150910,05781,buoyID,,,29,0.00,8.4,0.00,0.00,14.7,0.00,22.8,' \
               '8.6,28.6,28.6,0.00,203.3,66.6*5B'
        statistics = WavssADclStatisticsDataParticle(line, None, None, None)
        particle = statistics._build_parsed_values()
        my_dict = {x['value_id']: x['value'] for x in particle}

        # TODO - remove
        self.assertEqual(my_dict['dcl_controller_timestamp'], '2014/08/25 15:09:10.100')
        self.assertEqual(my_dict['date_string'], '20140825')
        self.assertEqual(my_dict['time_string'], '150910')

        self.assertEqual(my_dict['serial_number'], '05781')
        self.assertEqual(my_dict['number_zero_crossings'], 29)
        self.assertEqual(my_dict['average_wave_height'], 0.0)
        self.assertEqual(my_dict['mean_spectral_period'], 8.4)
        self.assertEqual(my_dict['max_wave_height'], 0.0)
        self.assertEqual(my_dict['significant_wave_height'], 0.0)
        self.assertEqual(my_dict['significant_period'], 14.7)
        self.assertEqual(my_dict['wave_height_10'], 0.0)
        self.assertEqual(my_dict['wave_period_10'], 22.8)
        self.assertEqual(my_dict['mean_wave_period'], 8.6)
        self.assertEqual(my_dict['peak_wave_period'], 28.6)
        self.assertEqual(my_dict['wave_period_tp5'], 28.6)
        self.assertEqual(my_dict['wave_height_hmo'], 0.0)
        self.assertEqual(my_dict['mean_direction'], 203.3)
        self.assertEqual(my_dict['mean_spread'], 66.6)

    def test_parse_tspna(self):
        line = '2014/08/25 15:16:42.432 $TSPNA,20140825,151642,05781,buoyID,,,123,0.030,0.005,7.459E-07,4.206E-07,' \
               '6.719E-08,2.162E-07,5.291E-08,7.220E-08,6.279E-08,5.573E-08,1.588E-08,7.655E-08,2.018E-08,1.105E-08,' \
               '5.943E-09,2.686E-09,7.255E-09,1.452E-09,1.234E-09,1.227E-09,3.562E-09,4.694E-09,1.219E-09,4.381E-10,' \
               '4.524E-10,6.204E-10,2.745E-10,8.695E-10,5.408E-10,4.507E-10,2.089E-10,2.713E-10,1.788E-11,2.374E-10,' \
               '3.534E-10,1.147E-10,1.394E-10,3.959E-08,6.480E-08,8.602E-09,6.799E-09,4.508E-09,4.986E-09,2.470E-08,' \
               '9.824E-09,6.896E-09,4.530E-09,1.540E-09,1.774E-09,8.661E-09,1.414E-09,9.119E-09,1.389E-08,8.045E-09,' \
               '9.907E-09,5.225E-09,5.090E-09,1.266E-08,1.808E-08,1.324E-08,6.395E-10,2.550E-09,2.976E-09,4.384E-09,' \
               '1.137E-08,4.375E-09,3.124E-09,3.415E-09,5.632E-09,3.080E-09,4.449E-09,6.763E-09,6.318E-10,1.831E-10,' \
               '7.131E-10,2.062E-09,6.528E-10,1.633E-09,4.253E-10,6.663E-10,1.215E-09,6.925E-10,3.502E-10,8.197E-10,' \
               '3.688E-09,1.774E-09,7.943E-10,1.306E-09,2.659E-10,1.133E-09,1.309E-09,7.863E-10,5.754E-10,4.124E-11,' \
               '3.137E-10,9.134E-10,7.137E-10,1.012E-09,5.034E-10,5.196E-10,6.875E-10,8.619E-10,3.378E-09,2.069E-09,' \
               '5.006E-10,3.947E-10,2.207E-10,1.665E-10,3.317E-10,7.853E-11,2.931E-10,4.610E-11,4.656E-10,7.965E-10,' \
               '1.538E-10,7.422E-10,3.099E-11,6.882E-11,4.341E-10,7.205E-11,1.012E-10,1.921E-10,1.860E-10,4.120E-10,' \
               '5.827E-10*19'
        spectra = WavssADclNonDirectionalDataParticle(line, None, None, None)
        particle = spectra._build_parsed_values()
        my_dict = {x['value_id']: x['value'] for x in particle}

        self.assertEqual(my_dict['serial_number'], '05781')
        self.assertEqual(my_dict['number_bands'], 123)
        self.assertEqual(my_dict['initial_frequency'], 0.03)
        self.assertEqual(len(my_dict['psd_non_directional']), 123)
        self.assertEqual(my_dict['frequency_spacing'], 0.005)
        self.assertEqual(my_dict['psd_non_directional'][0], 7.459e-07)

    def test_parse_tspma(self):
        line = '2014/08/25 15:16:42.654 $TSPMA,20140825,151642,05781,buoyID,,,86,0.030,0.005,214.05,60.54,7.459E-07,' \
               '197.1,59.5,4.206E-07,203.9,64.2,6.719E-08,289.6,66.1,2.162E-07,269.5,59.2,5.291E-08,226.7,45.6,' \
               '7.220E-08,220.9,52.6,6.279E-08,103.9,70.0,5.573E-08,72.5,66.6,1.588E-08,39.2,59.4,7.655E-08,11.3,' \
               '39.4,2.018E-08,10.4,41.2,1.105E-08,26.7,59.5,5.943E-09,52.2,76.1,2.686E-09,3.3,64.0,7.255E-09,347.4,' \
               '63.8,1.452E-09,299.9,53.6,1.234E-09,293.3,73.5,1.227E-09,178.7,63.5,3.562E-09,171.5,60.8,4.694E-09,' \
               '56.9,65.6,1.219E-09,47.3,56.4,4.381E-10,112.5,67.9,4.524E-10,91.9,59.3,6.204E-10,75.8,56.7,2.745E-10,' \
               '76.5,54.9,8.695E-10,82.3,56.5,5.408E-10,70.0,65.3,4.507E-10,73.8,71.5,2.089E-10,151.8,75.9,2.713E-10,' \
               '54.4,79.9,1.788E-11,30.1,72.2,2.374E-10,26.7,62.8,3.534E-10,17.2,63.8,1.147E-10,10.7,71.2,1.394E-10,' \
               '295.2,59.3,3.959E-08,222.9,58.8,6.480E-08,239.8,62.4,8.602E-09,278.3,61.8,6.799E-09,272.8,64.5,' \
               '4.508E-09,297.5,77.4,4.986E-09,314.3,77.4,2.470E-08,274.9,67.8,9.824E-09,183.7,67.1,6.896E-09,165.3,' \
               '66.6,4.530E-09,284.9,53.6,1.540E-09,197.5,69.8,1.774E-09,170.5,63.3,8.661E-09,351.8,76.4,1.414E-09,' \
               '339.4,54.5,9.119E-09,300.7,64.9,1.389E-08,260.5,73.1,8.045E-09,275.7,76.6,9.907E-09,53.5,79.1,' \
               '5.225E-09,52.9,75.9,5.090E-09,296.6,67.6,1.266E-08,247.1,42.4,1.808E-08,197.1,70.2,1.324E-08,113.2,' \
               '62.6,6.395E-10,164.7,62.8,2.550E-09,257.9,58.5,2.976E-09,264.0,55.1,4.384E-09,203.3,66.0,1.137E-08,' \
               '155.6,59.8,4.375E-09,167.9,72.4,3.124E-09,314.6,69.5,3.415E-09,60.4,74.8,5.632E-09,45.1,71.0,' \
               '3.080E-09,335.1,69.5,4.449E-09,63.5,61.7,6.763E-09,48.0,46.0,6.318E-10,7.4,62.5,1.831E-10,354.4,67.4,' \
               '7.131E-10,35.2,59.2,2.062E-09,55.4,54.7,6.528E-10,89.4,65.8,1.633E-09,108.5,67.7,4.253E-10,53.4,73.4,' \
               '6.663E-10,329.3,65.8,1.215E-09,309.8,61.1,6.925E-10,329.8,63.4,3.502E-10,355.5,73.9,8.197E-10,214.8,' \
               '70.1,3.688E-09,234.3,56.9,1.774E-09,240.0,52.9,7.943E-10,215.0,71.7,1.306E-09,235.0,74.4*4B'

        particle = WavssADclMeanDirectionalDataParticle(line, None, None, None)
        values = particle._build_parsed_values()
        my_dict = {x['value_id']: x['value'] for x in values}

        self.assertEqual(my_dict['serial_number'], '05781')
        self.assertEqual(my_dict['number_bands'], 86)
        self.assertEqual(my_dict['initial_frequency'], 0.03)
        self.assertEqual(my_dict['frequency_spacing'], 0.005)
        self.assertEqual(my_dict['mean_direction'], 214.05)
        self.assertEqual(my_dict['spread_direction'], 60.54)
        self.assertEqual(len(my_dict['psd_mean_directional']), MEAN_DIR_NUMBER_BANDS)
        self.assertEqual(len(my_dict['mean_direction_array']), MEAN_DIR_NUMBER_BANDS)
        self.assertEqual(len(my_dict['directional_spread_array']), MEAN_DIR_NUMBER_BANDS)
        self.assertEqual(my_dict['psd_mean_directional'][0], 7.459e-07)
        self.assertEqual(my_dict['mean_direction_array'][0], 197.1)
        self.assertEqual(my_dict['directional_spread_array'][0], 59.5)

    def test_parse_tspha(self):
        line = '2014/08/25 15:16:42.765 $TSPHA,20140825,151642,05781,buoyID,,,' \
               '344,15.659,0.783,0,' \
               '0.00,0.00,0.00,' '0.00,0.00,0.00,' '0.00,0.00,0.00,' '0.00,0.00,0.01,' '0.00,0.00,0.01,' \
               '0.00,0.00,0.01,' '0.00,0.00,0.01,' '0.00,0.00,0.00,' '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,' '0.00,-0.01,0.00,' '0.00,0.00,0.00,' '0.00,0.00,0.00,' '0.00,0.00,0.00,' \
               '0.00,0.00,-0.01,' '0.00,0.00,-0.01,' '0.00,0.00,-0.01,' '0.00,0.01,-0.01,' '0.00,0.01,-0.01,' \
               '0.00,0.01,-0.01,' '0.00,0.01,-0.01,' '0.00,0.01,-0.01,' '0.00,0.01,0.00,' '0.00,0.01,0.00,' \
               '0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,' \
               '0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,' \
               '-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,0.01,0.01,' \
               '0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,' \
               '0.01,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,' \
               '-0.01,-0.01,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,' \
               '-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,' \
               '0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,' \
               '0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,-0.01,0.00,-0.01,-0.01,' \
               '0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,-0.01,0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,' \
               '0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.01,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,-0.01,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,' \
               '0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,' \
               '-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,' \
               '-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,' \
               '0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,' \
               '0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.01,0.00,' \
               '0.00,0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,-0.01,0.00,0.00,' \
               '-0.01,0.00,0.00,-0.01,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.01,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,' \
               '0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00*79 '

        particle = WavssADclMotionDataParticle(line, None, None, None)
        values = particle._build_parsed_values()
        my_dict = {x['value_id']: x['value'] for x in values}

        samples = 344
        self.assertEqual(my_dict['serial_number'], '05781')
        self.assertEqual(my_dict['number_time_samples'], samples)
        self.assertEqual(my_dict['initial_time'], 15.659)
        self.assertEqual(my_dict['time_spacing'], 0.783)
        self.assertEqual(my_dict['solution_found'], 0)
        self.assertEqual(len(my_dict['heave_offset_array']), samples)
        self.assertEqual(len(my_dict['north_offset_array']), samples)
        self.assertEqual(len(my_dict['east_offset_array']), samples)
        self.assertEqual(my_dict['heave_offset_array'][0], 0.0)
        self.assertEqual(my_dict['north_offset_array'][7], 0.0)
        self.assertEqual(my_dict['north_offset_array'][8], -0.01)
        self.assertEqual(my_dict['east_offset_array'][2], 0.0)
        self.assertEqual(my_dict['east_offset_array'][3], 0.01)

    def test_parse_tspfb(self):
        line = '2015/07/30 02:07:47.852 $TSPFB,20150729,215253,05321,,,,' \
               '123,0.030,0.005,' \
               '6,0.030,0.005,' \
               '-0.12475,' \
               '-0.42156,0.01109,-0.65368,-0.11207,-0.34274,0.11105,-0.56087,0.016322,0.090185,0.74036,0.19947,' \
               '-0.041496,-0.067259,0.57661,0.19094,-0.20966,-0.2654,0.024548,-0.52033,0.033178,-0.037987,-0.054503,' \
               '-0.020514,0.0012896,-0.0016934,0.00011616,-NAN,0.0013071,-0.0017066,9.6419E-05,-NAN,0.0013285,' \
               '-0.0017135,7.9689E-05,-NAN,0.0013498,-0.0017157,6.6459E-05,-NAN,0.001369,-0.0017176,5.5246E-05,-NAN,' \
               '0.0013875,-0.0017237,4.4922E-05,-NAN,0.0014066,-0.001729,3.6866E-05,-NAN,0.0014269,-0.0017245,' \
               '3.2515E-05,-NAN,0.0014485,-0.0017053,3.1018E-05,-NAN,0.0014702,-0.0016719,2.9681E-05,-NAN,0.0014892,' \
               '-0.0016322,2.5282E-05,-NAN,0.0015041,-0.0015926,1.5621E-05,-NAN,0.0015171,-0.0015548,-9.2958E-07,' \
               '-NAN,0.0015307,-0.0015182,-2.3732E-05,-NAN,0.0015462,-0.0014814,-5.0298E-05,-NAN,0.0015583,' \
               '-0.0014457,-7.697E-05,-NAN,0.0015606,-0.0014145,-0.00010123,-NAN,0.0015552,-0.0013909,-0.00012458,' \
               '-NAN,0.0015473,-0.0013731,-0.00014802,-NAN,0.0015379,-0.0013535,-0.00017028,-NAN,0.0015236,' \
               '-0.0013253,-0.00019062,-NAN,0.0014979,-0.0012858,-0.00020896,-NAN,0.0014647,-0.0012429,-0.00022685,' \
               '-NAN,0.0014376,-0.0012073,-0.00024533,-NAN,0.0014217,-0.0011756,-0.00026033,-NAN,0.0014155,' \
               '-0.0011392,-0.0002689,-NAN,0.0014106,-0.0010925,-0.00027436,-NAN,0.0013993,-0.0010383,-0.00028228,' \
               '-NAN,0.0013839,-0.00098859,-0.00029864,-NAN,0.0013694,-0.00094441,-0.00032293,-NAN,0.0013598,' \
               '-0.00089523,-0.00034995,-NAN,0.0013556,-0.00083736,-0.00037763,-NAN,0.0013544,-0.00077289,' \
               '-0.00040494,-NAN,0.0013521,-0.00071092,-0.00043029,-NAN,0.0013435,-0.00065835,-0.00045306,-NAN,' \
               '0.0013296,-0.00060819,-0.00047365,-NAN,0.0013149,-0.00055328,-0.00049236,-NAN,0.0013044,-0.0004938,' \
               '-0.00050855,-NAN,0.001299,-0.00043401,-0.0005201,-NAN,0.0012921,-0.00037999,-0.00052601,-NAN,' \
               '0.0012809,-0.00033003,-0.00053139,-NAN,0.0012669,-0.00027814,-0.00054282,-NAN,0.0012499,-0.00022089,' \
               '-0.00056172,-NAN,0.0012282,-0.00015709,-0.00058541,-NAN,0.0011984,-8.8349E-05,-0.0006088,-NAN,' \
               '0.001158,-1.9296E-05,-0.00062844,-NAN,0.0011107,4.3754E-05,-0.00064343,-NAN,0.001058,9.9137E-05,' \
               '-0.00065373,-NAN,0.00099694,0.00014952,-0.00066076,-NAN,0.0009307,0.00019921,-0.00066921,-NAN,' \
               '0.00086795,0.00025117,-0.00068253,-NAN,0.0008152,0.0003018,-0.0006975,-NAN,0.00077474,0.00034766,' \
               '-0.00070964,-NAN,0.00073825,0.00039182,-0.00071688,-NAN,0.00069857,0.00044047,-0.00072373,-NAN,' \
               '0.0006608,0.0004996,-0.00073861,-NAN,0.00062916,0.00056626,-0.00076033,-NAN,0.00060174,0.00063063,' \
               '-0.00078239,-NAN,0.000573,0.00068806,-0.00080101,-NAN,0.00053696,0.00073925,-0.00081545,-NAN,' \
               '0.00049692,0.00079206,-0.00082852,-NAN,0.00045845,0.00085296,-0.00084116,-NAN,0.00041826,0.00091852,' \
               '-0.00085048,-NAN,0.00037408,0.00098256,-0.00085581,-NAN,0.00033287,0.0010399,-0.00085875,-NAN,' \
               '0.00030066,0.0010928,-0.00085959,-NAN,0.00027748,0.0011484,-0.00085747,-NAN,0.00025292,0.0012042,' \
               '-0.00085145,-NAN,0.00021236,0.0012543,-0.00084204,-NAN,0.00016132,0.0012973,-0.00083175,-NAN,' \
               '0.00011356,0.001336,-0.0008231,-NAN,7.1718E-05,0.0013781,-0.00081763,-NAN,3.1597E-05,0.001426,' \
               '-0.00081761,-NAN,-1.4997E-05,0.0014739,-0.00082596,-NAN,-6.8744E-05,0.0015179,-0.00084056,-NAN,' \
               '-0.00012047,0.0015566,-0.00085664,-NAN,-0.00016972,0.0015896,-0.00087088,-NAN,-0.00022141,0.001617,' \
               '-0.00088239,-NAN,-0.00027387,0.0016389,-0.00089424,-NAN,-0.00032233,0.0016564,-0.00090668,-NAN,' \
               '-0.00036254,0.0016728,-0.000915,-NAN,-0.0003953,0.0016877,-0.00091608,-NAN,-0.00042943,0.0016972,' \
               '-0.00091006,-NAN,-0.00046935,0.0017042,-0.00090178,-NAN,-0.00051237,0.0017167,-0.00089674,-NAN,' \
               '-0.00055704,0.0017379,-0.0008944,-NAN,-0.00060298,0.0017661,-0.00089182,-NAN,-0.00064975,0.0017928,' \
               '-0.00088782,-NAN,-0.00069622,0.0018117,-0.00088498,-NAN,-0.00074005,0.0018268,-0.00088918,-NAN,' \
               '-0.00077994,0.00184,-0.00090128,-NAN,-0.0008171,0.0018481,-0.00091756,-NAN,-0.0008517,0.0018494,' \
               '-0.0009333,-NAN,-0.00088255,0.0018446,-0.00094469,-NAN,-0.00091172,0.0018394,-0.00095258,-NAN,' \
               '-0.00094188,0.0018396,-0.00095792,-NAN,-0.00096976,0.0018406,-0.00095875,-NAN,-0.00098989,0.0018388,' \
               '-0.0009552,-NAN,-0.00099974,0.0018381,-0.00095076,-NAN,-0.0010028,0.001842,-0.00095027,-NAN,' \
               '-0.0010095,0.0018503,-0.00095687,-NAN,-0.0010248,0.001856,-0.0009635,-NAN,-0.0010461,0.0018502,' \
               '-0.0009612,-NAN,-0.0010725,0.0018382,-0.00095025,-NAN,-0.0011039,0.0018301,-0.00093574,-NAN,' \
               '-0.001139,0.0018288,-0.00092402,-NAN,-0.0011747,0.0018315,-0.00091508,-NAN,-0.001206,0.0018315,' \
               '-0.00090229,-NAN,-0.0012316,0.0018277,-0.00088582,-NAN,-0.001257,0.001826,-0.00086906,-NAN,' \
               '-0.0012843,0.0018269,-0.00085008,-NAN,-0.0013112,0.0018264,-0.00082511,-NAN,-0.0013366,0.001821,' \
               '-0.00079224,-NAN,-0.0013615,0.001809,-0.00075625,-NAN,-0.0013884,0.0017937,-0.00072997,-NAN*6E '

        fourier_data = WavssADclFourierDataParticle(line, None, None)
        particle = fourier_data._build_parsed_values()
        my_dict = {x['value_id']: x['value'] for x in particle}

        samples = 123
        bands = 6
        self.assertEqual(my_dict['serial_number'], '05321')
        self.assertEqual(my_dict['number_bands'], samples)
        self.assertEqual(my_dict['initial_frequency'], 0.03)
        self.assertEqual(my_dict['frequency_spacing'], 0.005)
        self.assertEqual(my_dict['number_directional_bands'], bands)
        self.assertEqual(my_dict['initial_directional_frequency'], 0.03)
        self.assertEqual(my_dict['directional_frequency_spacing'], 0.005)
        self.assertEqual(len(my_dict['fourier_coefficient_2d_array']), samples - 2)
        self.assertEqual(my_dict['fourier_coefficient_2d_array'][0], [-0.12475, -0.42156, 0.01109, -0.65368])

    def test_tspwa_telem(self):
        """
        Test a simple telemetered case that we can parse a single $TSPWA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspwa.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspwa_telem.yml", RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_tspna_telem(self):
        """
        Test a simple case that we can parse a single $TSPNA message
        """
        # this file also is missing a newline at the end of the file which tests the case of a missing line terminator
        with open(os.path.join(RESOURCE_PATH, 'tspna.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspna_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspna_with_nans_telem(self):
        """
        Test a simple case that we can parse a single $TSPNA message
        """
        # this file also is missing a newline at the end of the file which tests the case of a missing line terminator
        with open(os.path.join(RESOURCE_PATH, 'tspna_with_nan.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assertEqual(self.exception_callback_value, [])

    def test_tspma_telem(self):
        """
        Test a simple case that we can parse a single $TSPMA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspma.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspma_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspha_telem(self):
        """
        Test a simple case that we can parse a single $TSPHA message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspha.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspha_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_tspfb_telem(self):
        """
        Test a simple case that we can parse a single $TSPFB message
        """
        with open(os.path.join(RESOURCE_PATH, 'tspfb.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(1)

            self.assert_particles(particles, "tspfb_telem.yml", RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_simple(self):
        """
        Test a simple telemetered and recovered case with all the particle types
        """
        with open(os.path.join(RESOURCE_PATH, '20140825.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(6)

            self.assert_particles(particles, "20140825_telem.yml", RESOURCE_PATH)

        with open(os.path.join(RESOURCE_PATH, '20140825.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(6)

            self.assert_particles(particles, "20140825_recov.yml", RESOURCE_PATH)

        self.assertEqual(self.exception_callback_value, [])

    def test_tspwa_with_dplog(self):
        """
        Test a file with many tspwas that we ignore dplog marker lines
        """
        with open(os.path.join(RESOURCE_PATH, '20140804.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            # request more particles than are available
            particles = parser.get_records(26)

            # make sure we only get the available number of particles
            self.assertEqual(len(particles), 23)
            self.assertEqual(self.exception_callback_value, [])

    def test_bad_number_samples(self):
        """
        Test bad number of samples for all data particle types except tspwa (since that size is fixed in the regex)
        """
        with open(os.path.join(RESOURCE_PATH, 'bad_num_samples.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            n_test = 4  # number of particles to test
            particles = parser.get_records(n_test)

            # make sure none of the particles succeeded
            self.assertEqual(len(particles), 0)
            # check that there were 3 recoverable sample exceptions
            self.assertEqual(len(self.exception_callback_value), n_test)
            for i in range(0, n_test):
                self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))

    def test_unexpected(self):
        """
        Test with an unexpected line, confirm we get an exception
        """
        with open(os.path.join(RESOURCE_PATH, 'unexpected.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            # there are 4 lines in the file, 3rd, has error, TSPSA record is ignored
            n_test = 4
            particles = parser.get_records(n_test)
            self.assertEqual(len(particles), 2)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

    def test_bug_10046(self):
        """
        Test sample files from Redmine ticket #10046
        NaN values were found in other record types causing parser to fail.
        Verify parser parses files without error
        """
        with open(os.path.join(RESOURCE_PATH, '20150314.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            particles = parser.get_records(200)

            self.assertEqual(len(particles), 120)
            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, '20150315.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=False)

            particles = parser.get_records(200)

            self.assertEqual(len(particles), 120)
            self.assertEqual(self.exception_callback_value, [])

    def test_bug_11462(self):
        """
        Test sample files from Redmine ticket #11462
        NaN values were found in other record types causing parser to fail.
        Verify parser parses files without error
        """
        with open(os.path.join(RESOURCE_PATH, '20150730.wavss.log'), 'r') as file_handle:
            parser = WavssADclParser(file_handle, self.exception_callback, is_telemetered=True)

            tspwa_records = 24
            tspna_records = 24
            tspfb_records = 24
            tsp_records = tspfb_records + tspna_records + tspwa_records
            particles = parser.get_records(tsp_records + 10)

            self.assertEqual(len(particles), tsp_records)
            self.assertEqual(self.exception_callback_value, [])
