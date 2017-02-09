#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_m_fcoeff
@fid marine-integrations/mi/dataset/parser/test/test_adcpt_mfcoeff.py
@author Ronald Ronquillo
@brief Test code for a Adcpt_M_FCoeff data parser
"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.parser.adcpt_m_fcoeff import AdcptMFCoeffParser

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'adcpt_m', 'resource')


@attr('UNIT', group='mi')
class AdcptMFcoeffParserUnitTestCase(ParserUnitTestCase):
    """
    Adcpt_M_FCoeff Parser unit test suite
    """

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a AdcptMFCoeff parser for recovered data.
        """
        parser = AdcptMFCoeffParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_m_fcoeff',
             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptMFCoeffInstrumentDataParticle'},
            file_handle,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same
        particles will be used for the driver test it is helpful to write them to yaml in the same
        form they need in the results.yml here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            log.warn("PRINT DICT: %s", particles[i].generate_dict())

            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), str):
                    fid.write('    %s: %r\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), float):
                    fid.write('    %s: %.8f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), list):
                    if isinstance(val.get('value')[0], float):
                        fid.write('    %s: [' % (val.get('value_id')))
                        fid.write(", ".join(map(lambda x: '{0:06f}'.format(x), val.get('value'))))
                        fid.write(']\n')
                    else:
                        fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """
        fid = open(os.path.join(RESOURCE_PATH, 'FCoeff1404180021.txt'), 'rb')

        self.stream_handle = fid

        self.parser = self.create_parser('AdcptMFCoeffInstrumentDataParticle', fid)

        particles = self.parser.get_records(1)

        self.particle_to_yml(particles, 'FCoeff1404180021.yml')
        fid.close()

    def test_parse_input(self):
        """
        Read a file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done in the
        tests below. This is mainly for debugging the regexes.
        """
        in_file = self.open_file('FCoeff1404180021.txt')
        parser = self.create_parser('AdcptMFCoeffInstrumentDataParticle', in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(1)
        self.assertEqual(len(result), 1)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

    def test_recov(self):
        """
        Read a file and pull out a data particle.
        Verify that the results are those we expected.
        """
        in_file = self.open_file('FCoeff1404180021.txt')
        parser = self.create_parser('AdcptMFCoeffInstrumentDataParticle', in_file)

        # In a single read, get all particles for this file.
        result = parser.get_records(1)

        self.assertEqual(len(result), 1)
        self.assert_particles(result, 'FCoeff1404180021.yml', RESOURCE_PATH)

        self.assertListEqual(self.exception_callback_value, [])
        in_file.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        # Line 4: Fields missing
        # Line 5: Frequencies missing
        # Line 6: Fields is a float
        # Line 7: Frequencies is a float
        # Line 8: Fields has a non digit
        # Line 9: Frequencies has a non digit

        # Line 12: Frequency Bands width missing
        # Line 13: Frequencies band centered missing
        # Line 14: Frequency Bands width is a float
        # Line 15: Frequencies band centered is a float
        # Line 16: Frequency Bands width has a non digit
        # Line 17: Frequencies band centered has a non digit

        # Line 18: Frequency(Hz) missing
        # Line 19: Frequency(Hz) is an int
        # Line 20: Frequency(Hz) has a non digit

        # Line 21: Band width(Hz) missing
        # Line 22: Band width(Hz) is an int
        # Line 23: Band width(Hz) has a non digit

        # Line 24: Energy density(m^2/Hz) missing
        # Line 25: Energy density(m^2/Hz) is an int
        # Line 26: Energy density(m^2/Hz) has a non digit

        # Line 27: Direction (deg) missing
        # Line 28: Direction (deg) is an int
        # Line 29: Direction (deg) has a non digit

        # Line 30: A1 missing
        # Line 31: A1 is an int
        # Line 32: A1 has a non digit

        # Line 33: B1 missing
        # Line 34: B1 is an int
        # Line 35: B1 has a non digit

        # Line 36: A2 missing
        # Line 37: A2 is an int
        # Line 38: A2 has a non digit

        # Line 39: B2 missing
        # Line 40: B2 is an int
        # Line 41: B2 has a non digit

        # Line 42: Check Factor missing
        # Line 43: Check Factor is an int
        # Line 44: Check Factor has a non digit

        # Line 45: Space delimiter missing

        fid = open(os.path.join(RESOURCE_PATH, 'Corrupt_FCoeff1404180021.txt'), 'rb')

        parser = self.create_parser('AdcptMFCoeffInstrumentDataParticle', fid)

        result = parser.get_records(1)
        self.assertEqual(len(result), 0)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
            log.debug('Exception: %s', self.exception_callback_value[i])

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        fid.close()

    def test_missing_file_time(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        fid = open(os.path.join(RESOURCE_PATH, 'FCoeffNoTime.txt'), 'rb')

        parser = self.create_parser('AdcptMFCoeffInstrumentDataParticle', fid)

        result = parser.get_records(1)
        self.assertEqual(len(result), 0)

        for i in range(len(self.exception_callback_value)):
            self.assert_(isinstance(self.exception_callback_value[i], RecoverableSampleException))
            log.debug('Exception: %s', self.exception_callback_value[i])

        self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

        fid.close()