#!/usr/bin/env python

"""
@package mi.idk.test.test_result_set
@file mi/idk/test/test_result_set.py
@author Emily Hahn
@brief Test code for result set
"""
__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import numpy
from mi.core.unit_test import MiUnitTest
from mi.idk.result_set import ResultSet
from mi.core.instrument.dataset_data_particle import DataParticleKey

from mi.core.instrument.dataset_data_particle import DataParticle

TEST_PATH = 'mi/idk/test/resources/'


class ResultSetUnitTest(MiUnitTest):

    def test_yml_verification(self):
        """
        Test for errors when loading the .yml and that these errors occur
        """

        # header is empty
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'empty_header.yml')

        # no particle_object in header
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'missing_object.yml')

        # not particle_type in header
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'missing_type.yml')

        # no 'data' section
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'missing_data.yml')

        # data section, but nothing in it
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'empty_data.yml')

        # no index and no dictionary marker
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'no_index.yml')

        # has dictionary marker, no index
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'no_index2.yml')

        # two of the same indices defined in yml
        with self.assertRaises(IOError):
            rs = ResultSet(TEST_PATH + 'duplicate_index.yml')

    def test_fake_particle(self):
        """
        Create a fake data particle class and test that comparison either fails or passes as expected
        """
        fdp = FakeDataParticle([])

        # particle is missing internal_timestamp
        rs = ResultSet(TEST_PATH + 'missing_timestamp.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should be missing timestamp, but verification passed")

        # test with particle object and type in header
        rs = ResultSet(TEST_PATH + 'fake_particle.yml')
        # expect this to pass
        if not rs.verify([fdp]):
            self.fail("Failed particle verification")

        # test with MULTIPLE in particle object and type in header
        rs = ResultSet(TEST_PATH + 'fake_multiple.yml')
        # expect this to pass
        if not rs.verify([fdp]):
            self.fail("Failed particle verification")

        # particle class does not match
        rs = ResultSet(TEST_PATH + 'class_mismatch.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have class mismatch, but verification passed")

        # particle stream does not match
        rs = ResultSet(TEST_PATH + 'stream_mismatch.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have stream mismatch, but verification passed")

        # particle class does not match inside particle
        rs = ResultSet(TEST_PATH + 'class_mismatch_multiple.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have class mismatch, but verification passed")

        # particle stream does not match inside particle
        rs = ResultSet(TEST_PATH + 'stream_mismatch_multiple.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have stream mismatch, but verification passed")

        # particle timestamp does not match
        rs = ResultSet(TEST_PATH + 'timestamp_mismatch.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have timestamp mismatch, but verification passed")

        # particle string does not match
        rs = ResultSet(TEST_PATH + 'string_mismatch.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have string mismatch, but verification passed")

        # particle float does not match
        rs = ResultSet(TEST_PATH + 'float_mismatch.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have float mismatch, but verification passed")

        # 2nd particle is empty
        rs = ResultSet(TEST_PATH + 'empty_particle.yml')
        # expect this to fail
        if rs.verify([fdp, fdp]):
            self.fail("Should have empty particle, but verification passed")

        # particle class does not match
        rs = ResultSet(TEST_PATH + 'bad_key_particle.yml')
        # expect this to fail
        if rs.verify([fdp]):
            self.fail("Should have key mismatch, but verification passed")

    def test_full_types(self):
        """
        Confirm that all data types pass or fail verification as expected
        """
        ftdp = FullTypesDataParticle([])

        # First test with the correct data in the yml to confirm everything passes
        rs = ResultSet(TEST_PATH + 'full_types.yml')
        if not rs.verify([ftdp]):
            self.fail("Failed verification")

        # All parameters should not match
        rs = ResultSet(TEST_PATH + 'full_bad_types.yml')
        # expect this to fail
        if rs.verify([ftdp]):
            self.fail("Should have failed verification, but verification passed")

    def test_timestamp(self):
        """
        Test that the timestamp string conversion is working
        """
        ftdp = FakeDataParticle([])

        # File contains 4 particles, each with different formatted timestamp string
        rs = ResultSet(TEST_PATH + 'timestamp_string.yml')
        # confirm all strings match
        if not rs.verify([ftdp, ftdp, ftdp, ftdp]):
            self.fail("Should have failed verification, but verification passed")

    def test_incorrect_length(self):
        """
        Test that not having the matching number of particles in the yml and results generates fails
        """
        ftdp = FakeDataParticle([])

        # only one particle in results file
        rs = ResultSet(TEST_PATH + 'fake_particle.yml')
        # compare to two, this should fail
        if rs.verify([ftdp, ftdp]):
            self.fail("Should have failed particle verification, but verification passed")

    def test_not_data_particle(self):
        """
        Test that a class that is not a data particle is not accepted
        """
        ndp = NotDataParticle()

        # class is not a subclass of DataParticle
        rs = ResultSet(TEST_PATH + 'not_data_particle.yml')
        # this should fail
        if rs.verify([ndp]):
            self.fail("Should have failed particle verification, but verification passed")

    def test_no_particle_timestamp(self):
        """
        Test if a class has not set the particle timestamp but one is in the .yml that they do not match
        """
        ftdp = FakeNoTsParticle([])

        # .yml file contains timestamp, class does not
        rs = ResultSet(TEST_PATH + 'fake_no_ts_particle.yml')
        # this should fail
        if rs.verify([ftdp]):
            self.fail("Should have failed particle verification, but verification passed")

    def test_missing_type_multiple(self):
        """
        Test if a header has MULTIPLE but the particle does not specify the type that this does not match
        """
        ftdp = FakeDataParticle([])

        # yml file is missing type in individual particle (stream)
        rs = ResultSet(TEST_PATH + 'missing_type_multiple.yml')
        # this should fail
        if rs.verify([ftdp]):
            self.fail("Should have failed particle verification, but verification passed")

    def test_multiple_bad_type_object(self):
        """
        Test that a bad type or bad object does not match
        """
        ftdp = FakeDataParticle([])

        # yml has bad type in individual particle (stream)
        rs = ResultSet(TEST_PATH + 'bad_type_multiple.yml')
        # this should fail
        if rs.verify([ftdp]):
            self.fail("Should have failed particle verification, but verification passed")

        # yml has bad class in individual particle (stream)
        rs = ResultSet(TEST_PATH + 'bad_class_multiple.yml')
        # this should fail
        if rs.verify([ftdp]):
            self.fail("Should have failed particle verification, but verification passed")

    def test_round(self):
        """
        Test that rounding occurs
        """
        fdp = FakeDataParticle([])

        # test with a rounding dictionary in the yml
        rs = ResultSet(TEST_PATH + 'fake_round.yml')
        # expect this to pass
        if not rs.verify([fdp]):
            self.fail("Failed particle verification")

        frp = FakeRoundParticle([])

        # test with rounding dictionary with a nested list
        rs = ResultSet(TEST_PATH + 'fake_round_list.yml')
        # expect this to pass
        if not rs.verify([frp]):
            self.fail("Failed particle verification")

    def test_particle_dict_compare(self):
        """
        test that a particle already converted to a dictionary can be compared
        """
        fdp = FakeDataParticle([])
        fdp_dict = fdp.generate_dict()

        # normal fake particle
        rs = ResultSet(TEST_PATH + 'fake_particle.yml')
        # expect this to pass
        if not rs.verify([fdp_dict]):
            self.fail("Failed particle verification")



# create a fake data particle class for testing with
class FakeDataParticle(DataParticle):
    _data_particle_type = 'fake_particle_stream'

    def _build_parsed_values(self):
        self.set_internal_timestamp(3200000000)
        return [self._encode_value('param_1', 'ABC', str),
                self._encode_value('param_2', 3.2, float)]


# create a second fake data particle class for testing with
class FullTypesDataParticle(DataParticle):
    _data_particle_type = 'full_types_particle_stream'

    def _build_parsed_values(self):
        self.set_internal_timestamp(323000000.3278492)
        return [self._encode_value('param_1', 'ABC', str),
                self._encode_value('param_2', ['ABC', '1234', 'DEF', 'GHIJK'], list),
                self._encode_value('param_3', 3.2239587667, float),
                self._encode_value('param_4', [1.20345678, 3.45349862098, 6.789235987647], list),
                self._encode_value('param_5', [[1.20345678, 3.45349862098], [6.789235987647, 8.924958735],
                                               [32.98249523, 59.332098755]], list),
                self._encode_value('param_6', 3, int),
                self._encode_value('param_7', [3, 4, 5], list),
                self._encode_value('param_8', numpy.nan, float),
                self._encode_value('param_9', [12.34, numpy.nan, 3.567], list),
                self._encode_value('param_10', [[12.34, numpy.nan], [3.567, 4.253908467]], list),
                {DataParticleKey.VALUE_ID: 'param_11', DataParticleKey.VALUE: None}]


# create a fake class that exists but is not a data particle
class NotDataParticle(object):
    _data_particle_type = 'fake_particle_stream'

    def _build_parsed_values(self):
        return []


# create a fake particle that doesn't set the timestamp
class FakeNoTsParticle(DataParticle):
    _data_particle_type = 'fake_particle_stream'

    def _build_parsed_values(self):
        return [self._encode_value('param_1', 'ABC', str),
                self._encode_value('param_2', 3.2, float)]


# create a fake particle with nested list for rounding
class FakeRoundParticle(DataParticle):
    _data_particle_type = 'fake_particle_stream'

    def _build_parsed_values(self):
        self.set_internal_timestamp(3200000000)
        return [self._encode_value('param_1', 1, int),
                self._encode_value('param_2', [[3.23, 42.5], [3.67, 921.24]], list)]


