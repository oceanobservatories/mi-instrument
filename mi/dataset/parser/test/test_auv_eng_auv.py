#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@fid marine-integrations/mi/dataset/parser/test/test_auv_eng_auv.py
@author Jeff Roy
@brief Test code for a auv_eng_auv data parser

NOTE: there have been several other parsers built on auv_common tested already
all negative paths through the code are not again verified here.
Testing is limited to code specific to the derived classes of auv_eng_auv

"""

import os

from nose.plugins.attrib import attr

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
from mi.dataset.driver.auv_eng.auv.resource import RESOURCE_PATH
from mi.dataset.parser.auv_eng_auv import AuvEngAuvParser
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()


@attr('UNIT', group='mi')
class AuvEngAuvTestCase(ParserUnitTestCase):
    """
    auv_eng_auv Parser unit test suite
    """

    # IMAGENEX 852 TESTS
    def test_simple_imagenex(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_imagenex.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'imagenex_telem_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_imagenex.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'imagenex_recov_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_imagenex(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 2 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'imagenex_bad_timestamps.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 7

            self.assertEqual(len(particles), 7)
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    # DIGITAL USBL TESTS
    def test_simple_usbl(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_usbl.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'usbl_telem_22.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_usbl.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'usbl_recov_22.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_usbl(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 2 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'usbl_bad_timestamps.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 6

            self.assertEqual(len(particles), 6)
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    # TRI FIN MOTOR TESTS
    def test_simple_motor(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_motor.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(28)

            self.assert_particles(particles, 'motor_telem_28.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_motor.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(28)

            self.assert_particles(particles, 'motor_recov_28.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_motor(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 2 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'motor_bad_timestamp.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 4

            self.assertEqual(len(particles), 4)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    # EMERGENCY BOARD TESTS
    def test_simple_emergency(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_emergency.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'emergency_telem_7.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_emergency.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'emergency_recov_7.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_emergency(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'emergency_bad_timestamp.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 3

            self.assertEqual(len(particles), 3)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    # OIL COMPENSATOR TESTS
    def test_simple_oil_comp(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_oil_comp.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'oil_comp_telem_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_oil_comp.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'oil_comp_recov_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_oil_comp(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'oil_comp_bad_timestamps.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 4

            self.assertEqual(len(particles), 4)
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    # SMART BATTERY TESTS
    def test_simple_battery(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_battery.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'battery_telem_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_battery.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'battery_recov_20.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_battery(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'battery_bad_timestamp.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 4

            self.assertEqual(len(particles), 4)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    # DIGITAL TX BOARD TESTS
    def test_simple_tx_board(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_tx_board.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'tx_board_telem_22.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_tx_board.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'tx_board_recov_22.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_tx_board(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'tx_board_bad_timestamps.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 4

            self.assertEqual(len(particles), 4)
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    # FAULT MESSAGE TESTS
    def test_simple_fault(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_fault.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(35)

            self.assert_particles(particles, 'fault_telem_35.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_fault.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(35)

            self.assert_particles(particles, 'fault_recov_35.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_fault(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        with open(os.path.join(RESOURCE_PATH, 'fault_bad_timestamp.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 5

            self.assertEqual(len(particles), 5)
            self.assertEqual(len(self.exception_callback_value), 1)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)

    # AUV STATE TESTS
    def test_simple_state(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        """

        with open(os.path.join(RESOURCE_PATH, 'subset_state.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'state_telem_25.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset_state.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(25)

            self.assert_particles(particles, 'state_recov_25.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_bad_timestamp_state(self):
        """
        Read test data and pull out data particles.
        Assert the expected we get 1 errors due to incorrect epoch and mission time formats
        This tests the generic timestamp method with two parameters
        """

        # TODO the Mission time in this block looks to be way to big,  waiting to hear from Hydroid
        with open(os.path.join(RESOURCE_PATH, 'state_bad_timestamps.csv'), 'rU') as stream_handle:
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(10)  # ask for 10 should get 6

            self.assertEqual(len(particles), 6)
            self.assertEqual(len(self.exception_callback_value), 2)
            self.assertIsInstance(self.exception_callback_value[0], RecoverableSampleException)
            self.assertIsInstance(self.exception_callback_value[1], RecoverableSampleException)

    def test_get_many(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        This test parses a file containing all message types and verifies
        all of the engineering data messages
        """

        with open(os.path.join(RESOURCE_PATH, 'subset2_reduced.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            particles = parser.get_records(200)

            self.assert_particles(particles, 'subset2_reduced_telem.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

        with open(os.path.join(RESOURCE_PATH, 'subset2_reduced.csv'), 'rU') as stream_handle:

            # test the recovered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=False)

            particles = parser.get_records(200)

            self.assert_particles(particles, 'subset2_reduced_recov.yml', RESOURCE_PATH)

            self.assertEqual(self.exception_callback_value, [])

    def test_long_stream(self):
        """
        Read test data and pull out data particles.
        Assert that the results are those we expected.
        This test parses a very large file containing all message types and verifies
        there are no errors
        """

        with open(os.path.join(RESOURCE_PATH, 'subset2.csv'), 'rU') as stream_handle:

            # test the telemetered particle stream
            parser = AuvEngAuvParser(stream_handle,
                                     self.exception_callback,
                                     is_telemetered=True)

            parser.get_records(160000)

            self.assertEqual(self.exception_callback_value, [])


