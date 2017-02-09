"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_phsen_abcdef_dcl.py
@author Nick Almonte
@brief Test code for a phsen_abcdef_dcl data parser
"""

import os

from nose.plugins.attrib import attr

from mi.core.common import BaseEnum
from mi.core.log import get_logger
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.phsen_abcdef.dcl.resource import RESOURCE_PATH
from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclMetadataTelemeteredDataParticle, \
    PhsenAbcdefDclInstrumentTelemeteredDataParticle
from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclParser, PhsenAbcdefDclMetadataRecoveredDataParticle, \
    PhsenAbcdefDclInstrumentRecoveredDataParticle
from mi.dataset.test.test_parser import ParserUnitTestCase

log = get_logger()

# INPUT LOG FILES, TELEMETERED AND RECOVERED
#
# CE01ISSM-D00001-dcl35-phsen2 20140424.phsen2.log
# 10 Instrument records, file from the IDD
#
# phsen_dcl_large.log
# 10 Instrument records, 2 Control (metadata) records - 1 without battery_voltage, 1 with battery_voltage
#
# phsen_dcl_bad_checksum.log :
# 1 Instrument record, 1 Control (metadata) record, both records have a bad checksum
#
# phsen_dcl_co2_type.log
# 1 Instrument record, record_type is C02
#
# phsen_dcl_withBatteryAndWithoutBattery.log
# 2 Control (metadata) records
# 1 record without battery_voltage, 1 record with battery_voltage
#
# phsen_dcl_startOfDay.log
# 1 partial Instrument record (it is clipped at the beginning of the record due to the start of the day)
#
# phsen_dcl_endOfDay.log
# 1 partial Instrument record (it is clipped before the end of the record due to the end of the day)
#
# phsen_dcl_noData.log
# No instrument or control data, all lines with bracketed DCL entered data
#
# phsen_dcl_startOfDay_plusOneGoodRecord.log
# starts with a start-of-day clipped/partial record, then ends with a complete instrument record

INPUT_LOG_FILE_01 = 'CE01ISSM-D00001-dcl35-phsen2_20140424.phsen2.log'
INPUT_LOG_FILE_02 = 'phsen_dcl_large.log'
INPUT_LOG_FILE_03 = 'phsen_dcl_bad_checksum.log'
INPUT_LOG_FILE_04 = 'phsen_dcl_co2_type.log'
INPUT_LOG_FILE_05 = 'phsen_dcl_withBatteryAndWithoutBattery.log'
INPUT_LOG_FILE_06 = 'phsen_dcl_startOfDay.log'
INPUT_LOG_FILE_07 = 'phsen_dcl_endOfDay.log'
INPUT_LOG_FILE_08 = 'phsen_dcl_noData.log'
INPUT_LOG_FILE_09 = 'phsen_dcl_startOfDay_plusOneGoodRecord.log'

INPUT_LOG_FILE_01_YML_TELEMETERED = 'CE01ISSM-D00001-dcl35-phsen2_20140424.phsen2-TELEMETERED.yml'
INPUT_LOG_FILE_01_YML_RECOVERED = 'CE01ISSM-D00001-dcl35-phsen2_20140424.phsen2-RECOVERED.yml'
INPUT_LOG_FILE_02_YML_TELEMETERED = 'phsen_dcl_large-TELEMETERED.yml'
INPUT_LOG_FILE_02_YML_RECOVERED = 'phsen_dcl_large-RECOVERED.yml'
INPUT_LOG_FILE_03_YML_TELEMETERED = 'phsen_dcl_bad_checksum-TELEMETERED.yml'
INPUT_LOG_FILE_03_YML_RECOVERED = 'phsen_dcl_bad_checksum-RECOVERED.yml'
INPUT_LOG_FILE_05_YML_TELEMETERED = 'phsen_dcl_withBatteryAndWithoutBattery-TELEMETERED.yml'
INPUT_LOG_FILE_05_YML_RECOVERED = 'phsen_dcl_withBatteryAndWithoutBattery-RECOVERED.yml'
INPUT_LOG_FILE_06_YML_TELEMETERED = 'phsen_dcl_startOfDay-TELEMETERED.yml'
INPUT_LOG_FILE_06_YML_RECOVERED = 'phsen_dcl_startOfDay-RECOVERED.yml'
INPUT_LOG_FILE_07_YML_TELEMETERED = 'phsen_dcl_endOfDay-TELEMETERED.yml'
INPUT_LOG_FILE_07_YML_RECOVERED = 'phsen_dcl_endOfDay-RECOVERED.yml'
INPUT_LOG_FILE_09_YML_TELEMETERED = 'phsen_dcl_startOfDay_plusOneGoodRecord-TELEMETERED.yml'
INPUT_LOG_FILE_09_YML_RECOVERED = 'phsen_dcl_startOfDay_plusOneGoodRecord-RECOVERED.yml'


class DataTypeKey(BaseEnum):

    PHSEN_ABCDEF_DCL_RECOVERED = 'phsen_abcdef_dcl_recovered'
    PHSEN_ABCDEF_DCL_TELEMETERED = 'phsen_abcdef_dcl_telemetered'


@attr('UNIT', group='mi')
class PhsenAbcdefDclParserUnitTestCase(ParserUnitTestCase):
    """
    phsen_abcdef_dcl Parser unit test suite
    """
    def create_rec_parser(self, file_handle):
        """
        This function creates a PhsenAbcdefDcl parser for recovered data.
        """
        return PhsenAbcdefDclParser(self.config.get(DataTypeKey.PHSEN_ABCDEF_DCL_RECOVERED),
                                    file_handle, self.rec_exception_callback)

    def create_tel_parser(self, file_handle):
        """
        This function creates a PhsenAbcdefDcl parser for telemetered data.
        """
        return PhsenAbcdefDclParser(self.config.get(DataTypeKey.PHSEN_ABCDEF_DCL_TELEMETERED),
                                    file_handle, self.tel_exception_callback)

    def open_file(self, filename):

        return open(os.path.join(RESOURCE_PATH, filename), mode='r')

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1

    def tel_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.tel_exception_callback_value = exception
        self.tel_exceptions_detected += 1

    def setUp(self):

        ParserUnitTestCase.setUp(self)

        self.config = {
            DataTypeKey.PHSEN_ABCDEF_DCL_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_dcl',
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    'metadata_particle_class_key': PhsenAbcdefDclMetadataRecoveredDataParticle,
                    'data_particle_class_key': PhsenAbcdefDclInstrumentRecoveredDataParticle
                }
            },
            DataTypeKey.PHSEN_ABCDEF_DCL_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen_abcdef_dcl',
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    'metadata_particle_class_key': PhsenAbcdefDclMetadataTelemeteredDataParticle,
                    'data_particle_class_key': PhsenAbcdefDclInstrumentTelemeteredDataParticle
                }
            },
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0
        self.tel_state_callback_value = None
        self.tel_file_ingested_value = False
        self.tel_publish_callback_value = None
        self.tel_exception_callback_value = None
        self.tel_exceptions_detected = 0
        self.maxDiff = None

    def create_yml(self):
        """
        This is added as a testing helper, not actually as part of the parser tests. This utility creates a yml file
        """

        fid = open(os.path.join(RESOURCE_PATH, INPUT_LOG_FILE_02), 'r')
        # self.stream_handle = fid

        # Create YML for data
        parser = self.create_tel_parser(fid)
        particles = parser.get_records(12)
        self.particle_to_yml(particles, 'phsen_dcl_large-TELEMETERED.yml')

        fid.close()

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml files here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')
        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()
            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_bad_checksum_recovered(self):
        """
        Verifies the 2 particles (1 instrument, 1 control) from a log file where both particle
        data sets have a bad checksum
        """
        # BAD CHECKSUM (2 particles)
        input_file = INPUT_LOG_FILE_03
        expected_particles = 2
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_03_YML_RECOVERED, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_bad_checksum_telemetered(self):
        """
        Verifies the 2 particles (1 instrument, 1 control) from a log file where both particle
        data sets have a bad checksum
        """
        # BAD CHECKSUM (2 particles)
        input_file = INPUT_LOG_FILE_03
        expected_particles = 2
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_03_YML_TELEMETERED, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_co2_type_telemetered(self):
        """
        Verifies that no particles are created from a log file that includes data from a log file that includes C02
        instrument data
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_04
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_co2_type_recovered(self):
        """
        Verifies that no particles are created from a log file that includes data from a log file that includes C02
        instrument data
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_04
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_noData_telemetered(self):
        """
        Verifies that no particles are created from a log file that includes no data
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_08
        expected_particles = 0
        total_records = expected_particles + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, 0)
        in_file.close()

    def test_noData_recovered(self):
        """
        Verifies that no particles are created from a log file that includes no data
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_08
        expected_particles = 0
        total_records = expected_particles + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, 0)
        in_file.close()

    def test_endOfDay_telemetered(self):
        """
        Verifies that no particles are created from a log file that includes incomplete log data for a single instrument
        data record to span multiple files, when the record is being written out as the day changes
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_07
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_endOfDay_recovered(self):
        """
        Verifies that no particles are created from a log file that includes incomplete log data for a single instrument
        data record to span multiple files, when the record is being written out as the day changes
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_07
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_startOfDay_telemetered(self):
        """
        Verifies that no particles are created from a log file that includes incomplete log data for a single instrument
        data record to span multiple files, when the record is being written out as the day changes
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_06
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_startOfDay_recovered(self):
        """
        Verifies that no particles are created from a log file that includes incomplete log data for a single instrument
        data record to span multiple files, when the record is being written out as the day changes
        """
        # No Data (0 particles)
        input_file = INPUT_LOG_FILE_06
        expected_particles = 0
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_phsen_dcl_startOfDay_plusOneGoodRecord_telemetered(self):
        """
        Verifies that 1 instrument particle is produced despite incomplete (start of day) data at the top of the
        log file
        """
        # incomplete start of day data and good data (1 metadata particles)
        input_file = INPUT_LOG_FILE_09
        expected_particles = 1
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_09_YML_TELEMETERED, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_phsen_dcl_startOfDay_plusOneGoodRecord_recovered(self):
        """
        Verifies that 1 instrument particle is produced despite incomplete (start of day) data at the top of the
        log file
        """
        # incomplete start of day data and good data (1 metadata particles)
        input_file = INPUT_LOG_FILE_09
        expected_particles = 1
        expected_exceptions = 1
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_09_YML_RECOVERED, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_withBatteryAndWithoutBattery_telemetered(self):
        """
        Verifies the 2 metadata particles (2 control), one with battery data and one without battery data
        """
        # Battery & No Battery (2 metadata particles)
        input_file = INPUT_LOG_FILE_05
        expected_particles = 2
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_05_YML_TELEMETERED, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_withBatteryAndWithoutBattery_recovered(self):
        """
        Verifies the 2 metadata particles (2 control), one with battery data and one without battery data
        """
        # Battery & No Battery (2 metadata particles)
        input_file = INPUT_LOG_FILE_05
        expected_particles = 2
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_05_YML_RECOVERED, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_large_telemetered(self):
        """
        Verifies the 10 instrument particles and 2 metadata (control) particles
        """
        # 10 instrument, 2 metadata (control)
        input_file = INPUT_LOG_FILE_02
        expected_particles = 12
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_02_YML_TELEMETERED, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_large_recovered(self):
        """
        Verifies the 10 instrument particles and 2 metadata (control) particles
        """
        # 10 instrument, 2 metadata (control)
        input_file = INPUT_LOG_FILE_02
        expected_particles = 12
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_02_YML_RECOVERED, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_file_from_IDD_telemetered(self):
        """
        Verifies the 10 instrument particles from the log file included with the PHSEN DCL IDD
        """
        # 10 instrument records
        input_file = INPUT_LOG_FILE_01
        expected_particles = 10
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_tel_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_01_YML_TELEMETERED, RESOURCE_PATH)
        self.assertEqual(self.tel_exceptions_detected, expected_exceptions)
        in_file.close()

    def test_file_from_IDD_recovered(self):
        """
        Verifies the 10 instrument particles from the log file included with the PHSEN DCL IDD
        """
        # 10 instrument records
        input_file = INPUT_LOG_FILE_01
        expected_particles = 10
        expected_exceptions = 0
        total_records = expected_particles + expected_exceptions + 1

        in_file = self.open_file(input_file)
        parser = self.create_rec_parser(in_file)
        particles = parser.get_records(total_records)
        self.assertEqual(len(particles), expected_particles)
        self.assert_particles(particles, INPUT_LOG_FILE_01_YML_RECOVERED, RESOURCE_PATH)
        self.assertEqual(self.rec_exceptions_detected, expected_exceptions)
        in_file.close()
