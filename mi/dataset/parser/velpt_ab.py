#!/usr/bin/env python

"""
@package mi.dataset.parser
@file /mi/dataset/parser/velpt_ab.py
@author Chris Goodrich
@brief Parser for the velpt_ab recovered dataset driver
Release notes:

initial release
"""
__author__ = 'Chris Goodrich'
__license__ = 'Apache 2.0'

import struct

from mi.core.exceptions import RecoverableSampleException
from mi.core.log import get_logger
log = get_logger()
from mi.dataset.parser.velpt_ab_particles import VelptAbDataParticle
from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.common import BaseEnum
from mi.core.exceptions import ConfigurationException

"""
Sample Aquadopp Velocity Data Record (42 bytes)
A5 01 1500 15 16 13 00 14 08 0000 0000 7500 8C3B AC07 F4FF EEFF 00 11 F500 3E08 1B00 E5FF E9FF 8E 87 71 00 2CBB
---------------------------------------------------------------------------------------------------------------
Data Mapping:
Read                          Swapped     Name                Offset
A5      -> Sync               A5
01      -> Id                 01
1500    -> Record size        0015
15      -> Minute             15
16      -> Second             16
13      -> Day                13
00      -> Hour               00
14      -> Year               14
08      -> Month              08
0000    -> Error              0000        error_code          10
0000    -> AnaIn1             0000        analog1             12
7500    -> Battery            0075        battery_voltage     14
8C3B    -> SoundSpeed/AnaIn2  3B8C        sound_speed_analog2 16
AC07    -> Heading            07AC        heading             18
F4FF    -> Pitch              FFF4        pitch               20
EEFF    -> Roll               FFEE        roll                22
00      -> PressureMSB        00                              24
11      -> Status             11          status              25
F500    -> PressureLSW        00F5        pressure            26
3E08    -> Temperature        083E        temperature         28
1B00    -> Vel B1/X/E         001B        velocity_beam1      30
E5FF    -> Vel B2/Y/N         FFE5        velocity_beam2      32
E9FF    -> Vel B3/Z/U         FFE9        velocity_beam3      34
8E      -> Amp B1             8E          amplitude_beam1     36
87      -> Amp B2             87          amplitude_beam2     37
71      -> Amp B3             71          amplitude_beam3     38
00      -> Fill               00
2CBB    -> Checksum           BB2C

Sample Diagnostics Header Record (36 bytes)
A5 06 1200 1400 0100 00 00 00 00 2016 1300 1408 0000 0000 0000 0000 0000 000000000000 9FDA
------------------------------------------------------------------------------------------
Data Mapping:
Read                            Swapped         Name                        Offset
A5              -> Sync         A5
60              -> Id           60
1200            -> Record size  0012
1400            -> Records      0014            records_to_follow           4
0100            -> Cell         0001            cell_number_diagnostics     6
00              -> Noise1       00              noise_amplitude_beam1       8
00              -> Noise2       00              noise_amplitude_beam2       9
00              -> Noise3       00              noise_amplitude_beam3       10
00              -> Noise4       00              noise_amplitude_beam4       11
2016            -> ProcMagn1    1620            processing_magnitude_beam1  12
1300            -> ProcMagn2    0013            processing_magnitude_beam2  14
1408            -> ProcMagn3    0814            processing_magnitude_beam3  16
0000            -> ProcMagn4    0000            processing_magnitude_beam4  18
0000            -> Distance1    0000            distance_beam1              20
0000            -> Distance2    0000            distance_beam2              22
0000            -> Distance3    0000            distance_beam3              23
0000            -> Distance4    0000            distance_beam4              26
000000000000    -> Spare        000000000000
9FDA            -> Checksum     DA9F

Sample Diagnostics Data Record (42 bytes)
A5 80 1500 20 17 13 00 14 08 0000 0000 7500 8C3B A307 F3FF F1FF 00 11 EF00 3B08 67FF C8FA D916 35 33 35 00 B1F7
---------------------------------------------------------------------------------------------------------------
Data Mapping:
Read                            Swapped     Name                Offset
A5      -> Sync                 A5
80      -> Id                   80
1500    -> Record size          0015
20      -> Minute               15
17      -> Second               16
13      -> Day                  13
00      -> Hour                 00
14      -> Year                 14
08      -> Month                08
0000    -> Error                0000        error_code          10
0000    -> AnaIn1               0000        analog1             12
7500    -> Battery              0075        battery_voltage     14
8C3B    -> SoundSpeed/AnaIn2    3B8C        sound_speed_analog2 16
A307    -> Heading              07A3        heading             18
F3FF    -> Pitch                FFF3        pitch               20
F1FF    -> Roll                 FFF1        roll                22
00      -> PressureMSB          00                              24
11      -> Status               11          status              25
EF00    -> PressureLSW          00EF        pressure            26
3B08    -> Temperature          083B        temperature         28
67FF    -> Vel B1/X/E           FF67        velocity_beam1      30
C8FA    -> Vel B2/Y/N           FAC8        velocity_beam2      32
D916    -> Vel B3/Z/U           16D9        velocity_beam3      34
35      -> Amp B1               35          amplitude_beam1     36
33      -> Amp B2               33          amplitude_beam2     37
35      -> Amp B3               35          amplitude_beam3     38
00      -> Fill                 00
B1F7    -> Checksum             F7B1

"""


class VelptAbParticleClassKey (BaseEnum):
    """
    An enum for the keys application to the pco2w abc particle classes
    """
    METADATA_PARTICLE_CLASS = 'metadata_particle_class'
    DIAGNOSTICS_PARTICLE_CLASS = 'diagnostics_particle_class'
    INSTRUMENT_METADATA_PARTICLE_CLASS = 'instrument_metadata_particle_class'
    INSTRUMENT_PARTICLE_CLASS = 'instrument_particle_class'


class VelptAbParser(SimpleParser):
    """
    Class used to parse the velpt_ab data set.
    """
    # This marks the first byte in all six record types.
    SYNC_MARKER = b'\xA5'

    # This byte follows the Sync Byte and lets the parser know
    # what type of record is being parsed.
    VELOCITY_DATA_ID = b'\x01'
    DIAGNOSTIC_HEADER_ID = b'\x06'
    DIAGNOSTIC_DATA_ID = b'\x80'
    HARDWARE_CONFIGURATION_ID = b'\x05'
    HEAD_CONFIGURATION_ID = b'\x04'
    USER_CONFIGURATION_ID = b'\x00'

    # This is used if the Diagnostics Header record is
    # bad or not present. The number of diagnostics records
    # expected is defaulted to 20 as that number seems common
    # to the existing data files from the instrument. This is
    # configurable pre-deployment. If more or fewer records
    # are encountered, particles will be produced but the parser
    # will issue a warning indicating too many or too few and
    # the number found versus the number expected.
    DEFAULT_DIAGNOSTICS_COUNT = 20

    def __init__(self,
                 config,
                 file_handle,
                 exception_callback):

        self._record_buffer = []
        self._calculated_checksum = 0
        self._current_record = ''
        self._velocity_data = False
        self._diagnostic_header = False
        self._diagnostic_header_published = False
        self._diagnostic_data = False
        self._build_hardware_config_data = False
        self._build_head_config_data = False
        self._build_user_config_data = False
        self._end_of_file = False
        self._sending_diagnostics = False
        self._build_hardware_config_data = False
        self._config_metadata_built = False
        self._hardware_config_dict_generated = False
        self._head_config_dict_generated = False
        self._user_config_dict_generated = False
        self._bad_diagnostic_header = False
        self._build_config_metadata = False
        self._first_diagnostics_record = False
        self._diagnostics_count = 0
        self._total_diagnostic_records = 0
        self._velocity_data_dict = {}
        self._diagnostics_header_dict = {}
        self._diagnostics_data_dict = {}
        self._hardware_config_dict = VelptAbDataParticle.generate_empty_hardware_config_dict()
        self._head_config_dict = VelptAbDataParticle.generate_empty_head_config_dict()
        self._user_config_dict = VelptAbDataParticle.generate_empty_user_config_dict()
        self._instrument_metadata_dict = {}
        self._diagnostics_header_record = ''
        self._file_handle = file_handle

        # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)

            # Set the metadata and data particle classes to be used later

            if VelptAbParticleClassKey.METADATA_PARTICLE_CLASS in particle_classes_dict and \
               VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS in particle_classes_dict and \
                VelptAbParticleClassKey.INSTRUMENT_METADATA_PARTICLE_CLASS in particle_classes_dict and \
               VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS in particle_classes_dict:

                self._metadata_class = config[
                    DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][VelptAbParticleClassKey.METADATA_PARTICLE_CLASS]
                self._diagnostics_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    VelptAbParticleClassKey.DIAGNOSTICS_PARTICLE_CLASS]
                self._velocity_data_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    VelptAbParticleClassKey.INSTRUMENT_PARTICLE_CLASS]
                self._instrument_metadata_class = config[DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT][
                    VelptAbParticleClassKey.INSTRUMENT_METADATA_PARTICLE_CLASS]
            else:
                log.error(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.error('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

        super(VelptAbParser, self).__init__(config, file_handle, exception_callback)

    def good_record_type(self, id_byte):
        """
        Determine the record type and let the caller now if
        a good record type was found or not.
        :param id_byte: The ID byte from the record
        :return: boolean indicating success or failure
        """
        status = True
        self._velocity_data = False
        self._diagnostic_header = False
        self._diagnostic_data = False
        self._build_hardware_config_data = False
        self._build_head_config_data = False
        self._build_user_config_data = False

        # Determine the type of processing based on the ID byte
        if id_byte == self.VELOCITY_DATA_ID:
            self._velocity_data = True
        elif id_byte == self.DIAGNOSTIC_HEADER_ID:
            self._diagnostic_header = True
            self._diagnostic_header_published = False
            self._bad_diagnostic_header = False
        elif id_byte == self.DIAGNOSTIC_DATA_ID:
            self._diagnostic_data = True
        elif id_byte == self.HARDWARE_CONFIGURATION_ID:
            self._build_hardware_config_data = True
        elif id_byte == self.HEAD_CONFIGURATION_ID:
            self._build_head_config_data = True
        elif id_byte == self.USER_CONFIGURATION_ID:
            self._build_user_config_data = True
        else:
            status = False

        return status

    def _bad_checksum(self, record, length, checksum):
        """
        Calculate the record checksum and compare it to
        the checksum stored in the record. A bad checksum
        will cause the record to be ignored. A warning
        will be issued. Parsing continues.
        :param record: the read-in record
        :record_checksum: the checksum from the record
        :return: boolean
        """
        # 46476 is the base value of the checksum given in the IDD as 0xB58C
        self._calculated_checksum = 46476

        for x in range(0, length-3, 2):
            self._calculated_checksum += struct.unpack('<H', record[x:x+2])[0]

        # Modulo 65536 is applied to the checksum to keep it a 16 bit value
        self._calculated_checksum %= 65536

        if self._calculated_checksum != checksum:
            return True
        else:
            return False

    def load_record(self, fh):
        """
        Attempt to load a data record.
        :param fh: The file handle
        :return: boolean indicating success or failure
        """
        record_start = 0

        # Assume the operation will be successful
        status = True

        # Read the Sync byte
        sync_byte = fh.read(1)

        # If the first byte is a valid sync byte
        # determine the record type and the record length.
        if sync_byte == self.SYNC_MARKER:

            # Need to capture the record start address for reading the entire record later
            record_start = fh.tell() - 1

            # Get the ID byte and see if it's a valid record
            id_byte = fh.read(1)

            if not self.good_record_type(id_byte):
                status = False
                log.warning('Found invalid ID byte: %d, at %d skipping to next byte',
                            struct.unpack('B', id_byte)[0], fh.tell()-1)
                self._exception_callback(
                    RecoverableSampleException('Found Invalid ID Byte, skipping to next byte'))

        elif sync_byte == '':  # Found the end of the file
            self._end_of_file = True
            status = False
        else:
            status = False
            log.warning('Found invalid sync byte: %d at %d , skipping to next byte',
                        struct.unpack('B', sync_byte)[0], fh.tell()-1)
            self._exception_callback(
                RecoverableSampleException('Found Invalid Sync Byte, skipping to next byte'))

        # If the record is valid, read it
        if status:
            record_length = struct.unpack('<H', fh.read(2))[0]*2
            fh.seek(record_start, 0)
            self._current_record = fh.read(record_length)

            # If the length of what was read matches what we asked for,
            # update the next record start position. Otherwise we found
            # a malformed record at the end of the file. 
            if len(self._current_record) == record_length:

                # Check that the checksum of this record is good
                stored_checksum = struct.unpack('<H', self._current_record[(record_length-2):record_length])[0]

                if self._bad_checksum(self._current_record, record_length, stored_checksum):

                    # Did the checksum fail on a config record?
                    # If so, don't try to generate that part of
                    # the instrument metadata particle
                    self._build_hardware_config_data = False
                    self._build_head_config_data = False
                    self._build_user_config_data = False

                    # Did the checksum fail on a diagnostic header record?
                    if self._diagnostic_header:
                        self._total_diagnostic_records = self.DEFAULT_DIAGNOSTICS_COUNT  # Use the default diag count
                        self._bad_diagnostic_header = True
                        self._sending_diagnostics = True  # The header is bad, the records may be okay
                        log.warning('Diagnostic Header Invalid')
                        self._exception_callback(
                            RecoverableSampleException('Diagnostic Header Invalid, no particle generated'))

                    log.warning('Invalid checksum: %d, expected %d - record will not be processed',
                                stored_checksum, self._calculated_checksum)
                    self._exception_callback(
                        RecoverableSampleException('Invalid checksum, no particle generated'))

                    status = False

            else:
                self._end_of_file = True
                status = False
                log.warning('Last record in file was malformed')
                self._exception_callback(
                    RecoverableSampleException('Last record in file malformed, no particle generated'))

        return status

    def build_instrument_metadata_particle(self, timestamp):
        """
        The instrument metadata particle is built from three separate records:
        the hardware configuration record, the head configuration record and the
        user configuration record. These should all be concentrated at the very
        beginning of the recovered data file. This assumption is made because the
        instrument is configured before being deployed so the records holding
        this data would be stored first. The data files seen as of the date this
        code was written all start with the three config records, then a quantity
        of velocity data records, then a group of diagnostics (header plus data
        records) followed by more velocity data records. This sequence can be
        repeated a number of times in one file, though the config data will only
        occur once. It is remotely possible that a group of diagnostics could
        occur before any velocity data records are encountered. Publishing of
        the instrument metadata particle is triggered by encountering either the
        first velocity data record or the first diagnostics data record.
        Counting to see if all three configuration records were encountered and
        then producing the instrument metadata particle was rejected as it is
        remotely possible that one of the configuration records could be missing.
        """
        self._config_metadata_built = True

        date_time_group = VelptAbDataParticle.get_date_time_string(self._current_record)

        instrument_metadata_dict = VelptAbDataParticle.generate_instrument_metadata_dict\
            (date_time_group, self._hardware_config_dict, self._head_config_dict,
             self._user_config_dict)

        particle = self._extract_sample(self._instrument_metadata_class,
                                        None,
                                        instrument_metadata_dict,
                                        timestamp)

        self._record_buffer.append(particle)

        # Check to see if all the configuration records were found
        if not self._hardware_config_dict_generated:
            # Log a warning for the missing hardware config record
            log.warning(
                'Hardware configuration record invalid or not present in recovered data')
            self._exception_callback(RecoverableSampleException(
                'Hardware configuration record invalid or not present in recovered data'))

        if not self._head_config_dict_generated:
            # Log a warning for the missing head config record
            log.warning('Head configuration record invalid or not present in recovered data')
            self._exception_callback(RecoverableSampleException(
                'Head configuration record invalid or not present in recovered data'))

        if not self._user_config_dict_generated:
            # Log a warning for the missing user config record
            log.warning('User configuration record invalid or not present in recovered data')
            self._exception_callback(RecoverableSampleException(
                'User configuration record invalid or not present in recovered data'))

    def process_velocity_data(self):
        """
        Handles the processing of velocity data particles and handles error processing if events
        which should have occurred prior to receiving a velocity record did not happen.
        """
        # Get the timestamp of the velocity record in case we need it for the metadata particle.
        timestamp = VelptAbDataParticle.get_timestamp(self._current_record)

        # Check to see if the instrument metadata particle has been produced yet
        # If not, produce it now as this is the first velocity record. This assumes
        # that velocity data records will occur BEFORE diagnostics data records.
        if not self._config_metadata_built:
            self.build_instrument_metadata_particle(timestamp)

        # If this flag is still indicating TRUE, it means we found NO diagnostic records.
        # That's an error!
        if self._first_diagnostics_record:
            self._first_diagnostics_record = False
            log.warning('No diagnostic records present, just a header.'
                        'No particles generated')
            self._exception_callback(
                RecoverableSampleException('No diagnostic records present, just a header.'
                                           'No particles generated'))

        # This flag indicates that diagnostics were being produced and now that
        # the first velocity record has been encountered, it's time to match the
        # number of diagnostics particles produced against the number of diagnostic
        # records expected from the diagnostics header.
        if self._sending_diagnostics:
            self._sending_diagnostics = False
            if self._total_diagnostic_records != self._diagnostics_count:
                if self._diagnostics_count < self._total_diagnostic_records:
                    log.warning('Not enough diagnostics records, got %s, expected %s',
                                self._diagnostics_count, self._total_diagnostic_records)
                    self._exception_callback(
                        RecoverableSampleException('Not enough diagnostics records'))

                elif self._diagnostics_count > self._total_diagnostic_records:
                    log.warning('Too many diagnostics records, got %s, expected %s',
                                self._diagnostics_count, self._total_diagnostic_records)
                    self._exception_callback(
                        RecoverableSampleException('Too many diagnostics records'))
                    self._diagnostics_count = 0
                    self._total_diagnostic_records = 0

        velocity_data_dict = VelptAbDataParticle.generate_data_dict(self._current_record)

        particle = self._extract_sample(self._velocity_data_class,
                                        None,
                                        velocity_data_dict,
                                        timestamp)

        self._record_buffer.append(particle)

    def process_diagnostic_data(self):
        """
        Handles the processing of diagnostic data particles and handles error processing if events
        which should have occurred prior to receiving a diagnostic record did not happen.
        """
        # As diagnostics records have the same format as velocity records
        # you can use the same routine used to break down the velocity data

        timestamp = VelptAbDataParticle.get_timestamp(self._current_record)
        date_time_group = VelptAbDataParticle.get_date_time_string(self._current_record)

        self._diagnostics_data_dict = VelptAbDataParticle.generate_data_dict(self._current_record)

        # Check to see if the instrument metadata particle has been produced yet
        # If not, produce it now as this is the first diagnostics record. This assumes
        # that diagnostics data records will occur BEFORE velocity data records.
        if not self._config_metadata_built:
            self.build_instrument_metadata_particle(timestamp)

        # Upon encountering the first diagnostics record, grab its timestamp
        # for use in the diagnostics metadata particle. Produce that metadata
        # particle now.
        if self._first_diagnostics_record:
            self._first_diagnostics_record = False

            diagnostics_header_dict = VelptAbDataParticle.generate_diagnostics_header_dict(
                date_time_group, self._diagnostics_header_record)
            self._total_diagnostic_records = VelptAbDataParticle.\
                get_diagnostics_count(self._diagnostics_header_record)
            particle = self._extract_sample(self._metadata_class,
                                            None,
                                            diagnostics_header_dict,
                                            timestamp)
            self._diagnostic_header_published = True

            self._record_buffer.append(particle)

        # Cover the case where unexpected diagnostics records are encountered
        elif ((not self._diagnostic_header_published) | (not self._sending_diagnostics))\
            & (not self._bad_diagnostic_header):
            self._total_diagnostic_records = self.DEFAULT_DIAGNOSTICS_COUNT
            self._diagnostic_header_published = True
            log.warning('Unexpected diagnostic data record encountered')
            self._exception_callback(
                RecoverableSampleException('Unexpected diagnostic data record encountered, not preceded by header'))

        particle = self._extract_sample(self._diagnostics_class,
                                        None,
                                        self._diagnostics_data_dict,
                                        timestamp)

        self._record_buffer.append(particle)

        self._diagnostics_count += 1

    def parse_file(self):
        """
        Parser for velpt_ab data.
        """
        while not self._end_of_file:

            # Determine the type of record and load it for processing.
            good_record = self.load_record(self._file_handle)

            # Sequence through the various expected record types
            if good_record:

                if self._velocity_data:
                    self.process_velocity_data()

                elif self._diagnostic_data:
                    self.process_diagnostic_data()

                # Finding a diagnostic header in the data requires some
                # extra processing as the header record has no time tag
                # we need to get that from the first diagnostic record.
                # We also have to check that the number of following
                # diagnostic data records matches what is in the header.
                elif self._diagnostic_header:
                    self._first_diagnostics_record = True
                    self._diagnostics_count = 0
                    self._diagnostics_header_record = self._current_record
                    self._sending_diagnostics = True

                # This record and the following two are instrument configuration
                # records. We only expect one of each and we expect to get all
                # three. One instrument metadata particle is built from these
                # three records. Keep track of which ones we have and note which
                # ones are missing. Once a velocity record is received the
                # assumption is that we are done with the config data.
                elif self._build_hardware_config_data:
                    self._hardware_config_dict = VelptAbDataParticle.\
                        generate_hardware_config_dict(self._current_record)
                    self._hardware_config_dict_generated = True
                    self._build_hardware_config_data = False

                elif self._build_head_config_data:
                    self._head_config_dict = VelptAbDataParticle.\
                        generate_head_config_dict(self._current_record)
                    self._head_config_dict_generated = True
                    self._build_head_config_data = False

                elif self._build_user_config_data:
                    self._user_config_dict = VelptAbDataParticle.\
                        generate_user_config_dict(self._current_record)
                    self._user_config_dict_generated = True
                    self._build_user_config_data = False
                    self._build_config_metadata = True

        log.debug('File has been completely processed')
