#!/usr/bin/env python

"""
@package mi.dataset.parser.nutnr_b_dcl_full
@file mi/dataset/parser/nutnr_b_dcl_full.py
@author Mark Worden
@brief Parser for the nutnr_b_dcl_full dataset driver

This file contains code for the nutnr_b_dcl_full parsers and
code to produce data particles.  For telemetered data, there
is one parser which produces three types of data particles.  For
recovered data, there is one parser which produces three types
of data particles.  Both parsers produce light and dark frame instrument, and metadata
data particles.  There is 1 metadata data particle produced for
each data block in a file.  There may be 1 or more data blocks
in a file.  There is 1 instrument data particle produced for
each record in a file.  The input files and the content of the
data particles are the same for both recovered and telemetered.
Only the names of the output particle streams are different.

Input files are ASCII with variable length records.
"""

__author__ = 'mworden'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import UnexpectedDataException, SampleException

from mi.dataset.parser.nutnr_b_dcl_parser_base import NutnrBDclParser

from mi.dataset.parser.nutnr_b_dcl_parser_base import \
    InstrumentDataMatchGroups, \
    INST_FULL_DATA_MATCHER, \
    IDLE_TIME_MATCHER, \
    NEXT_WAKEUP_MATCHER, \
    META_MESSAGE_MATCHER, \
    NUTR_B_DCL_IGNORE_MATCHER, \
    FULL_FRAME_TYPES, \
    NITRATE_LIGHT_FULL, \
    NITRATE_DARK_FULL

from mi.dataset.parser.nutnr_b_particles import \
    NutnrBDclFullRecoveredInstrumentDataParticle, \
    NutnrBDclDarkFullRecoveredInstrumentDataParticle, \
    NutnrBDclFullTelemeteredMetadataDataParticle, \
    NutnrBDclDarkFullTelemeteredInstrumentDataParticle, \
    NutnrBDclFullRecoveredMetadataDataParticle, \
    NutnrBDclFullTelemeteredInstrumentDataParticle, \
    NutnrBDataParticleKey


class NutnrBDclFullParser(NutnrBDclParser):
    """
    Parser for Nutnr_b_dcl_full data.
    In addition to the standard parser constructor parameters,
    this constructor needs the following additional parameters:
      instrument particle class for light frame
      instrument particle class for dark frame
      metadata particle class.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 instrument_particle_class,
                 dark_instrument_particle_class,
                 metadata_particle_class):

        super(NutnrBDclFullParser, self).__init__(config,
                                                  stream_handle,
                                                  state_callback,
                                                  publish_callback,
                                                  exception_callback,
                                                  instrument_particle_class,
                                                  dark_instrument_particle_class,
                                                  metadata_particle_class,
                                                  FULL_FRAME_TYPES)

    def _create_instrument_particle(self, inst_match):
        """
        This method will create a nutnr_b_dcl_full instrument particle given
        instrument match data found from parsing an input file.
        """

        # Obtain the ntp timestamp
        ntp_timestamp = self._extract_instrument_ntp_timestamp(inst_match)

        frame_type = inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_TYPE)

        # need to determine if this is a light or dark frame
        # spectral channel parameter and stream names are
        # different for each
        if frame_type == NITRATE_LIGHT_FULL:
            particle_class = self._instrument_particle_class
            spectral_key = NutnrBDataParticleKey.SPECTRAL_CHANNELS
        elif frame_type == NITRATE_DARK_FULL:
            particle_class = self._dark_instrument_particle_class
            spectral_key = NutnrBDataParticleKey.DARK_FRAME_SPECTRAL_CHANNELS

        else:  # this should never happen but just in case
            message = "invalid frame type passed to particle"
            log.error(message)
            raise SampleException(message)

        # Create the instrument data list of tuples from the instrument match data
        instrument_data_tuple = [
            (NutnrBDataParticleKey.DCL_CONTROLLER_TIMESTAMP,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_DCL_TIMESTAMP),
             str),
            (NutnrBDataParticleKey.FRAME_HEADER,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FRAME_HEADER),
             str),
            (NutnrBDataParticleKey.FRAME_TYPE,
             frame_type,
             str),
            (NutnrBDataParticleKey.SERIAL_NUMBER,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_SERIAL_NUMBER),
             str),
            (NutnrBDataParticleKey.DATE_OF_SAMPLE,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_JULIAN_DATE),
             int),
            (NutnrBDataParticleKey.TIME_OF_SAMPLE,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_TIME_OF_DAY),
             float),
            (NutnrBDataParticleKey.NITRATE_CONCENTRATION,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_NITRATE),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_1,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FITTING1),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_2,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FITTING2),
             float),
            (NutnrBDataParticleKey.AUX_FITTING_3,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_FITTING3),
             float),
            (NutnrBDataParticleKey.RMS_ERROR,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_RMS_ERROR),
             float),
            (NutnrBDataParticleKey.TEMP_INTERIOR,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_INTERIOR),
             float),
            (NutnrBDataParticleKey.TEMP_SPECTROMETER,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_SPECTROMETER),
             float),
            (NutnrBDataParticleKey.TEMP_LAMP,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_TEMP_LAMP),
             float),
            (NutnrBDataParticleKey.LAMP_TIME,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_LAMP_TIME),
             int),
            (NutnrBDataParticleKey.HUMIDITY,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_HUMIDITY),
             float),
            (NutnrBDataParticleKey.VOLTAGE_LAMP,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_LAMP),
             float),
            (NutnrBDataParticleKey.VOLTAGE_ANALOG,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_ANALOG),
             float),
            (NutnrBDataParticleKey.VOLTAGE_MAIN,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_VOLTAGE_MAIN),
             float),
            (NutnrBDataParticleKey.REF_CHANNEL_AVERAGE,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_REF_CHANNEL_AVERAGE),
             float),
            (NutnrBDataParticleKey.REF_CHANNEL_VARIANCE,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_REF_CHANNEL_VARIANCE),
             float),
            (NutnrBDataParticleKey.SEA_WATER_DARK,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_SEA_WATER_DARK),
             float),
            (NutnrBDataParticleKey.SPEC_CHANNEL_AVERAGE,
             inst_match.group(InstrumentDataMatchGroups.INST_GROUP_SPEC_CHANNEL_AVERAGE),
             float),
            (spectral_key,
             map(int, inst_match.group(InstrumentDataMatchGroups.INST_GROUP_SPECTRAL_CHANNELS).split(',')),
             list),
        ]

        # Extract the instrument particle sample providing the instrument data
        # tuple and ntp timestamp
        particle = self._extract_sample(particle_class,
                                        None,
                                        instrument_data_tuple,
                                        ntp_timestamp)

        return particle

    def parse_file(self):
        """
        This method will parse a nutnr_b_dcl_full input file and collect the
        particles.
        """

        # Read the first line in the file
        line = self._stream_handle.readline()

        # While a new line in the file exists
        while line:

            # Attempt to create a match for each possible line that should
            # exist in the file
            idle_match = IDLE_TIME_MATCHER.match(line)
            next_wakeup_match = NEXT_WAKEUP_MATCHER.match(line)
            meta_match = META_MESSAGE_MATCHER.match(line)
            inst_match = INST_FULL_DATA_MATCHER.match(line)
            ignore_match = NUTR_B_DCL_IGNORE_MATCHER.match(line)

            # Let's first check to see if we have an ignore match
            if ignore_match is not None:

                log.debug("Found ignore match - line: %s", line)

            # Did the line match an idle line?
            elif idle_match is not None:

                log.debug("Found idle match: %s", line)

                # Process the idle state metadata match
                self._process_idle_metadata_record(idle_match)

            # Did the line match a next wakeup record?
            elif next_wakeup_match is not None:

                log.debug("Found next wakeup match: %s", line)

                self._process_next_wakeup_match()

            # Did the line match one of the possible metadata possibilities?
            elif meta_match is not None:

                log.debug("Found potential metadata part match: %s", line)

                # Process the metadata record match
                self._process_metadata_record_part(line)

            # Did the line match one of the possible instrument lines?
            elif inst_match is not None:

                log.debug("Found potential instrument match: %s", line)

                # process the instrument record match
                self._process_instrument_record_match(inst_match)

            else:
                # We found a line in the file that was unexpected.  Report a
                # RecoverableSampleException
                message = "Unexpected data in file, line: " + line
                log.warn(message)
                self._exception_callback(UnexpectedDataException(message))

            # Read the next line in the file
            line = self._stream_handle.readline()

        # Set an indication that the file was fully parsed
        self._file_parsed = True


class NutnrBDclFullRecoveredParser(NutnrBDclFullParser):
    """
    This is the recovered version of the nutnr_b_dcl_full parser which provides
    the NutnrBDclFullRecoveredInstrumentDataParticle, NutnrBDclDarkFullRecoveredInstrumentDataParticle and
    NutnrBDclFullRecoveredMetadataDataParticle particles to the super class's
    constructor
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback):

        super(NutnrBDclFullRecoveredParser, self).__init__(config,
                                                           stream_handle,
                                                           state_callback,
                                                           publish_callback,
                                                           exception_callback,
                                                           NutnrBDclFullRecoveredInstrumentDataParticle,
                                                           NutnrBDclDarkFullRecoveredInstrumentDataParticle,
                                                           NutnrBDclFullRecoveredMetadataDataParticle)


class NutnrBDclFullTelemeteredParser(NutnrBDclFullParser):
    """
    This is the recovered version of the nutnr_b_dcl_full parser which provides
    the NutnrBDclFullTelemeteredInstrumentDataParticle, NutnrBDclDarkFullTelemeteredInstrumentDataParticle and
    NutnrBDclFullTelemeteredMetadataDataParticle particles to the super class's
    constructor
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback):

        super(NutnrBDclFullTelemeteredParser, self).__init__(config,
                                                             stream_handle,
                                                             state_callback,
                                                             publish_callback,
                                                             exception_callback,
                                                             NutnrBDclFullTelemeteredInstrumentDataParticle,
                                                             NutnrBDclDarkFullTelemeteredInstrumentDataParticle,
                                                             NutnrBDclFullTelemeteredMetadataDataParticle)
