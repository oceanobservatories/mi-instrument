#!/usr/bin/env python

"""
@package mi.dataset.parser.mopak_o_dcl
@file marine-integrations/mi/dataset/parser/mopak_o_dcl.py
@author Emily Hahn
@brief Parser for the mopak_o_dcl dataset driver
Release notes:

initial release
"""

import ntplib
import struct


from mi.core.log import get_logger

from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import DataParticle
from mi.core.exceptions import \
    SampleException, \
    ConfigurationException

from mi.dataset.dataset_parser import SimpleParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser import utilities

log = get_logger()

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

ACCEL_ID = b'\xCB'
RATE_ID = b'\xCF'
ACCEL_BYTES = 43
RATE_BYTES = 31

MAX_TIMER = 4294967296
TIMER_TO_SECONDS = 62500.0
TIMER_DIFF_FACTOR = 2.1


class StateKey(BaseEnum):
    POSITION = 'position'
    TIMER_ROLLOVER = 'timer_rollover'
    TIMER_START = 'timer_start'


class MopakParticleClassType(BaseEnum):
    ACCEL_PARTICLE_CLASS = 'accel_particle_class'
    RATE_PARTICLE_CLASS = 'rate_particle_class'


class MopakDataParticleType(BaseEnum):
    ACCEL_TELEM = 'mopak_o_dcl_accel'
    RATE_TELEM = 'mopak_o_dcl_rate'
    ACCEL_RECOV = 'mopak_o_dcl_accel_recovered'
    RATE_RECOV = 'mopak_o_dcl_rate_recovered'


class MopakODclAccelParserDataParticleKey(BaseEnum):
    MOPAK_ACCELX = 'mopak_accelx'
    MOPAK_ACCELY = 'mopak_accely'
    MOPAK_ACCELZ = 'mopak_accelz'
    MOPAK_ANG_RATEX = 'mopak_ang_ratex'
    MOPAK_ANG_RATEY = 'mopak_ang_ratey'
    MOPAK_ANG_RATEZ = 'mopak_ang_ratez'
    MOPAK_MAGX = 'mopak_magx'
    MOPAK_MAGY = 'mopak_magy'
    MOPAK_MAGZ = 'mopak_magz'
    MOPAK_TIMER = 'mopak_timer'


class MopakODclAccelAbstractDataParticle(DataParticle):
    """
    Abstract Class for parsing data from the Mopak_o_stc data set
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        # match the data inside the wrapper
        if len(self.raw_data) < ACCEL_BYTES or self.raw_data[0] != ACCEL_ID:
            raise SampleException("MopakODclAccelParserDataParticle: Not enough bytes provided in [%s]",
                                  self.raw_data)
        fields = struct.unpack('>fffffffffI', self.raw_data[1:ACCEL_BYTES - 2])

        result = [self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ACCELX, fields[0], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ACCELY, fields[1], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ACCELZ, fields[2], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ANG_RATEX, fields[3], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ANG_RATEY, fields[4], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_ANG_RATEZ, fields[5], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_MAGX, fields[6], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_MAGY, fields[7], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_MAGZ, fields[8], float),
                  self._encode_value(MopakODclAccelParserDataParticleKey.MOPAK_TIMER, fields[9], int)]

        return result


class MopakODclAccelParserDataParticle(MopakODclAccelAbstractDataParticle):
    """
    Class for parsing data from the Mopak_o_stc data set
    """

    _data_particle_type = MopakDataParticleType.ACCEL_TELEM


class MopakODclAccelParserRecoveredDataParticle(MopakODclAccelAbstractDataParticle):
    """
    Class for parsing data from the Mopak_o_stc data set
    """

    _data_particle_type = MopakDataParticleType.ACCEL_RECOV


class MopakODclRateParserDataParticleKey(BaseEnum):
    MOPAK_ROLL = 'mopak_roll'
    MOPAK_PITCH = 'mopak_pitch'
    MOPAK_YAW = 'mopak_yaw'
    MOPAK_ANG_RATEX = 'mopak_ang_ratex'
    MOPAK_ANG_RATEY = 'mopak_ang_ratey'
    MOPAK_ANG_RATEZ = 'mopak_ang_ratez'
    MOPAK_TIMER = 'mopak_timer'


class MopakODclRateParserDataAbstractParticle(DataParticle):
    """
    Abstract Class for parsing data from the mopak_o_dcl data set
    """

    _data_particle_type = None

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        # match the data inside the wrapper
        if len(self.raw_data) < RATE_BYTES or self.raw_data[0] != RATE_ID:
            raise SampleException("MopakODclRateParserDataParticle: Not enough bytes provided in [%s]",
                                  self.raw_data)
        fields = struct.unpack('>ffffffI', self.raw_data[1:RATE_BYTES - 2])

        result = [self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_ROLL, fields[0], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_PITCH, fields[1], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_YAW, fields[2], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_ANG_RATEX, fields[3], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_ANG_RATEY, fields[4], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_ANG_RATEZ, fields[5], float),
                  self._encode_value(MopakODclRateParserDataParticleKey.MOPAK_TIMER, fields[6], int)]

        return result


class MopakODclRateParserDataParticle(MopakODclRateParserDataAbstractParticle):
    """
    Class for parsing data from the mopak_o_dcl data set
    """

    _data_particle_type = MopakDataParticleType.RATE_TELEM


class MopakODclRateParserRecoveredDataParticle(MopakODclRateParserDataAbstractParticle):
    """
    Class for parsing data from the mopak_o_dcl data set
    """

    _data_particle_type = MopakDataParticleType.RATE_RECOV


class MopakODclParser(SimpleParser):
    def __init__(self,
                 config,
                 stream_handle,
                 filename,
                 exception_callback):

        self.timer_diff = None

        self._timer_start = None
        self._timer_rollover = 0

        self._start_time_utc = utilities.formatted_timestamp_utc_time(filename[:15],
                                                                      "%Y%m%d_%H%M%S")

        try:
            # Get the particle classes to publish from the configuration
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            self._accel_particle_class = particle_classes_dict.get(MopakParticleClassType.ACCEL_PARTICLE_CLASS)
            self._rate_particle_class = particle_classes_dict.get(MopakParticleClassType.RATE_PARTICLE_CLASS)

        except Exception:
            log.error('Parser configuration missing or incorrect')
            raise ConfigurationException

        super(MopakODclParser, self).__init__(config,
                                              stream_handle,
                                              exception_callback)

    def compare_checksum(self, raw_bytes):
        rcv_chksum = struct.unpack('>H', raw_bytes[-2:])
        calc_chksum = self.calc_checksum(raw_bytes[:-2])
        if rcv_chksum[0] == calc_chksum:
            return True
        log.debug('checksum received %d does not match calculated %d', rcv_chksum[0], calc_chksum)
        return False

    @staticmethod
    def calc_checksum(raw_bytes):
        return sum(bytearray(raw_bytes)) % 65535

    def timer_to_timestamp(self, timer):
        """
        convert a timer value to a ntp formatted timestamp
        :param timer Timer value from data record.
        """
        # if the timer has rolled over, multiply by the maximum value for timer so the time keeps increasing
        rollover_offset = self._timer_rollover * MAX_TIMER
        # make sure the timer starts at 0 for the file by subtracting the first timer
        # divide timer by 62500 to go from counts to seconds
        offset_secs = float(timer + rollover_offset - self._timer_start) / TIMER_TO_SECONDS
        # add in the utc start time
        time_secs = float(self._start_time_utc) + offset_secs
        # convert to ntp64
        return float(ntplib.system_to_ntp_time(time_secs))

    def parse_file(self):

        position = 0
        last_timer = 0
        bad_data = False
        record_type = self._stream_handle.read(1)

        while record_type:  # will be None when EOF is found
            self._stream_handle.seek(position)  # reset file position to beginning of record
            particle = None
            fields = None
            if record_type == ACCEL_ID:
                data = self._stream_handle.read(ACCEL_BYTES)
                if self.compare_checksum(data):
                    particle = self._extract_sample(self._accel_particle_class, None, data, None)
                    fields = struct.unpack('>I', data[37:41])
                    position += ACCEL_BYTES
                else:
                    position += 1
                    log.error("Found accel record whose checksum doesn't match at byte :0x%s", position)
                    self._exception_callback(SampleException(
                        "Found accel record whose checksum doesn't match at byte :0x%s" % position))
                    bad_data = True
            elif record_type == RATE_ID:
                data = self._stream_handle.read(RATE_BYTES)
                if self.compare_checksum(data):
                    # particle-ize the data block received, return the record
                    particle = self._extract_sample(self._rate_particle_class, None, data, None)
                    fields = struct.unpack('>I', data[25:29])
                    position += RATE_BYTES
                else:
                    position += 1
                    log.error("Found rate record whose checksum doesn't match at byte :0x%s", position)
                    self._exception_callback(SampleException(
                        "Found rate record whose checksum doesn't match at byte :0x%s" % position))
                    bad_data = True
            else:
                position += 1
                if not bad_data:  # only need to send this exception once per bad data block
                    log.error("Found unexpected non-data at byte :0x%s", position)
                    self._exception_callback(SampleException("Found unexpected non-data at byte :0x%s" % position))
                    bad_data = True

            # compute timestamp
            if fields:  # will be None if ID or checksum validation failed.
                timer = int(fields[0])
                # store the first timer value so we can subtract it to zero out the count at the
                # start of the file
                if self._timer_start is None:
                    self._timer_start = timer
                # keep track of the timer rolling over or being reset
                if timer < last_timer:
                    # check that the timer was not reset instead of rolling over, there should be
                    # a large difference between the times, give it a little leeway with the 2.1
                    # this is unlikely to happen in the first place, but there is still a risk of
                    # rolling over on the second sample and not having timer_diff calculated yet,
                    # or rolling in the last sample of the file within the fudge factor
                    if self.timer_diff and (last_timer - timer) < (MAX_TIMER - self.timer_diff * TIMER_DIFF_FACTOR):
                        # timer was reset before it got to the end
                        log.warn('Timer was reset, time of particles unknown')
                        # TODO exception callback?
                        raise SampleException('Timer was reset, time of particle now unknown')
                    log.info("Timer has rolled")
                    self._timer_rollover += 1

                timestamp = self.timer_to_timestamp(timer)

                if particle:
                    particle.set_internal_timestamp(timestamp)
                    self._record_buffer.append(particle)
                    bad_data = False

                # use the timer diff to determine if the timer has been reset instead of rolling over
                # at the end
                if last_timer != 0 and self.timer_diff is None:
                    # get an idea of interval used in this file
                    self.timer_diff = timer - last_timer
                last_timer = timer

            self._stream_handle.seek(position)
            record_type = self._stream_handle.read(1)
