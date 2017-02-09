#!/usr/bin/env python

"""
@package mi.dataset.parser.adcps_jln_sio
@file mi/dataset/parser/adcps_jln_sio.py
@author Emily Hahn
@brief An adcps jln series through the sio dataset agent parser
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import re
import struct
import binascii

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.sio_mule_common import SioParser, SIO_HEADER_MATCHER, SIO_HEADER_GROUP_ID, \
    SIO_HEADER_GROUP_TIMESTAMP

from mi.dataset.parser import utilities

from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException, UnexpectedDataException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey, DataParticleValue


class DataParticleType(BaseEnum):
    SAMPLE = 'adcps_jln_sio_mule_instrument'


class AdcpsJlnSioDataParticleKey(BaseEnum):
    CONTROLLER_TIMESTAMP = 'sio_controller_timestamp'
    ENSEMBLE_NUMBER = 'ensemble_number'
    UNIT_ID = 'unit_id'
    FIRMWARE_VERSION = 'firmware_version'
    FIRMWARE_REVISION = 'firmware_revision'
    ADCPS_JLN_YEAR = 'adcps_jln_year'
    ADCPS_JLN_MONTH = 'adcps_jln_month'
    ADCPS_JLN_DAY = 'adcps_jln_day'
    ADCPS_JLN_HOUR = 'adcps_jln_hour'
    ADCPS_JLN_MINUTE = 'adcps_jln_minute'
    ADCPS_JLN_SECOND = 'adcps_jln_second'
    ADCPS_JLN_HSEC = 'adcps_jln_hsec'
    ADCPS_JLN_HEADING = 'adcps_jln_heading'
    ADCPS_JLN_PITCH = 'adcps_jln_pitch'
    ADCPS_JLN_ROLL = 'adcps_jln_roll'
    ADCPS_JLN_TEMP = 'adcps_jln_temp'
    ADCPS_JLN_PRESSURE = 'adcps_jln_pressure'
    VELOCITY_PO_ERROR_FLAG = 'velocity_po_error_flag'
    VELOCITY_PO_UP_FLAG = 'velocity_po_up_flag'
    VELOCITY_PO_NORTH_FLAG = 'velocity_po_north_flag'
    VELOCITY_PO_EAST_FLAG = 'velocity_po_east_flag'
    SUBSAMPLING_PARAMETER = 'subsampling_parameter'
    ADCPS_JLN_STARTBIN = 'adcps_jln_startbin'
    ADCPS_JLN_BINS = 'adcps_jln_bins'
    ADCPS_JLN_ERR = 'error_velocity'
    ADCPS_JLN_UP = 'water_velocity_up'
    ADCPS_JLN_NORTH = 'water_velocity_north'
    ADCPS_JLN_EAST = 'water_velocity_east'

DATA_WRAPPER_REGEX = b'<Executing/>\x0d\x0a<SampleData ID=\'0x[0-9a-f]+\' LEN=\'[0-9]+\' ' \
                     'CRC=\'(0x[0-9a-f]+)\'>([\x00-\xFF]+)</SampleData>\x0d\x0a<Executed/>\x0d\x0a'
DATA_WRAPPER_MATCHER = re.compile(DATA_WRAPPER_REGEX)

DATA_FAIL_REGEX = b'<E[rR][rR][oO][rR] type=(.+) msg=(.+)/>\x0d\x0a'
DATA_FAIL_MATCHER = re.compile(DATA_FAIL_REGEX)

DATA_REGEX = b'\x6e\x7f[\x00-\xFF]{32}([\x00-\xFF]+)([\x00-\xFF]{2})'
DATA_MATCHER = re.compile(DATA_REGEX)

SIO_HEADER_BYTES = 33
STARTING_BYTES = 34

CRC_TABLE = [0, 1996959894, 3993919788, 2567524794, 124634137, 1886057615, 3915621685, 2657392035,
             249268274, 2044508324, 3772115230, 2547177864, 162941995, 2125561021, 3887607047, 2428444049,
             498536548, 1789927666, 4089016648, 2227061214, 450548861, 1843258603, 4107580753, 2211677639,
             325883990, 1684777152, 4251122042, 2321926636, 335633487, 1661365465, 4195302755, 2366115317,
             997073096, 1281953886, 3579855332, 2724688242, 1006888145, 1258607687, 3524101629, 2768942443,
             901097722, 1119000684, 3686517206, 2898065728, 853044451, 1172266101, 3705015759, 2882616665,
             651767980, 1373503546, 3369554304, 3218104598, 565507253, 1454621731, 3485111705, 3099436303,
             671266974, 1594198024, 3322730930, 2970347812, 795835527, 1483230225, 3244367275, 3060149565,
             1994146192, 31158534, 2563907772, 4023717930, 1907459465, 112637215, 2680153253, 3904427059,
             2013776290, 251722036, 2517215374, 3775830040, 2137656763, 141376813, 2439277719, 3865271297,
             1802195444, 476864866, 2238001368, 4066508878, 1812370925, 453092731, 2181625025, 4111451223,
             1706088902, 314042704, 2344532202, 4240017532, 1658658271, 366619977, 2362670323, 4224994405,
             1303535960, 984961486, 2747007092, 3569037538, 1256170817, 1037604311, 2765210733, 3554079995,
             1131014506, 879679996, 2909243462, 3663771856, 1141124467, 855842277, 2852801631, 3708648649,
             1342533948, 654459306, 3188396048, 3373015174, 1466479909, 544179635, 3110523913, 3462522015,
             1591671054, 702138776, 2966460450, 3352799412, 1504918807, 783551873, 3082640443, 3233442989,
             3988292384, 2596254646, 62317068, 1957810842, 3939845945, 2647816111, 81470997, 1943803523,
             3814918930, 2489596804, 225274430, 2053790376, 3826175755, 2466906013, 167816743, 2097651377,
             4027552580, 2265490386, 503444072, 1762050814, 4150417245, 2154129355, 426522225, 1852507879,
             4275313526, 2312317920, 282753626, 1742555852, 4189708143, 2394877945, 397917763, 1622183637,
             3604390888, 2714866558, 953729732, 1340076626, 3518719985, 2797360999, 1068828381, 1219638859,
             3624741850, 2936675148, 906185462, 1090812512, 3747672003, 2825379669, 829329135, 1181335161,
             3412177804, 3160834842, 628085408, 1382605366, 3423369109, 3138078467, 570562233, 1426400815,
             3317316542, 2998733608, 733239954, 1555261956, 3268935591, 3050360625, 752459403, 1541320221,
             2607071920, 3965973030, 1969922972, 40735498, 2617837225, 3943577151, 1913087877, 83908371,
             2512341634, 3803740692, 2075208622, 213261112, 2463272603, 3855990285, 2094854071, 198958881,
             2262029012, 4057260610, 1759359992, 534414190, 2176718541, 4139329115, 1873836001, 414664567,
             2282248934, 4279200368, 1711684554, 285281116, 2405801727, 4167216745, 1634467795, 376229701,
             2685067896, 3608007406, 1308918612, 956543938, 2808555105, 3495958263, 1231636301, 1047427035,
             2932959818, 3654703836, 1088359270, 936918000, 2847714899, 3736837829, 1202900863, 817233897,
             3183342108, 3401237130, 1404277552, 615818150, 3134207493, 3453421203, 1423857449, 601450431,
             3009837614, 3294710456, 1567103746, 711928724, 3020668471, 3272380065, 1510334235, 755167117]


class AdcpsJlnSioDataParticle(DataParticle):
    """
    Class for parsing data from the ADCPS instrument on a MSFM platform node
    """

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):

        super(AdcpsJlnSioDataParticle, self).__init__(raw_data,
                                                      port_timestamp,
                                                      internal_timestamp,
                                                      preferred_timestamp,
                                                      quality_flag,
                                                      new_sequence)

        self._data_match = DATA_MATCHER.match(self.raw_data[8:])

        if not self._data_match:

            raise RecoverableSampleException("AdcpsJlnSioParserDataParticle: No regex match of "
                                             "parsed sample data [%s]" % self.raw_data[8:])

        date_str = self.unpack_date(self._data_match.group(0)[11:19])

        unix_time = utilities.zulu_timestamp_to_utc_time(date_str)

        self.set_internal_timestamp(unix_time=unix_time)

    def _build_parsed_values(self):
        """
        Take something in the binary data values and turn it into a
        particle with the appropriate tag.
        throws SampleException If there is a problem with sample creation
        """

        # raw data includes the sio controller timestamp at the start
        # match the data inside the wrapper

        result = []

        if self._data_match:
            match = self._data_match

            try:
                fields = struct.unpack('<HHIBBBdHhhhIbBB', match.group(0)[0:STARTING_BYTES])
                num_bytes = fields[1]

                if len(match.group(0)) - 2 != num_bytes:
                    raise ValueError('num bytes %d does not match data length %d'
                                     % (num_bytes, len(match.group(0))))
                nbins = fields[14]

                if len(match.group(0)) < (36+(nbins*8)):
                    raise ValueError('Number of bins %d does not fit in data length %d' % (nbins,
                                                                                           len(match.group(0))))

                date_fields = struct.unpack('HBBBBBB', match.group(0)[11:19])

                velocity_data = struct.unpack_from('<%dh' % (nbins * 4),
                                              match.group(0)[STARTING_BYTES:])

                vel_east = velocity_data[:nbins]
                vel_north = velocity_data[nbins:nbins*2]
                vel_up = velocity_data[nbins*2:nbins*3]
                vel_err = velocity_data[nbins*3:]

                CHECKSUM_INDEX = STARTING_BYTES + nbins * 8
                checksum = struct.unpack_from('<H', match.group(0)[CHECKSUM_INDEX:])
                calculated_checksum = AdcpsJlnSioDataParticle.calc_inner_checksum(match.group(0)[:-2])

                if checksum[0] != calculated_checksum:
                    raise ValueError("Inner checksum %s does not match %s" % (checksum[0], calculated_checksum))

            except (ValueError, TypeError, IndexError) as ex:
                # we can recover and read additional samples after this, just this one is missed
                log.warn("Error %s while decoding parameters in data [%s]", ex, match.group(0))
                raise RecoverableSampleException("Error (%s) while decoding parameters in data: [%s]" %
                                                (ex, match.group(0)))

            result = [self._encode_value(AdcpsJlnSioDataParticleKey.CONTROLLER_TIMESTAMP, self.raw_data[0:8],
                                         AdcpsJlnSioDataParticle.encode_int_16),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ENSEMBLE_NUMBER, fields[2], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.UNIT_ID, fields[3], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.FIRMWARE_VERSION, fields[4], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.FIRMWARE_REVISION, fields[5], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_YEAR, date_fields[0], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_MONTH, date_fields[1], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_DAY, date_fields[2], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_HOUR, date_fields[3], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_MINUTE, date_fields[4], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_SECOND, date_fields[5], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_HSEC, date_fields[6], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_HEADING, fields[7], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_PITCH, fields[8], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_ROLL, fields[9], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_TEMP, fields[10], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_PRESSURE, fields[11], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.VELOCITY_PO_ERROR_FLAG, fields[12] & 1, int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.VELOCITY_PO_UP_FLAG, (fields[12] & 2) >> 1, int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.VELOCITY_PO_NORTH_FLAG, (fields[12] & 4) >> 2, int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.VELOCITY_PO_EAST_FLAG, (fields[12] & 8) >> 3, int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.SUBSAMPLING_PARAMETER, (fields[12] & 240) >> 4, int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_STARTBIN, fields[13], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_BINS, fields[14], int),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_ERR, vel_err, list),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_UP, vel_up, list),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_NORTH, vel_north, list),
                      self._encode_value(AdcpsJlnSioDataParticleKey.ADCPS_JLN_EAST, vel_east, list)]

        log.trace('AdcpsParserDataParticle: particle=%s', result)

        return result

    @staticmethod
    def unpack_date(data):
        fields = struct.unpack('HBBBBBB', data)
        #log.debug('Unpacked data into date fields %s', fields)
        zulu_ts = "%04d-%02d-%02dT%02d:%02d:%02d.%02dZ" % (
            fields[0], fields[1], fields[2], fields[3],
            fields[4], fields[5], fields[6])
        return zulu_ts

    @staticmethod
    def encode_int_16(hex_str):
        return int(hex_str, 16)

    @staticmethod
    def calc_inner_checksum(data_block):
        """
        calculate the checksum on the adcps data block, which occurs at the end of the data block
        """
        crc = 0
        # sum all bytes and take last 2 bytes
        for i in range(0, len(data_block)):
            val = struct.unpack('<B', data_block[i])
            crc += int(val[0])
            # values are "unsigned short", wrap around if we go outside
            if crc < 0:
                crc += 65536
            elif crc > 65536:
                crc -= 65536
        return crc


class AdcpsJlnSioParser(SioParser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,
                 *args, **kwargs):

        super(AdcpsJlnSioParser, self).__init__(config,
                                                stream_handle,
                                                exception_callback,
                                                *args,
                                                **kwargs)

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """

        result_particles = []
        (timestamp, chunk) = self._chunker.get_next_data()

        while chunk is not None:

            header_match = SIO_HEADER_MATCHER.match(chunk)

            if header_match.group(SIO_HEADER_GROUP_ID) == 'AD':
                log.debug("matched chunk header %s", chunk[1:32])

                # start search and match after header
                data_fail_match = DATA_FAIL_MATCHER.search(chunk[SIO_HEADER_BYTES:])
                data_wrapper_match = DATA_WRAPPER_MATCHER.match(chunk[SIO_HEADER_BYTES:])

                if data_fail_match:
                    # this ignores any invalid sample data prior to the error tag
                    msg = "Found adcps error type %s exception %s" % (data_fail_match.group(1),
                          data_fail_match.group(2))
                    log.warn(msg)
                    self._exception_callback(RecoverableSampleException(msg))

                elif data_wrapper_match:

                    calculated_xml_checksum = AdcpsJlnSioParser.calc_xml_checksum(data_wrapper_match.group(2))
                    xml_checksum = int(data_wrapper_match.group(1), 16)

                    if calculated_xml_checksum == xml_checksum:

                        data_match = DATA_MATCHER.search(data_wrapper_match.group(2))

                        if data_match:

                            log.debug('Found data match in chunk %s', chunk[1:32])

                            # particle-ize the data block received, return the record
                            sample = self._extract_sample(AdcpsJlnSioDataParticle, None,
                                                          header_match.group(SIO_HEADER_GROUP_TIMESTAMP) +
                                                          data_match.group(0),
                                                          None)
                            if sample:
                                # create particle
                                result_particles.append(sample)

                        else:
                            msg = "Matched adcps xml wrapper but not inside data %s" % \
                                  binascii.hexlify(data_wrapper_match.group(0))
                            log.warn(msg)
                            self._exception_callback(RecoverableSampleException(msg))
                    else:
                        msg = "Xml checksum %s does not match calculated %s" % (xml_checksum, calculated_xml_checksum)
                        log.warn(msg)
                        self._exception_callback(RecoverableSampleException(msg))

                else:
                    msg = "Unexpected data found within header %s: 0x%s" % (chunk[1:32], binascii.hexlify(chunk))
                    log.warning(msg)
                    self._exception_callback(UnexpectedDataException(msg))

            (timestamp, chunk) = self._chunker.get_next_data()

        return result_particles

    @staticmethod
    def calc_xml_checksum(data_block):
        """
        calculate the checksum to compare to the xml wrapper around adcps block of data
        """

        # corresponds to 0xFFFFFFFF
        crc = 4294967295

        for i in range(0, len(data_block)):
            val = struct.unpack('<b', data_block[i])
            table_idx = (crc ^ int(val[0])) & 255
            crc = CRC_TABLE[table_idx] ^ (crc >> 8)
        return crc
