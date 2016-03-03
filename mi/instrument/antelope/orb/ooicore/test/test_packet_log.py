import mock

from io import BytesIO
from unittest import TestCase
from nose.plugins.attrib import attr
from mi.instrument.antelope.orb.ooicore.packet_log import PacketLogHeader, PacketLog, GapException
from collections import namedtuple

from mi.core.log import get_logger
log = get_logger()

__author__ = 'petercable'

HeaderTuple = namedtuple('HeaderTuple', 'net, location, station, channel, starttime, maxtime, rate, calib, calper refdes')
header_values = HeaderTuple('OO', 'XX', 'AXAS1', 'EHE', 1.0, 100.0, 200.0, 1.0, 0.0, 'refdes')

PacketTuple = namedtuple('PacketTuple',
                         'net, loc, sta, chan, time, rate, calib, calper, nsamp, data')
packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 1.0, 200.0, 1.0, 0.0, 5, [1, 2, 3, 4, 5])
gap_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 50.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
early_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 0.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
late_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 100.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
overlapping_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 99.90, 200.0, 1.0, 0.0, 200, [5] * 200)


@attr('UNIT_ANTELOPE', group='mi')
class PacketLogUnitTest(TestCase):
    def test_header_create(self):
        header = PacketLogHeader(*header_values)

    def test_header_properties(self):
        header = PacketLogHeader(*header_values)

        self.assertEqual(header.time, '1970-01-01T00:00:01.000000')
        self.assertEqual(header.delta, 1.0 / header_values.rate)
        self.assertEqual(header.name, 'OO-AXAS1-XX-EHE')
        self.assertEqual(header.endtime, header.starttime)
        self.assertEqual(header.fname, 'OO-AXAS1-XX-EHE-1970-01-01T00:00:01.000000.mseed')

        # add some samples, verify the endtime advances
        header.num_samples = 200
        self.assertEqual(header.endtime, header.starttime + 1)

    def test_log_create(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

    def test_log_properties(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        PacketLog.base_dir = './antelope_data'
        log.create(*header_values)

        self.assertEqual(log.absname, './antelope_data/refdes/1970/01/01/'
                                      'OO-AXAS1-XX-EHE-1970-01-01T00:00:01.000000.mseed')

    def test_log_add_packet(self):
        packet_log = PacketLog()
        packet_log.filehandle = BytesIO()
        packet_log.create(*header_values)

        packet_log.add_packet(packet_values._asdict())

        self.assertEqual(list(packet_log.data.get()), packet_values.data)

    def test_log_flush(self):
        # here we'll mock the methods that actually write to disk
        # so we can test the flush interface without creating any files
        trace_write = 'obspy.core.trace.Trace.write'

        with mock.patch(trace_write, new_callable=mock.Mock) as mocked_write:
            packet_log = PacketLog()
            packet_log.filehandle = BytesIO()
            packet_log.create(*header_values)

            packet_log.add_packet(packet_values._asdict())
            # assert the record was updated
            self.assertEqual(packet_log.header.num_samples, 5)

            packet_log.flush()

            # assert Trace.write was called
            mocked_write.assert_called_once_with(packet_log.absname, format='MSEED')

    def test_packet_gap_exception(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        log.add_packet(packet_values._asdict())

        with self.assertRaises(GapException):
            log.add_packet(gap_packet_values._asdict())

    def test_packet_range_exceptions(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        log.add_packet(packet_values._asdict())

        with self.assertRaises(GapException):
            log.add_packet(early_packet_values._asdict())

        with self.assertRaises(GapException):
            log.add_packet(late_packet_values._asdict())

    def test_packet_overlap(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        log.add_packet(packet_values._asdict())

        self.assertEqual(log.header.num_samples, 5)

        # fudge our num_samples value to avoid gap exception
        log.header.num_samples += 19775

        # add our overlapping data
        packet = log.add_packet(overlapping_packet_values._asdict())

        # assert that a packet containing 180 samples with a starttime of 100.0 is returned
        self.assertEqual(packet['nsamp'], 180)
        self.assertEqual(packet['time'], 100.0)

        # assert our container is full
        self.assertEqual(log.header.num_samples, 200 * 99)
        self.assertEqual(log.header.endtime, 100.0)
