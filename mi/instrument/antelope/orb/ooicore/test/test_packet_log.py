import mock
import numpy as np

from ctypes import sizeof
from io import BytesIO
from unittest import TestCase
from nose.plugins.attrib import attr
from ..packet_log import PacketLogHeader, PacketLog, GapException, TimeRangeException
from collections import namedtuple

__author__ = 'petercable'

HeaderTuple = namedtuple('HeaderTuple', 'net, location, station, channel, starttime, maxtime, rate, calib, calper')
header_values = HeaderTuple('OO', 'XX', 'AXAS1', 'EHE', 1.0, 100.0, 200.0, 1.0, 0.0)

PacketTuple = namedtuple('PacketTuple',
                         'net, location, station, channel, starttime, sampling_rate, calib, calper, npts, data')
packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 1.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
gap_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 50.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
early_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 0.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
late_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 100.0, 200.0, 1.0, 0.0, 5, (1, 2, 3, 4, 5))
overlapping_packet_values = PacketTuple('OO', 'XX', 'AXAS1', 'EHE', 99.90, 200.0, 1.0, 0.0, 200, [5] * 200)


@attr('UNIT', group='mi')
class PacketLogUnitTest(TestCase):
    def test_header(self):
        header = PacketLogHeader(*header_values)

        for field in header_values._fields:
            self.assertEqual(getattr(header, field), getattr(header_values, field))

        # dump to bytes, then restore
        bb = BytesIO()
        bb.write(header)
        bb.seek(0)

        header = PacketLogHeader()
        bb.readinto(header)

        for field in header_values._fields:
            self.assertEqual(getattr(header, field), getattr(header_values, field))

    def test_header_properties(self):
        header = PacketLogHeader(*header_values)

        self.assertEqual(header.time, '1970-01-01T00:00:01.000000')
        self.assertEqual(header.delta, 1.0 / header_values.rate)
        self.assertEqual(header.name, 'OO.XX.AXAS1.EHE')
        self.assertEqual(header.endtime, header.starttime)

        # add some samples, verify the endtime advances
        header.num_samples = 200
        self.assertEqual(header.endtime, header.starttime + 1)

    def test_log_create(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        # verify we wrote the header to the backing store
        log.filehandle.seek(0)
        header_bytes = log.filehandle.read(sizeof(log.header))
        self.assertEqual(header_bytes, memoryview(log.header).tobytes())

        # verify the values are correct
        for field in header_values._fields:
            self.assertEqual(getattr(log.header, field), getattr(header_values, field))

    def test_log_from_filehandle(self):
        header = PacketLogHeader(*header_values)
        filehandle = BytesIO()
        filehandle.write(header)

        log = PacketLog.from_file(filehandle)

        # verify the values are correct
        for field in header_values._fields:
            self.assertEqual(getattr(log.header, field), getattr(header_values, field))

    def test_log_properties(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        self.assertEqual(log.filename, 'OO.XX.AXAS1.EHE.1970-01-01T00:00:01.000000')
        self.assertEqual(log.data_start, sizeof(log.header))

    def test_log_add_packet(self):
        log = PacketLog()
        log.filehandle = BytesIO()
        log.create(*header_values)

        log.add_packet(packet_values._asdict())

        # we can't just call log.data, since numpy uses underlying c code to read from the open
        # filehandle. Instead we'll read in the data ourselves.
        log.filehandle.seek(log.data_start)
        data = np.fromstring(log.filehandle.read(), dtype='int32')

        self.assertEqual(tuple(data), packet_values.data)

    def test_log_flush(self):
        # here we'll mock the methods that actually write to disk
        # so we can test the flush interface without creating any files
        packetlog_data = 'mi.instrument.antelope.orb.ooicore.packet_log.PacketLog.data'
        trace_write = 'obspy.core.trace.Trace.write'

        with mock.patch(packetlog_data, new_callable=mock.PropertyMock) as mocked_data, \
                mock.patch(trace_write, new_callable=mock.Mock) as mocked_write:
            mocked_data.return_value = np.array(packet_values.data, dtype='int32')
            log = PacketLog()
            log.filehandle = BytesIO()
            log.create(*header_values)

            log.add_packet(packet_values._asdict())

            header = PacketLogHeader()
            log.filehandle.seek(0)
            log.filehandle.readinto(header)

            # we haven't flushed, assert that the data on disk hasn't changed
            self.assertEqual(header.num_samples, 0)

            log.flush()

            log.filehandle.seek(0)
            log.filehandle.readinto(header)

            # post flush, assert the record was updated
            self.assertEqual(header.num_samples, 5)

            # assert Trace.write was called
            mocked_write.assert_called_once()

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

        with self.assertRaises(TimeRangeException):
            log.add_packet(early_packet_values._asdict())

        with self.assertRaises(TimeRangeException):
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
        self.assertEqual(packet['npts'], 180)
        self.assertEqual(packet['starttime'], 100.0)

        # assert our container is full
        self.assertEqual(log.header.num_samples, 200 * 99)
        self.assertEqual(log.header.endtime, 100.0)
