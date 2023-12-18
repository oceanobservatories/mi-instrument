import os
import math
import uuid
from datetime import datetime
from decimal import Decimal, getcontext

from obspy.core import Stats
import numpy as np
from obspy import Trace, Stream

from mi.core.log import get_logger
from mi.core.exceptions import InstrumentProtocolException

log = get_logger()
getcontext().prec = 7


class Vector(object):
    def __init__(self, size, dtype, factor=1.25):
        self.dtype = dtype
        self.factor = factor
        self.backing_store = np.zeros(size, dtype=dtype)
        self.length = size
        self.index = 0
        self.indices = []

    def append(self, value):
        self._realloc(self.index+1)
        self.backing_store[self.index] = value
        self.index += 1

    def extend(self, value):
        new_index = self.index + len(value)
        self.indices.append(new_index)
        self._realloc(new_index)
        self.backing_store[self.index:new_index] = value
        self.index = new_index

    def _realloc(self, new_index):
        while new_index >= self.length:
            self.length = int(self.length * self.factor)
            self.backing_store.resize((self.length,))

    def get(self):
        return self.backing_store[:self.index]


class GapException(Exception):
    pass


class PacketLogHeader(object):
    def __init__(self, net, location, station, channel, starttime, mintime, maxtime, rate, calib, calper, refdes):
        self.net = net
        self.location = location
        self.station = station
        self.channel = channel
        self.starttime = starttime
        self.mintime = mintime
        self.maxtime = maxtime
        self.rate = rate
        self.calib = calib
        self.calper = calper
        self.refdes = refdes
        self.num_samples = 0

    @property
    def time(self):
        return self._format_time(self.mintime)

    @staticmethod
    def _format_time(timestamp):
        t = datetime.isoformat(datetime.utcfromtimestamp(timestamp))
        # if the time has no fractional value the iso timestamp won't contain the trailing zeros
        # add them back in if they are missing for consistency in file naming
        if t[-3] == ':':
            t += '.000000'
        t += 'Z'
        return t

    @property
    def name(self):
        return '-'.join((self.net, self.station, self.location, self.channel))

    @property
    def file_format(self):
        return 'mseed'

    @property
    def fname(self):
        return '%s-%s.%s' % (self.name, self.time, self.file_format)

    @property
    def stats(self):
        return Stats({
            'network': self.net,
            'location': self.location,
            'station': self.station,
            'channel': self.channel,
            'starttime': self.starttime,
            'sampling_rate': self.rate,
            'npts': self.num_samples,
            'calib': self.calib
        })

    @property
    def delta(self):
        return 1.0 / self.rate

    @property
    def endtime(self):
        return self.starttime + self.delta * self.num_samples

    def __repr__(self):
        return '%s | %s - %s | %.1f Hz, %d samples' % (self.name, self._format_time(self.starttime),
                                                       self._format_time(self.endtime), self.rate, self.num_samples)


class PacketLog(object):
    TIME_FUDGE_PCNT = 10
    base_dir = './antelope_data'
    is_diverted = False

    def __init__(self):
        self.header = None
        self.needs_flush = False
        self.closed = False
        self.data = Stream([])
        self._relpath = None

        # Generate a UUID for this PacketLog
        self.bin_uuid = str(uuid.uuid4())

    def create(self, net, location, station, channel, start, mintime, maxtime, rate, calib, calper, refdes):
        self.header = PacketLogHeader(net, location, station, channel, start, mintime, maxtime, rate, calib, calper, refdes)
        if not os.path.exists(self.abspath):
            try:
                os.makedirs(self.abspath)
            except OSError:
                raise InstrumentProtocolException('OSError occurred while creating file path: ' + self.abspath)
        elif os.path.isfile(self.abspath):
            raise InstrumentProtocolException('Error creating file path: File exists with same name: ' + self.abspath)

    @staticmethod
    def from_packet(packet, bin_start, bin_end, refdes):
        packet_log = PacketLog()
        packet_log.create(
            packet.get('net', ''),
            packet.get('loc', ''),
            packet.get('sta', ''),
            packet.get('chan', ''),
            packet['time'],
            bin_start,
            bin_end,
            packet['samprate'],
            packet['calib'],
            packet['calper'],
            refdes
            )
        return packet_log

    @property
    def relpath(self):
        if self._relpath is None:
            # Get the year, month and day for the directory structure of the data file from the packet start time
            packet_start_time = datetime.utcfromtimestamp(self.header.mintime)
            year = str(packet_start_time.year)
            month = '%02d' % packet_start_time.month
            day = '%02d' % packet_start_time.day
            if self.is_diverted:
                self._relpath = os.path.join(year, month, day, 'addendum')
            else:
                self._relpath = os.path.join(year, month, day)
        return self._relpath

    @property
    def abspath(self):
        return os.path.join(self.base_dir, self.header.refdes, self.relpath)

    @property
    def relname(self):
        return os.path.join(self.relpath, self.header.fname)

    @property
    def absname(self):
        return os.path.join(self.base_dir, self.header.refdes, self.relname)

    def add_packet(self, packet):
        # If the current packet is not within the 5 min file, then move to the next 5 minute file
        if self.header.mintime > packet['time'] or packet['time'] >= self.header.maxtime:
            log.info('******** GAP EXCEPTION ABOUT TO BE RAISED ********')
            log.info('packet[\'time\']: %s' % str(packet['time']))
            log.info('starttime: %s' % str(self.header.starttime))
            log.info('mintime: %s' % str(self.header.mintime))
            log.info('maxtime: %s' % str(self.header.maxtime))
            log.info('num_samples: %s' % str(self.header.num_samples))
            raise GapException()

        # Check that the packet is fully contained in the 5m file
        packet_endtime = float(Decimal(str(packet['time'])) + (Decimal(self.header.delta) * Decimal(str(packet['nsamp']))))
        if self.header.maxtime >= packet_endtime:
            self._write_data(packet['data'], float(Decimal(str(packet['time']))))
            return None

        # Check the time delta between the current 5m max time and packet start time
        # Used to tell whether we are on a boundary
        diff = Decimal(str(self.header.maxtime)) - Decimal(str(packet['time']))
        # Check if diff is > 0
        if float(diff) > 0:
            log.info('maxtime: %s' % str(self.header.maxtime))
            log.info('packet[\'time\']: %s' % str(packet['time']))
            log.info('diff: %s' % str(diff))

            # Number of samples between the packet start time and 5 min mark
            # The maximum number of samples can be 19,200,000 for a 5 minute file, need to check this condition
            nsamps = int(math.ceil(diff * Decimal(str(self.header.rate))))
            log.info('nsamps before: %s' % str(nsamps))

            # Write the data up to the current 5m mark
            data_before = packet['data'][:nsamps]
            self._write_data(data_before, float(Decimal(str(packet['time']))))

            # Prepare the remaining data in the packet and send back for the next 5m file
            packet['data'] = packet['data'][nsamps:]
            packet['nsamp'] = len(packet['data'])

            packet['time'] = float(Decimal(str(packet['time'])) + (Decimal(self.header.delta) * Decimal(str(len(data_before)))))
            log.info('nsamps after: %s' % str(packet['nsamp']))
            log.info('packet[\'time\'] after: %s' % str(packet['time']))
            return packet
        else:
            return None

    def _write_data(self, data, mintime):
        # Append Trace to Stream
        count = len(data)
        # Set the number of data points in the Trace metadata
        self.header.num_samples = count
        # Set the Trace metadata starttime to the packet's first data point time
        self.header.starttime = float(Decimal(str(mintime)))
        self.data.append(Trace(np.asarray(data, dtype='i'), self.header.stats))
        self.needs_flush = True

    def _write_trace(self):
        # Write multi-trace Stream to MSEED
        log.info('_write_trace: Hydrophone data rate: %s' % str(self.header.rate))
        stream = self.data
        stream.write(self.absname, format='MSEED')

    def flush(self):
        if self.needs_flush:
            log.info('flush: %-40s', self.absname)
            self._write_trace()

            self.needs_flush = False
