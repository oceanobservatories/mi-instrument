import os
import math
import uuid
from datetime import datetime

from obspy.core import Stats
import numpy as np
from obspy import Trace

from mi.core.log import get_logger
from mi.core.exceptions import InstrumentProtocolException

log = get_logger()


class Vector(object):
    def __init__(self, size, dtype, factor=1.25):
        self.dtype = dtype
        self.factor = factor
        self.backing_store = np.zeros(size, dtype=dtype)
        self.length = size
        self.index = 0

    def append(self, value):
        self._realloc(self.index+1)
        self.backing_store[self.index] = value
        self.index += 1

    def extend(self, value):
        new_index = self.index + len(value)
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
    def __init__(self, net, location, station, channel, starttime, maxtime, rate, calib, calper, refdes):
        self.net = net
        self.location = location
        self.station = station
        self.channel = channel
        self.starttime = starttime
        self.maxtime = maxtime
        self.rate = rate
        self.calib = calib
        self.calper = calper
        self.refdes = refdes
        self.num_samples = 0

    @property
    def time(self):
        return self._format_time(self.starttime)

    @staticmethod
    def _format_time(timestamp):
        t = datetime.isoformat(datetime.utcfromtimestamp(timestamp))
        # if the time has no fractional value the iso timestamp won't contain the trailing zeros
        # add them back in if they are missing for consistency in file naming
        if t[-3] == ':':
            t += '.000000'
        return t

    @property
    def name(self):
        return '%s.%s.%s.%s' % (self.net, self.station, self.location, self.channel)

    @property
    def fname(self):
        return '%s.%s.mseed' % (self.name, self.time)

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

    def __init__(self):
        self.header = None
        self.needs_flush = False
        self.closed = False
        self.data = Vector(1000, 'i')
        self._relpath = None

        # Generate a UUID for this PacketLog
        self.bin_uuid = str(uuid.uuid4())

    def create(self, net, location, station, channel, start, end, rate, calib, calper, refdes):
        self.header = PacketLogHeader(net, location, station, channel, start, end, rate, calib, calper, refdes)
        if not os.path.exists(self.abspath):
            try:
                os.makedirs(self.abspath)
            except OSError:
                raise InstrumentProtocolException('OSError occurred while creating file path: ' + self.abspath)
        elif os.path.isfile(self.abspath):
            raise InstrumentProtocolException('Error creating file path: File exists with same name: ' + self.abspath)

    @staticmethod
    def from_packet(packet, end, refdes):
        packet_log = PacketLog()
        packet_log.create(
            packet.get('net', ''),
            packet.get('loc', ''),
            packet.get('sta', ''),
            packet.get('chan', ''),
            packet['time'],
            end,
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
            packet_start_time = datetime.utcfromtimestamp(self.header.starttime)
            year = str(packet_start_time.year)
            month = '%02d' % packet_start_time.month
            day = '%02d' % packet_start_time.day
            self._relpath = os.path.join(year, month, day)
        return self._relpath

    @property
    def abspath(self):
        return os.path.join(self.base_dir, self.header.refdes, self.relpath)

    @property
    def relname(self):
        return os.path.join(self.relpath, self.header.name + '.' + self.header.time + '.mseed')

    @property
    def absname(self):
        return os.path.join(self.base_dir, self.header.refdes, self.relname)

    def add_packet(self, packet):
        if self.header.starttime > packet['time'] or packet['time'] >= self.header.maxtime:
            raise GapException()

        # check if there is a gap, if so reject this packet
        diff = abs(self.header.endtime - packet['time'])
        maxdiff = packet['nsamp'] * self.header.delta * self.TIME_FUDGE_PCNT / 100
        if diff > maxdiff:
            raise GapException()

        # split this packet if necessary
        packet_endtime = packet['time'] + self.header.delta * packet['nsamp']
        if self.header.maxtime >= packet_endtime:
            self._write_data(packet['data'])
            return None

        diff = self.header.maxtime - packet['time']
        nsamps = int(math.ceil(diff * self.header.rate))
        self._write_data(packet['data'][:nsamps])
        packet['data'] = packet['data'][nsamps:]
        packet['nsamp'] = len(packet['data'])
        packet['time'] += nsamps * self.header.delta
        return packet

    def _write_data(self, data):
        count = len(data)
        self.data.extend(data)
        self.header.num_samples += count
        self.needs_flush = True

    def _write_trace(self):
        trace = Trace(self.data.get(), self.header.stats)
        trace.write(self.absname, format='MSEED')

    def flush(self):
        if self.needs_flush:
            log.info('flush: %-40s', self.absname)
            self._write_trace()

            self.needs_flush = False
