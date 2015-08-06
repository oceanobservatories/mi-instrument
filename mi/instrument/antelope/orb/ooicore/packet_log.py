from datetime import datetime

from obspy.core import Stats
import numpy as np
from obspy import Trace

from mi.core.log import get_logger

log = get_logger()


class GapException(Exception):
    pass


class TimeRangeException(Exception):
    pass


class PacketLogHeader(object):
    def __init__(self, net, location, station, channel, starttime, maxtime, rate, calib, calper):
        self.net = net
        self.location = location
        self.station = station
        self.channel = channel
        self.starttime = starttime
        self.maxtime = maxtime
        self.rate = rate
        self.calib = calib
        self.calper = calper
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

    def __init__(self):
        self.header = None
        self.needs_flush = False
        self.closed = False
        self.data = []

    def create(self, net, location, station, channel, start, end, rate, calib, calper):
        self.header = PacketLogHeader(net, location, station, channel, start, end, rate, calib, calper)

    @staticmethod
    def from_packet(packet, end):
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
            packet['calper']
            )
        return packet_log

    @property
    def filename(self):
        return self.header.name + '.' + self.header.time + '.mseed'

    def add_packet(self, packet):
        if self.header.starttime > packet['time'] or packet['time'] >= self.header.maxtime:
            raise TimeRangeException()

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
        nsamps = int(round(diff * self.header.rate, 10))
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
        trace = Trace(np.array(self.data, dtype='int32'), self.header.stats)
        trace.write(self.filename, format='MSEED')

    def flush(self):
        if self.needs_flush:
            log.info('flush: %-40s', self.filename)
            self._write_trace()

            self.needs_flush = False
