import os
import math
import uuid
from datetime import datetime

from obspy.core import Stats
import numpy as np
from obspy import Trace
from obspy.core.utcdatetime import UTCDateTime

import soundfile as sf
import json
import io

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
        return 'flac'

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

    def __init__(self):
        self.header = None
        self.needs_flush = False
        self.closed = False
        self.data = Vector(1000, 'i')
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
        log.info('Hydrophone data rate: %s' % str(self.header.rate))

        # Store the Orb data to an ObsPy Trace, trim to 5 minutes and pad with zeros
        trace = Trace(self.data.get(), self.header.stats)
        trace_trimmed_padded = trace.copy()
        trace_trimmed_padded.trim(starttime=UTCDateTime(self.header.mintime),
                                  endtime=UTCDateTime(self.header.maxtime),
                                  pad=True,
                                  nearest_sample=True,
                                  fill_value=0)

        # Write the trimmed and padded FLAC file and JSON metadata to disk
        flac_trimmed_padded_filename = self.absname
        with sf.SoundFile(flac_trimmed_padded_filename, 'w', int(self.header.rate), 1, 'PCM_24') as f:
            # trace.data is in int32, Soundfile native is float64, so convert it
            trace_float = trace_trimmed_padded.data / ((2.0 ** 23) - 1)
            f.write(trace_float)
        flac_trimmed_padded_filename_json = flac_trimmed_padded_filename + '.json'
        with io.open(flac_trimmed_padded_filename_json, 'w', encoding='utf-8') as f:
            file_stats = sf.info(flac_trimmed_padded_filename, verbose=True)
            file_stats_dict = file_stats.__dict__
            file_stats_clean = json.dumps({key: value for key, value in file_stats_dict.items()}, indent=4, sort_keys=True, default=str)
            f.write(unicode(file_stats_clean))
        log.info('Wrote 5 minute, zero-padded FLAC file to disk: %s' % flac_trimmed_padded_filename)

        # TODO: Remove this block after testing
        # Write the MSEED file to disk
        trace.write(self.absname.replace('.flac', '_orig.mseed'), format='MSEED')
        mseed_filename_json = self.absname.replace('.flac', '_orig.mseed') + '.json'
        with io.open(mseed_filename_json, 'w', encoding='utf-8') as f:
            file_stats = trace.stats
            file_stats_dict = file_stats.__dict__
            file_stats_clean = json.dumps({key: value for key, value in file_stats_dict.items()}, indent=4, sort_keys=True, default=str)
            f.write(unicode(file_stats_clean))
        log.info('Wrote original Trace MSEED file to disk: %s' % self.absname.replace('.flac', '_orig.mseed'))

        # TODO: Remove this block after testing
        # Write the trimmed and padded MSEED file to disk
        trace_trimmed_padded_filename = self.absname.replace('.flac', '.mseed')
        trace_trimmed_padded.write(trace_trimmed_padded_filename, format='MSEED')
        mseed_trimmed_padded_filename_json = trace_trimmed_padded_filename + '.json'
        with io.open(mseed_trimmed_padded_filename_json, 'w', encoding='utf-8') as f:
            file_stats = trace_trimmed_padded.stats
            file_stats_dict = file_stats.__dict__
            file_stats_clean = json.dumps({key: value for key, value in file_stats_dict.items()}, indent=4, sort_keys=True, default=str)
            f.write(unicode(file_stats_clean))
        log.info('Wrote trimmed and zero padded MSEED file to disk: %s' % trace_trimmed_padded_filename)

        # TODO: Remove this block after testing
        # Write the FLAC file to disk
        flac_filename = self.absname.replace('.flac', '_orig.flac')
        with sf.SoundFile(flac_filename, 'w', int(self.header.rate), 1, 'PCM_24') as f:
            # trace.data is in int32, Soundfile native is float64, so convert it
            trace_float = trace.data / ((2.0 ** 23) - 1)
            f.write(trace_float)
        flac_filename_json = flac_filename + '.json'
        with io.open(flac_filename_json, 'w', encoding='utf-8') as f:
            file_stats = sf.info(flac_filename, verbose=True)
            file_stats_dict = file_stats.__dict__
            file_stats_clean = json.dumps({key: value for key, value in file_stats_dict.items()}, indent=4, sort_keys=True, default=str)
            f.write(unicode(file_stats_clean))
        log.info('Wrote original Trace FLAC file to disk: %s' % flac_filename)

    def flush(self):
        if self.needs_flush:
            log.info('flush: %-40s', self.absname)
            self._write_trace()

            self.needs_flush = False
