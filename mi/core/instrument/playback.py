#!/usr/bin/env python
"""
@package mi.core.instrument.playback
@file mi/core/instrument/playback.py
@author Ronald Ronquillo
@brief Playback process using ZMQ messaging.

Usage:
    playback datalog <module> <refdes> <event_url> <particle_url> [--allowed=<particles>]  [--max_events=<events>] <files>...
    playback ascii <module> <refdes> <event_url> <particle_url> [--allowed=<particles>] [--max_events=<events>] <files>...
    playback chunky <module> <refdes> <event_url> <particle_url> [--allowed=<particles>] [--max_events=<events>] <files>...
    playback zplsc <module> <refdes> <event_url> <particle_url> [--allowed=<particles>] [--max_events=<events>] <files>...

Options:
    -h, --help          Show this screen
    --allowed=<particles> Comma-separated list of publishable particles

    To run without installing:
    python -m mi.core.instrument.playback ...
"""
import glob
import importlib
import sys
import time
from datetime import datetime

import os
import re
from docopt import docopt
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_protocol import \
    MenuInstrumentProtocol,\
    CommandResponseInstrumentProtocol, \
    InstrumentProtocol
from mi.core.instrument.publisher import Publisher
from mi.logging import log
from ooi_port_agent.common import PacketType
from ooi_port_agent.packet import Packet, PacketHeader
from wrapper import EventKeys, encode_exception, DriverWrapper

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'

NTP_DIFF = (datetime(1970, 1, 1) - datetime(1900, 1, 1)).total_seconds()
Y2K = (datetime(2000, 1, 1) - datetime(1900, 1, 1)).total_seconds()
DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def string_to_ntp_date_time(datestr):
    """
    Extract an ntp date from a ISO8601 formatted date string.
    @param datestr an ISO8601 formatted string containing date information
    @retval an ntp date number (seconds since jan 1 1900)
    @throws InstrumentParameterException if datestr cannot be formatted to
    a date.
    """
    if not isinstance(datestr, basestring):
        raise IOError('Value %s is not a string.' % str(datestr))

    if not DATE_MATCHER.match(datestr):
        raise ValueError("date string not in ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSSZ")

    try:
        # This assumes input date string are in UTC (=GMT)

        # if there is no decimal place, add one to match the date format
        if datestr.find('.') == -1:
            if datestr[-1] != 'Z':
                datestr += '.0Z'
            else:
                datestr = datestr[:-1] + '.0Z'

        # if there is no trailing 'Z' on the input string add one
        if datestr[-1:] != 'Z':
            datestr += 'Z'

        dt = datetime.strptime(datestr, DATE_FORMAT)
        timestamp = (dt - datetime(1900, 1, 1)).total_seconds()

    except ValueError as e:
        raise ValueError('Value %s could not be formatted to a date. %s' % (str(datestr), e))

    return timestamp


class PlaybackPacket(Packet):
    def get_data_length(self):
        return len(self.payload)

    def get_data(self):
        return self.payload

    def get_timestamp(self):
        return self.header.time

    def __repr__(self):
        return repr(self.payload)

    @staticmethod
    def packet_from_fh(file_handle):
        data_buffer = bytearray()
        while True:
            byte = file_handle.read(1)
            if byte == '':
                return None

            data_buffer.append(byte)
            sync_index = data_buffer.find(PacketHeader.sync)
            if sync_index != -1:
                # found the sync bytes, read the rest of the header
                data_buffer.extend(file_handle.read(PacketHeader.header_size - len(PacketHeader.sync)))

                if len(data_buffer) < PacketHeader.header_size:
                    return None

                header = PacketHeader.from_buffer(data_buffer, sync_index)
                # read the payload
                payload = file_handle.read(header.payload_size)
                if len(payload) == header.payload_size:
                    packet = PlaybackPacket(payload=payload, header=header)
                    return packet


class PlaybackWrapper(object):
    def __init__(self, module, refdes, event_url, particle_url, reader_klass, allowed, files, max_events, handler=None):
        version = DriverWrapper.get_version(module)
        headers = {'sensor': refdes, 'deliveryType': 'streamed', 'version': version, 'module': module}
        self.max_events = max_events
        self.event_publisher = Publisher.from_url(event_url, handler=handler, headers=headers)
        self.particle_publisher = Publisher.from_url(particle_url, handler=handler, headers=headers, allowed=allowed,
                                                     max_events=max_events)
        self.protocol = self.construct_protocol(module)
        self.reader = reader_klass(files, self.got_data)

    def set_header_filename(self, filename):
        self.event_publisher.set_source(filename)
        self.particle_publisher.set_source(filename)

    def playback(self):
        for index, filename in enumerate(self.reader.read()):
            if filename is not None:
                self.set_header_filename(filename)
                if hasattr(self.protocol, 'got_filename'):
                    self.protocol.got_filename(filename)
            if index % 1000 == 0:
                self.publish()

        self.publish()
        if hasattr(self.particle_publisher, 'write'):
            self.particle_publisher.write()

    def zplsc_playback(self):
        for index, filename in enumerate(self.reader.read()):
            if filename:
                self.set_header_filename(filename)
                log.info("filename is: %s", filename)
                if hasattr(self.protocol, 'got_filename'):
                    self.protocol.got_filename(filename)

        pub_index = 0
        while True:
            self.publish()
            pub_index = pub_index + 1
            log.info("publish index is: %d", pub_index)

    def got_data(self, packet):
        try:
            self.protocol.got_data(packet)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.exception(e)

    @staticmethod
    def find_base_class(base):
        targets = (MenuInstrumentProtocol, CommandResponseInstrumentProtocol, InstrumentProtocol, object)
        while True:
            if base in targets:
                return base
            base = base.__base__

    def construct_protocol(self, proto_module):
        module = importlib.import_module(proto_module)
        if hasattr(module, 'create_playback_protocol'):
            return module.create_playback_protocol(self.handle_event)

        log.error('Unable to import and create playback protocol from module: %r', module)
        sys.exit(1)

    def publish(self):
        for publisher in [self.event_publisher, self.particle_publisher]:
            remaining = publisher.publish()
            while remaining >= publisher._max_events:
                remaining = publisher.publish()

    def handle_event(self, event_type, val=None):
        """
        Construct and send an asynchronous driver event.
        @param event_type a DriverAsyncEvent type specifier.
        @param val event value for sample and test result events.
        """
        event = {
            'type': event_type,
            'value': val,
            'time': time.time()
        }

        if isinstance(event[EventKeys.VALUE], Exception):
            event[EventKeys.VALUE] = encode_exception(event[EventKeys.VALUE])

        if event[EventKeys.TYPE] == DriverAsyncEvent.ERROR:
            log.error(event)

        if event[EventKeys.TYPE] == DriverAsyncEvent.SAMPLE:
            if event[EventKeys.VALUE].get('stream_name') != 'raw':
                # don't publish raw
                self.particle_publisher.enqueue(event)
        else:
            self.event_publisher.enqueue(event)


class DatalogReader(object):
    def __init__(self, files, callback):
        self.callback = callback

        self.files = []
        for each in files:
            self.files.extend(glob.glob(each))

        self.files.sort()
        if not all([os.path.isfile(f) for f in self.files]):
            raise Exception('Not all files found')
        self._filehandle = None
        self.target_types = [PacketType.FROM_INSTRUMENT, PacketType.PA_CONFIG]
        self.file_name_list = []

    def read(self):
        while True:
            if self._filehandle is None and not self.files:
                log.info('Completed reading specified port agent logs, exiting...')
                raise StopIteration

            if self._filehandle is None:
                name = self.files.pop(0)
                log.info('Begin reading: %r', name)
                # yield the filename so we can pass it through to the driver
                yield name
                self.file_name_list.append(name)
                self._filehandle = open(name, 'r')

            if not self._process_packet():
                self._filehandle.close()
                self._filehandle = None

            yield

    def _process_packet(self):
        packet = PlaybackPacket.packet_from_fh(self._filehandle)
        if packet is None:
            return False
        if packet.header.packet_type in self.target_types:
            self.callback(packet)
        return True


class DigiDatalogAsciiReader(DatalogReader):
    def __init__(self, files, callback):
        self.ooi_ts_regex = re.compile(r'<OOI-TS (.+?) [TX][NS]>\r\n(.*?)<\\OOI-TS>', re.DOTALL)
        self.buffer = ''
        self.MAXBUF = 65535

        super(DigiDatalogAsciiReader, self).__init__(files, callback)

        if all((self.search_utc(f) for f in self.files)):
            self.files.sort(key=self.search_utc)

    # special case for RSN archived data
    # if all files have date_UTC in filename then sort by that
    @staticmethod
    def search_utc(f):
        match = re.search('(\d+T\d+_UTC)', f)
        if match is None:
            return None
        return match.group(1)

    def _process_packet(self):
        chunk = self._filehandle.read(1024)
        if chunk != '':
            self.buffer += chunk
            new_index = 0
            for match in self.ooi_ts_regex.finditer(self.buffer):
                payload = match.group(2)
                try:
                    packet_time = string_to_ntp_date_time(match.group(1))
                    header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT,
                                          payload_size=len(payload), packet_time=packet_time)
                    header.set_checksum(payload)
                    packet = PlaybackPacket(payload=payload, header=header)
                    self.callback(packet)
                except ValueError:
                    log.error('Unable to extract timestamp from record: %r' % match.group())
                new_index = match.end()

            if new_index > 0:
                self.buffer = self.buffer[new_index:]

            if len(self.buffer) > self.MAXBUF:
                self.buffer = self.buffer[-self.MAXBUF:]

            return True

        return False


class ChunkyDatalogReader(DatalogReader):
    def _process_packet(self):
        data = self._filehandle.read(1024)
        if data != '':
            header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT,
                                  payload_size=len(data), packet_time=0)
            header.set_checksum(data)
            packet = PlaybackPacket(payload=data, header=header)
            self.callback(packet)
            return True
        return False


class ZplscReader(DatalogReader):
    def __init__(self, files, callback):
        super(ZplscReader, self).__init__(files, callback)

    def _process_packet(self):

        for name in self.file_name_list:
            data = 'downloaded file:' + name + '\n'
            header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT,
                                  payload_size=len(data), packet_time=0)
            header.set_checksum(data)
            packet = PlaybackPacket(payload=data, header=header)
            self.callback(packet)

        return False


def main():
    options = docopt(__doc__)

    module = options['<module>']
    refdes = options['<refdes>']
    event_url = options['<event_url>']
    particle_url = options['<particle_url>']
    files = options.get('<files>')
    allowed = options.get('--allowed')
    if allowed is not None:
        allowed = [_.strip() for _ in allowed.split(',')]
    max_events = options.get('--max_events')
    if not max_events:
        max_events = Publisher.DEFAULT_MAX_EVENTS
    else:
        max_events = int(max_events)

    # when running with the profiler, files will be a string
    # coerce to list
    if isinstance(files, basestring):
        files = [files]
        
    zplsc_reader = False
    
    if options['datalog']:
        reader = DatalogReader
    elif options['ascii']:
        reader = DigiDatalogAsciiReader
    elif options['chunky']:
        reader = ChunkyDatalogReader
    elif options['zplsc']:
        reader = ZplscReader
        zplsc_reader = True
    else:
        reader = None

    wrapper = PlaybackWrapper(module, refdes, event_url, particle_url, reader, allowed, files, max_events)
    if zplsc_reader:
        wrapper.zplsc_playback()
    else:
        wrapper.playback()

if __name__ == '__main__':
    main()
