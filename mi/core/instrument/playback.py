#!/usr/bin/env python
"""
@package mi.core.instrument.playback
@file mi/core/instrument/playback.py
@author Ronald Ronquillo
@brief Playback process using ZMQ messaging.

Usage:
    playback datalog <module> <protocol_class> <refdes> <event_url> <particle_url> <files>...
    playback ascii <module> <protocol_class> <refdes> <event_url> <particle_url> <files>...
    playback chunky <module> <protocol_class> <refdes> <event_url> <particle_url> <files>...

Options:
    -h, --help          Show this screen

    To run without installing:
    python -m mi.core.instrument.playback ...
"""

import importlib
import glob
import sys
from mi.core.instrument.instrument_protocol import MenuInstrumentProtocol, CommandResponseInstrumentProtocol, \
    InstrumentProtocol
import os
import re
import time
from docopt import docopt

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.publisher import Publisher
from ooi_port_agent.packet import Packet, PacketHeader
from ooi_port_agent.common import string_to_ntp_date_time, PacketType
from wrapper import EventKeys, encode_exception


__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


class PlaybackPacket(Packet):
    def get_data_length(self):
        return len(self.payload)

    def get_data(self):
        return self.payload

    def get_timestamp(self):
        return self.header.time

    def __repr__(self):
        return repr(self.payload)


class PlaybackWrapper(object):
    def __init__(self, module, klass, refdes, event_url, particle_url, reader_klass, files):
        headers = {'sensor': refdes, 'deliveryType': 'playback'}
        self.event_publisher = Publisher.from_url(event_url, headers)
        self.particle_publisher = Publisher.from_url(particle_url, headers)
        self.events = []
        self.particles = []

        self.protocol = self.construct_protocol(module, klass)
        self.reader = reader_klass(files, self.got_data)

    def playback(self):
        for index, _ in enumerate(self.reader.read()):
            if index % 100 == 0:
                self.publish()
        self.publish()

    def got_data(self, packet):
        try:
            self.protocol.got_data(packet)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.exception(e)

    def construct_protocol(self, proto_module, proto_class):
        module = importlib.import_module(proto_module)
        klass = getattr(module, proto_class)
        if klass.__base__ == MenuInstrumentProtocol:
            log.info('Found MenuInstrumentProtocol')
            return klass(None, None, None, self.handle_event)
        elif klass.__base__ == CommandResponseInstrumentProtocol:
            log.info('Found CommandResponseInstrumentProtocol')
            return klass(BaseEnum, None, self.handle_event)
        elif klass.__base__ ==  InstrumentProtocol:
            log.info('Found InstrumentProtocol')
            return klass(self.handle_event)

        log.error('Unable to import and create protocol from module: %r class: %r', module, proto_class)
        sys.exit(1)

    def publish(self):
        if self.events:
            self.event_publisher.publish(self.events)
            self.events = []
        if self.particles:
            self.particle_publisher.publish(self.particles)
            self.particles = []

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
                self.particles.append(event)
        else:
            self.events.append(event)


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

    def read(self):
        while True:
            if self._filehandle is None and not self.files:
                log.info('Completed reading specified port agent logs, exiting...')
                raise StopIteration

            if self._filehandle is None:
                name = self.files.pop(0)
                log.info('Begin reading: %r', name)
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


def main():
    options = docopt(__doc__)

    module = options['<module>']
    refdes = options['<refdes>']
    event_url = options['<event_url>']
    particle_url = options['<particle_url>']
    klass = options.get('<protocol_class>')
    files = options.get('<files>')

    # when running with the profiler, files will be a string
    # coerce to list
    if isinstance(files, basestring):
        files = [files]

    if options['datalog']:
        reader = DatalogReader
    elif options['ascii']:
        reader = DigiDatalogAsciiReader
    elif options['chunky']:
        reader = ChunkyDatalogReader
    else:
        reader = None

    wrapper = PlaybackWrapper(module, klass, refdes, event_url, particle_url, reader, files)
    wrapper.playback()

if __name__ == '__main__':
    main()
