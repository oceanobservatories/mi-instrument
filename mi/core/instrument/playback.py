#!/usr/bin/env python

"""
@package mi.core.instrument.playback
@file mi/core/instrument/playback.py
@author Ronald Ronquillo
@brief Playback process using ZMQ messaging.
"""

import importlib
import glob
import Queue
import re
import threading
import time
import traceback

from mi.core.common import BaseEnum
from mi.core.instrument.instrument_driver import DriverAsyncEvent, InstrumentDriver
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.publisher import Publisher
from ooi_port_agent.packet import Packet, PacketHeader
from ooi_port_agent.common import string_to_ntp_date_time, PacketType

from wrapper import DriverWrapper, EventKeys, encode_exception

from mi.core.log import get_logger, get_logging_metaclass
log = get_logger()

META_LOGGER = get_logging_metaclass('trace')

log.info('help!')

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


class PlaybackDriver(InstrumentDriver):
    def get_resource_state(self, *args, **kwargs):
        return DriverProtocolState.AUTOSAMPLE


class PlaybackWrapper(DriverWrapper):
    """
    Base class for messaging enabled OS-level driver processes. Provides
    run loop, dynamic driver import and construction and interface
    for messaging implementation subclasses.
    """

    def __init__(self, driver_module, driver_class, refdes, event_url, particle_url,
                 datalog_reader_class, files):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        super(PlaybackWrapper, self).__init__(
            driver_module, driver_class, refdes, event_url, particle_url)

        self.publish_interval = 10
        self.data_to_read = True
        self.read_thread = None

        self.construct_driver()
        self.read_thread = threading.Thread(
            target=self.protocol_read, args=(datalog_reader_class, files, self.driver))
        self.read_thread.start()

    def protocol_read(self, reader_class, files, driver):
        datalog_reader = reader_class({'files': files}, driver)

        while self.data_to_read:
            self.data_to_read = datalog_reader.read()

    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        try:
            module = importlib.import_module(self.driver_module)
            driver_class = getattr(module, self.driver_class)
            self.driver = driver_class(BaseEnum, None, PlaybackDriver(self.send_event)._driver_event)

            log.info('Imported and created driver from module: %r class: %r driver: %r refdes: %r',
                     module, driver_class, self.driver, self.refdes)
            return True
        except:
            pass

        # Try again to support Protocols that only need the driver event callback parameter
        try:
            module = importlib.import_module(self.driver_module)
            driver_class = getattr(module, self.driver_class)
            self.driver = driver_class(PlaybackDriver(self.send_event)._driver_event)

            log.info('Imported and created driver from module: %r class: %r driver: %r refdes: %r',
                     module, driver_class, self.driver, self.refdes)
            return True

        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error('Could not import/construct driver module %s, class %s, refdes: %r.',
                      self.driver_module, self.driver_class, self.refdes)
            log.error('%s' % str(e))
            return False

    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. This ZMQ implementation starts and
        joins command and event threads, managing nonblocking send/recv calls
        on REP and PUB sockets, respectively. Terminate loops and close
        sockets when stop flag is set in driver process.
        """
        self.evt_thread = threading.Thread(target=self.send_evt_msg)
        self.evt_thread.start()
        self.messaging_started = True

    def send_evt_msg(self):
        """
        Await events on the driver process event queue and publish them
        on a ZMQ PUB socket to the driver process client.
        """
        self.stop_evt_thread = False
        headers = {'sensor': self.refdes, 'deliveryType': 'streamed'}
        event_publisher = Publisher.from_url(self.event_url, headers)
        particle_publisher = Publisher.from_url(self.particle_url, headers)

        events = []
        particles = []

        while not self.stop_evt_thread:
            try:
                evt = self.events.get_nowait()
                # log.info(evt)
                if isinstance(evt[EventKeys.VALUE], Exception):
                    evt[EventKeys.VALUE] = encode_exception(evt[EventKeys.VALUE])
                if evt[EventKeys.TYPE] == DriverAsyncEvent.ERROR:
                    log.error(evt)

                if evt[EventKeys.TYPE] == DriverAsyncEvent.SAMPLE:
                    if evt[EventKeys.VALUE].get('stream_name') == 'raw':
                        # don't publish raw
                        continue
                    particles.append(evt)
                else:
                    events.append(evt)

                if len(particles) >= self.publish_interval:
                    particle_publisher.publish(particles)
                    self.particle_count += len(particles)
                    particles = []

            except Queue.Empty:

                if particles:
                    particle_publisher.publish(particles)
                    self.particle_count += len(particles)
                    particles = []

                if events:
                    event_publisher.publish(events)
                    events = []

                if self.data_to_read is False:
                    log.info("Particles published: %s", self.particle_count)
                    self.stop_messaging()

                time.sleep(.5)
            except Exception:
                traceback.print_exc()


class DatalogReader(object):
    def __init__(self, config, router):
        self.router = router

        self.files = []
        for each in config['files']:
            self.files.extend(glob.glob(each))

        self.files.sort()
        self._filehandle = None
        self.target_types = [PacketType.FROM_INSTRUMENT, PacketType.PA_CONFIG]

    def read(self):
        """
        Read one packet, publish if appropriate, then return.
        We must not read all packets in a loop here,
        or we will not actually publish them until the end...
        """
        if self._filehandle is None and not self.files:
            log.info('Completed reading specified port agent logs, exiting...')
            return False

        if self._filehandle is None:
            name = self.files.pop(0)
            log.info('Begin reading: %r', name)
            self._filehandle = open(name, 'r')

        if not self._process_packet():
            self._filehandle.close()
            self._filehandle = None

        return True

    def _process_packet(self):
        packet = Packet.packet_from_fh(self._filehandle)
        if packet is not None:
            if packet.header.packet_type in self.target_types:
                self.router.got_data(packet)
            return True
        else:
            return False


class DigiDatalogAsciiReader(DatalogReader):
    def __init__(self, config, router):

        self.ooi_ts_regex = re.compile(r'<OOI-TS (.+?) [TX][NS]>\r\n(.*?)<\\OOI-TS>', re.DOTALL)
        self.buffer = ''
        self.MAXBUF = 65535

        super(DigiDatalogAsciiReader, self).__init__(config, router)

        if all((self.search_utc(f) for f in self.files)):
            self.files.sort(key=self.search_utc)

    # special case for RSN archived data
    # if all files have date_UTC in filename then sort by that
    @staticmethod
    def search_utc(f):
            match = re.search('(\d+T\d+_UTC)', f)
            if match is None:
                return None
            else:
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
                    packet = Packet(payload=payload, header=header)
                    self.router.got_data(packet)
                except ValueError:
                    log.err('Unable to extract timestamp from record: %r' % match.group())
                new_index = match.end()

            if new_index > 0:
                self.buffer = self.buffer[new_index:]

            if len(self.buffer) > self.MAXBUF:
                self.buffer = self.buffer[-self.MAXBUF:]

            return True

        else:
            return False


class ChunkyDatalogReader(DatalogReader):
    def __init__(self, config, router):
        super(ChunkyDatalogReader, self).__init__(config, router)

    def _process_packet(self):
        data = self._filehandle.read(1024)
        if data != '':
            header = PacketHeader(packet_type=PacketType.FROM_INSTRUMENT,
                                  payload_size=len(data), packet_time=0)
            header.set_checksum(data)
            packet = [Packet(payload=data, header=header)]
            self.router.got_data(packet)
            return True
        else:
            return False