#!/usr/bin/env python

"""
@package mi.core.instrument.port_agent_client
@file mi/core/instrument/port_agent_client
@author David Everett
@brief Client to connect to the port agent
and logging.
"""
import errno
import socket
import struct
import threading
import time

from mi.core.exceptions import InstrumentConnectionException, InstrumentException
from mi.core.log import get_logger

__author__ = 'David Everett'
__license__ = 'Apache 2.0'


log = get_logger()


# pure python LRC in case we don't have ooi_port_agent
def py_lrc(data, seed=0):
    for val in bytearray(data):
        seed ^= val
    return seed

try:
    from ooi_port_agent.lrc import lrc
except ImportError:
    log.error('Unable to import compiled LRC function, falling back to pure python implementation (SLOW!)')
    lrc = py_lrc


HEADER_FORMAT = '>4BHHII'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
OFFSET_P_CHECKSUM_LOW = 6
OFFSET_P_CHECKSUM_HIGH = 7


# Offsets into the unpacked header fields
SYNC_BYTE1_INDEX = 0
TYPE_INDEX = 3
LENGTH_INDEX = 4  # packet size (including header)
CHECKSUM_INDEX = 5
TIMESTAMP_UPPER_INDEX = 6
TIMESTAMP_LOWER_INDEX = 7

MAX_SEND_ATTEMPTS = 15  # Max number of times we can get EAGAIN
NEWLINE = '\n'


class SocketClosed(Exception):
    pass


class Done(Exception):
    pass


class PortAgentPacket:
    """
    An object that encapsulates the details packets that are sent to and
    received from the port agent.
    https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+Port+Agent+Design
    """

    # Port Agent Packet Types
    DATA_FROM_INSTRUMENT = 1
    DATA_FROM_DRIVER = 2
    PORT_AGENT_COMMAND = 3
    PORT_AGENT_STATUS = 4
    PORT_AGENT_FAULT = 5
    PORT_AGENT_CONFIG = 6
    DIGI_CMD = 7
    DIGI_RSP = 8
    HEARTBEAT = 9
    PICKLED_FROM_INSTRUMENT = 10

    def __init__(self, packet_type=None):
        self.__header = None
        self.__data = None
        self.__type = packet_type
        self.__length = None
        self.__port_agent_timestamp = None
        self.__recv_checksum = None
        self.__checksum = None
        self.__isValid = False

    def unpack_header(self, header):
        self.__header = header
        variable_tuple = struct.unpack_from(HEADER_FORMAT, header)
        # change offset to index.
        self.__type = variable_tuple[TYPE_INDEX]
        self.__length = int(variable_tuple[LENGTH_INDEX]) - HEADER_SIZE
        self.__recv_checksum = int(variable_tuple[CHECKSUM_INDEX])
        upper = variable_tuple[TIMESTAMP_UPPER_INDEX]
        lower = variable_tuple[TIMESTAMP_LOWER_INDEX]
        # NTP timestamps are a 64-bit value. High 32 bits are seconds, low 32 are fractional seconds
        self.__port_agent_timestamp = upper + float(lower) / 2 ** 32

    def pack_header(self):
        """
        Given a type and length, pack a header to be sent to the port agent.
        """
        if self.__data is None:
            log.error('pack_header: no data!')

        else:
            # Set the packet type if it was not passed in as parameter
            if self.__type is None:
                self.__type = self.DATA_FROM_DRIVER
            self.__length = len(self.__data)
            self.__port_agent_timestamp = time.time()

            int_secs = int(self.__port_agent_timestamp)
            frac_secs = int((self.__port_agent_timestamp - int_secs) * 2**32)
            variable_tuple = (0xa3, 0x9d, 0x7a, self.__type,
                              self.__length + HEADER_SIZE, 0x0000,
                              int_secs, frac_secs)

            self.__header = struct.pack(HEADER_FORMAT, *variable_tuple)
            self.__checksum = self.calculate_checksum()
            self.__recv_checksum = self.__checksum

    def attach_data(self, data):
        self.__data = data

    def calculate_checksum(self):
        checksum = lrc(self.__header[:OFFSET_P_CHECKSUM_LOW])
        checksum = lrc(self.__header[OFFSET_P_CHECKSUM_HIGH:], checksum)
        checksum = lrc(self.__data, checksum)
        return checksum

    def verify_checksum(self):
        checksum = lrc(self.__header, lrc(self.__data))
        self.__isValid = checksum == 0

    def get_header(self):
        return self.__header

    def set_header(self, header):
        """
        This method is used for testing only; we want to test the checksum so
        this is one of the hoops we jump through to do that.
        :param header:
        """
        self.__header = header

    def get_data(self):
        return self.__data

    def get_timestamp(self):
        return self.__port_agent_timestamp

    def attach_timestamp(self, timestamp):
        self.__port_agent_timestamp = timestamp

    def set_timestamp(self):
        self.attach_timestamp(time.time())

    def get_data_length(self):
        return self.__length

    def set_data_length(self, length):
        self.__length = length

    def get_header_type(self):
        return self.__type

    def get_header_checksum(self):
        return self.__checksum

    def get_header_recv_checksum(self):
        return self.__recv_checksum

    def get_as_dict(self):
        """
        Return a dictionary representation of a port agent packet
        """
        return {
            'type': self.__type,
            'length': self.__length,
            'checksum': self.__checksum,
            'raw': self.__data
        }

    def is_valid(self):
        return self.__isValid


class PortAgentClient(object):
    """
    A port agent process client class to abstract the TCP interface to the
    of port agent. From the instrument driver's perspective, data is sent
    to the port agent with this client's send method, and data is received
    asynchronously via a callback from this client's listener thread.
    """

    BREAK_COMMAND = "break "
    GET_CONFIG_COMMAND = "get_config"
    GET_STATE_COMMAND = "get_state"

    def __init__(self, host, port, cmd_port, callback, error_callback, heartbeat=10, max_missed_heartbeats=5):
        """
        PortAgentClient constructor.
        """
        self.host = host
        self.port = port
        self.cmd_port = cmd_port
        self.sock = None
        self.listener_thread = None
        self.stop_event = None
        self.heartbeat = heartbeat
        self.max_missed_heartbeats = max_missed_heartbeats
        self.send_attempts = MAX_SEND_ATTEMPTS
        self.callback = callback
        self.error_callback = error_callback
        self.last_retry_time = None

    def init_comms(self):
        """
        Initialize client comms and start a listener thread.
        """
        try:
            self._create_connection()

            ###
            # start the listener thread
            ###
            self.listener_thread = Listener(self.sock, self.callback, self.error_callback,
                                            self.heartbeat, self.max_missed_heartbeats)
            self.listener_thread.start()
            self.send_get_state()
            self.send_get_config()
        except socket.error as e:
            raise InstrumentConnectionException('Unable to connect (%r)', e)

    def stop_comms(self):
        """
        Stop the listener thread if there is one, and close client comms
        with the device logger. This is called by the done function.
        """
        log.info('PortAgentClient shutting down comms.')
        if self.listener_thread:
            self.listener_thread._done = True
            self.listener_thread.join()

        self._destroy_connection()
        log.info('Port Agent Client stopped.')

    def send_break(self, duration):
        """
        Command the port agent to send a break
        :param duration:
        """
        self._command_port_agent(self.BREAK_COMMAND + str(duration))

    def send_get_config(self):
        """
        Command the port agent to send a break
        """
        self._command_port_agent(self.GET_CONFIG_COMMAND)

    def send_get_state(self):
        """
        Command the port agent to return its current state
        :return:
        """
        self._command_port_agent(self.GET_STATE_COMMAND)

    def send(self, data, sock=None, host=None, port=None):
        """
        Send data to the port agent.
        :param data:
        :param sock:
        :param host:
        :param port:
        """
        total_bytes_sent = 0

        # The socket can be a parameter (in case we need to send to the command
        # port, for instance); if not provided, default to self.sock which
        # should be the data port.  The same pattern applies to the host and port,
        # but those are for logging information in case of error.
        sock = sock if sock else self.sock
        host = host if host else self.host
        port = port if port else self.port

        if sock:
            would_block_tries = 0
            continuing = True
            while len(data) > 0 and continuing:
                try:
                    sent = sock.send(data)
                    total_bytes_sent = len(data[:sent])
                    data = data[sent:]
                except socket.error as e:
                    if e.errno == errno.EWOULDBLOCK:
                        would_block_tries += 1
                        if would_block_tries > self.send_attempts:
                            error_string = 'Send EWOULDBLOCK attempts (%d) exceeded while sending to %r'
                            log.error(error_string, would_block_tries, sock.getpeername())
                            continuing = False
                            self._error()
                        else:
                            error_string = 'Socket error while sending to %r: %r; tries = %d'
                            log.error(error_string, sock.getpeername(), e, would_block_tries)
                            time.sleep(.1)
                    else:
                        error_string = 'Socket error while sending to (%r:%r): %r' % (host, port, e)
                        log.error(error_string)
                        self._error()
        else:
            error_string = 'No socket defined!'
            log.error(error_string)
            self._error()

        return total_bytes_sent

    def _error(self):
        self.stop_comms()
        self.error_callback()

    def _create_sock(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(0)
        return sock

    def _create_connection(self):
        self._destroy_connection()
        self.sock = self._create_sock(self.host, self.port)

    def _destroy_connection(self):
        if self.sock:
            self.sock.close()
            self.sock = None
            log.info('Port agent data socket closed.')

    def _command_port_agent(self, cmd):
        """
        Command the port agent.  We connect to the command port, send the command
        and then disconnect.  Connection is not persistent
        @raise InstrumentConnectionException if cmd_port is missing.  We don't
                        currently do this on init  where is should happen because
                        some instruments wont set the  command port quite yet.
        """
        if not cmd.endswith(NEWLINE):
            cmd += NEWLINE
        try:
            if not self.cmd_port:
                raise InstrumentConnectionException("Missing port agent command port config")
            sock = self._create_sock(self.host, self.cmd_port)
            log.info('_command_port_agent(): connected to port agent at %s:%i.', self.host, self.cmd_port)
            self.send(cmd, sock)
            sock.close()
        except Exception as e:
            log.error("_command_port_agent(): Exception occurred.", exc_info=True)
            raise InstrumentConnectionException('Failed to connect to port agent command port at %s:%s (%s).'
                                                % (self.host, self.cmd_port, e))


class Listener(threading.Thread):
    """
    A listener thread to monitor the client socket data incoming from
    the port agent process.
    """
    MAX_HEARTBEAT_INTERVAL = 20  # Max, for range checking parameter
    MAX_MISSED_HEARTBEATS = 5  # Max number we can miss
    HEARTBEAT_FUDGE = 1  # Fudge factor to account for delayed heartbeat

    def __init__(self, sock, callback, error_callback, heartbeat, max_missed_heartbeats):
        """
        Listener thread constructor.
        @param sock The socket to listen on.
        @param callback The callback on data arrival.
        @param error_callback The callback on error
        @param heartbeat The heartbeat interval in which to expect heartbeat messages from the Port Agent.
        @param max_missed_heartbeats The number of allowable missed heartbeats before attempting recovery.
        """
        threading.Thread.__init__(self)
        self.sock = sock
        self._done = False
        self.heartbeat_timer = None
        self.thread_name = None
        self.max_missed_heartbeats = max_missed_heartbeats if max_missed_heartbeats else self.MAX_MISSED_HEARTBEATS
        self.heartbeat_missed_count = self.max_missed_heartbeats
        self.heartbeat = min(heartbeat + self.HEARTBEAT_FUDGE, self.MAX_HEARTBEAT_INTERVAL)
        self.callback = callback
        self.error_callback = error_callback

    def heartbeat_timeout(self):
        self.heartbeat_missed_count -= 1
        log.error('HEARTBEAT timeout: %d remaining', self.heartbeat_missed_count)

        # Take corrective action here.
        if self.heartbeat_missed_count <= 0:
            error_string = 'Maximum allowable Port Agent heartbeats (%d) missed!'
            log.error(error_string, self.max_missed_heartbeats)
            self.error()
        else:
            self.start_heartbeat_timer()

    def error(self):
        self._done = True
        self.error_callback()

    def start_heartbeat_timer(self):
        """
        Note: the threading timer here is only run once.  The cancel
        only applies if the function has yet run.  You can't reset
        it and start it again, you have to instantiate a new one.
        I don't like this; we need to implement a tread timer that
        stays up and can be reset and started many times.
        """
        if not self._done:
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()

            self.heartbeat_timer = threading.Timer(self.heartbeat,
                                                   self.heartbeat_timeout)
            self.heartbeat_timer.start()

    def handle_packet(self, pa_packet):
        packet_type = pa_packet.get_header_type()
        if packet_type == PortAgentPacket.HEARTBEAT:
            # Got a heartbeat; reset the timer and re-init
            # heartbeat_missed_count.
            log.debug("HEARTBEAT Packet Received")
            if 0 < self.heartbeat:
                self.start_heartbeat_timer()
            self.heartbeat_missed_count = self.max_missed_heartbeats

        else:
            self.callback(pa_packet)

    def _receive_n_bytes(self, count):
        data_buffer = bytearray(count)
        data_view = memoryview(data_buffer)
        bytes_left = count
        while bytes_left and not self._done:
            try:
                bytes_rx = self.sock.recv_into(data_view[-bytes_left:], bytes_left)
                log.trace('RX BYTES %d LEFT %d SOCK %r', bytes_rx, bytes_left, self.sock)
                if bytes_rx <= 0:
                    raise SocketClosed()
                bytes_left -= bytes_rx
            except socket.error as e:
                if e.errno == errno.EWOULDBLOCK:
                    time.sleep(.1)
                else:
                    raise

        if self._done:
            raise Done()

        return str(data_buffer)

    def run(self):
        """
        Listener thread processing loop. Block on receive from port agent.
        Receive HEADER_SIZE bytes to receive the entire header.  From that,
        get the length of the whole packet (including header); compute the
        length of the remaining data and read that.
        """
        self.thread_name = threading.current_thread().name
        log.info('PortAgentClient listener thread: %s started.', self.thread_name)

        if self.heartbeat:
            self.start_heartbeat_timer()

        while not self._done:
            try:
                header = self._receive_n_bytes(HEADER_SIZE)
                pa_packet = PortAgentPacket()
                pa_packet.unpack_header(header)
                data_size = pa_packet.get_data_length()
                data = self._receive_n_bytes(data_size)
                # Should have complete port agent packet.
                pa_packet.attach_data(data)
                self.handle_packet(pa_packet)

            except Done:
                pass

            except (SocketClosed, socket.error) as e:
                error_string = 'Listener: %s Socket error while receiving from port agent: %r' % (self.thread_name, e)
                log.error(error_string)
                self.error()

            except Exception as e:
                if not isinstance(e, InstrumentException):
                    e = InstrumentException(e.message)

                log.error(e.get_triple())
                self.callback(e)

        log.info('Port_agent_client thread done listening; going away.')
