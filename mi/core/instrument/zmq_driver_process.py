#!/usr/bin/env python

"""
@package ion.services.mi.zmq_driver_process
@file ion/services/mi/zmq_driver_process.py
@author Edward Hunter
@brief Driver processes using ZMQ messaging.
"""
import Queue
import os
import threading
import time
import json
import sys

import zmq
from mi.core.common import BaseEnum

import mi.core.exceptions
import mi.core.instrument.driver_process as driver_process
from mi.core.log import get_logger


log = get_logger()

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


class DirectCommands(BaseEnum):
    STOP_DRIVER = 'stop_driver_process'
    TEST_EVENTS = 'test_events'
    PING = 'process_echo'
    OVERALL_STATE = 'overall_state'


class Events(BaseEnum):
    DRIVER_SYNC_EVENT = 'DRIVER_SYNCHRONOUS_EVENT_REPLY'
    DRIVER_ASYNC_EVENT = 'DRIVER_AYSNC_EVENT_REPLY'
    DRIVER_ASYNC_FUTURE = 'DRIVER_ASYNC_EVENT_FUTURE'
    DRIVER_BUSY = 'DRIVER_BUSY_EVENT'
    DRIVER_EXCEPTION = 'DRIVER_EXCEPTION_EVENT'


class EventKeys(BaseEnum):
    TIME = 'time'
    TYPE = 'type'
    VALUE = 'value'
    COMMAND = 'cmd'
    ARGS = 'args'
    KWARGS = 'kwargs'
    TRANSID = 'transaction_id'


class ZmqDriverProcess(driver_process.DriverProcess):
    """
    A OS-level driver process that communicates with ZMQ sockets.
    Command-REP and event-PUB sockets monitor and react to comms
    needs in separate threads, which can be signaled to end
    by setting boolean flags stop_cmd_thread and stop_evt_thread.
    """

    def __init__(self, driver_module, driver_class, command_port, event_port):
        """
        Zmq driver process constructor.
        """
        driver_process.DriverProcess.__init__(self, driver_module, driver_class, 0)
        self.cmd_host_string = 'tcp://*:%d' % command_port
        self.event_host_string = 'tcp://*:%d' % event_port
        self.evt_thread = None
        self.stop_evt_thread = True
        self.cmd_thread = None
        self.stop_cmd_thread = True
        self.busy = 0
        self.transaction_id = 0

    @staticmethod
    def deunicode(msg):
        if isinstance(msg, list):
            return [ZmqDriverProcess.deunicode(x) for x in msg]
        if isinstance(msg, dict):
            return {k: ZmqDriverProcess.deunicode(v) for k, v in msg.iteritems()}
        if isinstance(msg, unicode):
            return str(msg)
        else:
            return msg

    @staticmethod
    def _encode_exception(exception):
        if isinstance(exception, mi.core.exceptions.InstrumentException):
            # InstrumentExceptions have corresponding IonException error code built-in
            return exception.get_triple()
        else:
            # all others are wrapped to capture stack and appropriate code
            ex = mi.core.exceptions.UnexpectedError("%s('%s')" % (exception.__class__.__name__, exception.message))
            return ex.get_triple()

    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. This ZMQ implementation starts and
        joins command and event threads, managing nonblocking send/recv calls
        on REP and PUB sockets, respectively. Terminate loops and close
        sockets when stop flag is set in driver process.
        """
        self.cmd_thread = threading.Thread(target=self.recv_cmd_msg)
        self.evt_thread = threading.Thread(target=self.send_evt_msg)
        self.cmd_thread.start()
        self.evt_thread.start()
        self.messaging_started = True

    def send_command(self, command, args, kwargs):
        self.transaction_id += 1
        if self.transaction_id > sys.maxint:
            self.transaction_id = 1
        if self.busy:
            return Events.DRIVER_BUSY
        self.busy = self.transaction_id

        if args is None:
            args = []
        if type(args) not in [list, tuple]:
            args = (args,)

        def inner():
            reply = ''
            had_exception = False
            try:
                cmd_func = getattr(self.driver, command, None)

                if cmd_func and callable(cmd_func):
                    reply = cmd_func(*args, **kwargs)
                else:
                    reply = self._encode_exception(
                        mi.core.exceptions.InstrumentCommandException('Unknown driver command.'))
                    had_exception = True
            except Exception as e:
                reply = self._encode_exception(e)
                had_exception = True
            finally:
                self.busy = 0
                e = {
                    EventKeys.TYPE: Events.DRIVER_ASYNC_EVENT,
                    EventKeys.COMMAND: {
                        EventKeys.COMMAND: command,
                        EventKeys.ARGS: args,
                        EventKeys.KWARGS: kwargs
                    },
                    EventKeys.VALUE: reply,
                    EventKeys.TRANSID: self.transaction_id,
                    EventKeys.TIME: time.time()
                }
                if had_exception:
                    e[EventKeys.TYPE] = Events.DRIVER_EXCEPTION
                self.events.put(e)

        t = threading.Thread(target=inner)
        t.start()

        return self.transaction_id

    def cmd_driver(self, msg):
        """
        This method should NEVER throw an exception, as this will break the event loop
        """
        log.info('command: %s', msg)

        # some parts of the driver call isinstance(<var>, str)
        # so we need to convert unicode values back to ascii
        msg = self.deunicode(msg)

        command = msg.get(EventKeys.COMMAND, '')
        args = msg.get(EventKeys.ARGS, ())
        kwargs = msg.get(EventKeys.KWARGS, {})

        reply = {
            EventKeys.TIME: time.time(),
            EventKeys.COMMAND: {
                EventKeys.COMMAND: command,
                EventKeys.ARGS: args,
                EventKeys.KWARGS: kwargs
            }
        }

        try:
            if command == '':
                raise mi.core.exceptions.InstrumentCommandException('No command received')

            if DirectCommands.has(command):
                reply[EventKeys.TYPE] = Events.DRIVER_SYNC_EVENT

                if command == DirectCommands.STOP_DRIVER:
                    self.stop_messaging()
                    reply[EventKeys.VALUE] = 'Stopped driver process'
                elif command == DirectCommands.TEST_EVENTS:
                    events = kwargs['events']
                    if type(events) == list:
                        for e in events:
                            self.events.put(e)
                    else:
                        self.events.put(events)
                    reply[EventKeys.VALUE] = 'Enqueued test events'
                elif command == DirectCommands.PING:
                    reply[EventKeys.VALUE] = 'ping from wrapper pid:%s, resource:%s' % (os.getpid(), self.driver)
                elif command == DirectCommands.OVERALL_STATE:
                    reply[EventKeys.VALUE] = {'capabilities': self.driver.get_resource_capabilities(),
                                              'state': self.driver.get_resource_state(),
                                              'metadata': self.driver.get_config_metadata(),
                                              'parameters': self.driver.get_cached_config(),
                                              'init_params': self.driver.get_init_params(),
                                              'busy': self.busy}
            else:
                reply[EventKeys.VALUE] = self.send_command(command, args, kwargs)
                if reply[EventKeys.VALUE] == Events.DRIVER_BUSY:
                    reply[EventKeys.TYPE] = reply[EventKeys.VALUE]
                else:
                    reply[EventKeys.TYPE] = Events.DRIVER_ASYNC_FUTURE

            # TODO still necessary?
            if type(reply[EventKeys.VALUE]) == str:
                # noinspection PyBroadException
                try:
                    reply[EventKeys.VALUE] = json.loads(reply)
                except:
                    pass

        except Exception as e:
            try:
                reply[EventKeys.VALUE] = self._encode_exception(e)

            except Exception as e1:
                reply[EventKeys.VALUE] = e1.message

        return reply

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Set flags to cause
        command and event threads to close sockets and conclude.
        """
        self.stop_cmd_thread = True
        self.stop_evt_thread = True
        self.messaging_started = False

    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        driver_process.DriverProcess.shutdown(self)

    def recv_cmd_msg(self):
        """
        Await commands on a ZMQ REP socket, forwarding them to the
        driver for processing and returning the result.
        """
        context = zmq.Context()
        sock = context.socket(zmq.REP)
        sock.bind(self.cmd_host_string)

        self.stop_cmd_thread = False
        while not self.stop_cmd_thread:
            try:
                msg = sock.recv_json()
                reply = self.cmd_driver(msg)

                log.debug("Reply from driver: %r %s", reply, type(reply))

                if isinstance(reply, Exception):
                    reply = self._encode_exception(reply)

                sock.send_json(reply, zmq.NOBLOCK)

            except zmq.ZMQError as e:
                log.info("ZMQ error: %s", e)
                time.sleep(.1)
            except Exception as e:
                log.info('Exception in command loop: %s', e.message)
                sock.send_json(self._encode_exception(e))

        sock.close()
        context.term()
        log.info('Driver process cmd socket closed.')

    def send_evt_msg(self):
        """
        Await events on the driver process event queue and publish them
        on a ZMQ PUB socket to the driver process client.
        """
        context = zmq.Context()
        sock = context.socket(zmq.PUB)
        sock.bind(self.event_host_string)

        self.stop_evt_thread = False
        while not self.stop_evt_thread:
            try:
                evt = self.events.get_nowait()
                while evt:
                    try:
                        if isinstance(evt, Exception):
                            evt = self._encode_exception(evt)
                        sock.send_json(evt, flags=zmq.NOBLOCK)
                        evt = None
                        log.trace('Event sent!')
                    except zmq.ZMQError:
                        time.sleep(.1)
                        if self.stop_evt_thread:
                            break
            except Queue.Empty:
                time.sleep(.1)

        sock.close()
        context.term()
        log.info('Driver process event socket closed')
