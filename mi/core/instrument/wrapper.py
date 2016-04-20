#!/usr/bin/env python
"""
@package mi.core.instrument.wrapper
@file mi/core/instrument/wrapper.py
@author Peter Cable
@brief Driver process using ZMQ messaging.

Usage:
    run_driver <module> <driver_class> <refdes> <event_url> <particle_url>
    run_driver <module> <driver_class> <refdes> <event_url> <particle_url> <config_file>

Options:
    -h, --help          Show this screen.

"""
import consulate
import importlib
import json
import os
import Queue
import signal
import threading
import time
import traceback

import yaml
import zmq

from docopt import docopt
from logging import _levelNames
from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedError, InstrumentCommandException, InstrumentException
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.publisher import Publisher

from mi.core.log import get_logger, get_logging_metaclass

log = get_logger()

META_LOGGER = get_logging_metaclass('trace')
PUBLISH_INTERVAL = 5
MAX_NUM_EVENTS = 1000

__author__ = 'Peter Cable'
__license__ = 'Apache 2.0'


class Commands(BaseEnum):
    STOP_DRIVER = 'stop_driver_process'
    TEST_EVENTS = 'test_events'
    PING = 'process_echo'
    OVERALL_STATE = 'overall_state'
    STOP_WORKER = 'stop_worker'
    DEFAULT = 'default'
    SET_LOG_LEVEL = 'set_log_level'


class EventKeys(BaseEnum):
    TIME = 'time'
    TYPE = 'type'
    VALUE = 'value'
    COMMAND = 'cmd'
    ARGS = 'args'
    KWARGS = 'kwargs'


# max seconds between interrupts before killing driver
INTERRUPT_REPEAT_INTERVAL = 3

# semaphore to prevent multiple simultaneous commands into the driver
COMMAND_SEM = threading.BoundedSemaphore(1)


def encode_exception(exception):
    if not isinstance(exception, InstrumentException):
        exception = UnexpectedError("%s('%s')" % (exception.__class__.__name__, exception.message))
    return exception.get_triple()


def _decode(data):
    if isinstance(data, (list, tuple)):
        return [_decode(x) for x in data]
    if isinstance(data, dict):
        return {_decode(k): _decode(v) for k, v in data.iteritems()}
    if isinstance(data, basestring):
        return data.decode('utf-8', 'ignore')
    return data


def build_event(event_type, value, command=None, args=None, kwargs=None):
    event = {
        EventKeys.TIME: time.time(),
        EventKeys.TYPE: event_type,
        EventKeys.VALUE: value
    }

    if any((command, args, kwargs)):
        event[EventKeys.COMMAND] = {
            EventKeys.COMMAND: command,
            EventKeys.ARGS: args,
            EventKeys.KWARGS: kwargs
        }

    return event


class StatusThread(threading.Thread):
    def __init__(self, wrapper, ttl=120):
        super(StatusThread, self).__init__()
        self.wrapper = wrapper
        self.consul = consulate.Consul()
        self.ttl = ttl
        # sleep for 1/2 ttl, so 2 missed polls will degrade the state
        self.sleep_time = ttl / 2.0
        self.running = True

    def run(self):
        refdes = self.wrapper.refdes
        service = 'instrument_driver'
        service_id = '%s_%s' % (service, refdes)
        check = 'service:%s' % service_id
        self.consul.agent.service.register('instrument_driver', service_id=service_id,
                                           port=self.wrapper.port, tags=[refdes],
                                           ttl='%ds' % self.ttl)

        while self.running:
            self.consul.agent.check.ttl_pass(check)
            time.sleep(self.sleep_time)


class CommandHandler(threading.Thread):
    def __init__(self, wrapper, worker_url):
        super(CommandHandler, self).__init__()
        self.wrapper = wrapper
        self.driver = wrapper.driver
        self.events = wrapper.events
        self.worker_url = worker_url
        self._stop = False

        self._routes = {
            Commands.SET_LOG_LEVEL: self._set_log_level,
            Commands.OVERALL_STATE: self._overall_state,
            Commands.PING: self._ping,
            Commands.TEST_EVENTS: self._test_events,
            Commands.STOP_DRIVER: self._stop_driver,
            Commands.STOP_WORKER: self._stop_worker,
        }

    def _execute(self, command, args, kwargs):

        _func = self._routes.get(command, self._send_command)
        if not isinstance(args, (list, tuple)):
            args = (args,)

        try:
            reply = _func(command, *args, **kwargs)
            event_type = DriverAsyncEvent.RESULT
        except Exception as e:
            log.error('Exception in command handler: %r', e)
            reply = encode_exception(e)
            event_type = DriverAsyncEvent.ERROR

        event = build_event(event_type, reply, command, args, kwargs)
        log.trace('CommandHandler generated event: %r', event)
        return event

    def _set_log_level(self, *args, **kwargs):
        level_name = kwargs.get('level')
        level = None

        if isinstance(level_name, int):
            if level_name in _levelNames:
                level = level_name
        elif isinstance(level_name, basestring):
            level_name = level_name.upper()
            level = _levelNames.get(level_name)

        if level is None:
            raise UnexpectedError('Invalid logging level supplied')

        log.setLevel(level)
        return 'Set logging level to %s' % level

    def _test_events(self, *args, **kwargs):
        events = kwargs['events']
        if type(events) not in (list, tuple):
            events = [events]
        for e in events:
            self.events.put(e)

        return 'Enqueued test events'

    def _stop_driver(self, *args, **kwargs):
        self.wrapper.stop_messaging()
        return 'Stopped driver process'

    def _stop_worker(self, *args, **kwargs):
        self._stop = True
        return 'Stopping worker thread'

    def _ping(self, *args, **kwargs):
        return 'ping from wrapper pid:%s, resource:%s' % (os.getpid(), self.driver)

    def _overall_state(self, *args, **kwargs):
        return {'capabilities': self.driver.get_resource_capabilities(),
                'state': self.driver.get_resource_state(),
                'metadata': self.driver.get_config_metadata(),
                'parameters': self.driver.get_cached_config(),
                'direct_config': self.driver.get_direct_config(),
                'init_params': self.driver.get_init_params()}

    def _send_command(self, command, *args, **kwargs):
        if not COMMAND_SEM.acquire(False):
            return 'BUSY'

        try:
            cmd_func = getattr(self.driver, command, None)

            if cmd_func and callable(cmd_func):
                reply = cmd_func(*args, **kwargs)
            else:
                raise InstrumentCommandException('Unknown driver command.')

            return reply

        finally:
            COMMAND_SEM.release()

    def cmd_driver(self, msg):
        """
        This method should NEVER throw an exception, as this will break the event loop
        """
        log.debug('executing command: %s', msg)
        command = msg.get(EventKeys.COMMAND, '')
        args = msg.get(EventKeys.ARGS, ())
        kwargs = msg.get(EventKeys.KWARGS, {})

        return self._execute(command, args, kwargs)

    def run(self):
        """
        Await commands on a ZMQ REP socket, forwarding them to the
        driver for processing and returning the result.
        """
        context = zmq.Context.instance()
        sock = context.socket(zmq.REQ)
        sock.connect(self.worker_url)
        sock.send('READY')
        address = None

        while not self._stop:
            try:
                address, _, request = sock.recv_multipart()
                msg = json.loads(request)
                log.info('received message: %r', msg)
                reply = _decode(self.cmd_driver(msg))
                sock.send_multipart([address, '', json.dumps(reply)])
            except zmq.ContextTerminated:
                log.info('ZMQ Context terminated, exiting worker thread')
                break
            except zmq.ZMQError:
                # If we have an error on the socket we'll need to restart it
                sock = context.socket(zmq.REQ)
                sock.connect(self.worker_url)
                sock.send('READY')
            except Exception as e:
                log.error('Exception in command loop: %r', e)
                if address is not None:
                    event = build_event(DriverAsyncEvent.ERROR, repr(e))
                    sock.send_multipart([address, '', event])

        sock.close()


class LoadBalancer(object):
    """
    The load balancer creates two router connections.
    Workers and clients create REQ sockets to connect. A worker will
    send 'READY' upon initialization and subsequent "requests" will be
    the results from the previous command.
    """

    def __init__(self, wrapper, num_workers, worker_url='inproc://workers'):
        self.wrapper = wrapper
        self.num_workers = num_workers
        self.worker_url = worker_url
        self.context = zmq.Context.instance()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.port = self.frontend.bind_to_random_port('tcp://*')
        self.backend.bind(worker_url)
        self._start_workers()
        self.running = True

    def run(self):
        workers = []
        poller = zmq.Poller()

        poller.register(self.backend, zmq.POLLIN)
        while self.running:
            try:
                sockets = dict(poller.poll(100))

                if self.backend in sockets:
                    request = self.backend.recv_multipart()
                    worker, _, client = request[:3]
                    if not workers:
                        poller.register(self.frontend, zmq.POLLIN)

                    workers.append(worker)
                    if client != 'READY' and len(request) > 3:
                        _, reply = request[3:]
                        self.frontend.send_multipart([client, '', reply])

                if self.frontend in sockets:
                    client, _, request = self.frontend.recv_multipart()
                    worker = workers.pop(0)

                    self.backend.send_multipart([worker, '', client, '', request])
                    if not workers:
                        poller.unregister(self.frontend)

            except zmq.ContextTerminated:
                log.info('ZMQ Context terminated, exiting load balancer loop')
                break

        log.info('Load balancer done')

    def _start_workers(self):
        for _ in xrange(self.num_workers):
            t = CommandHandler(self.wrapper, self.worker_url)
            t.setDaemon(True)
            t.start()


class DriverWrapper(object):
    """
    Base class for messaging enabled OS-level driver processes. Provides
    run loop, dynamic driver import and construction and interface
    for messaging implementation subclasses.
    """
    __metaclass__ = META_LOGGER
    worker_url = "inproc://workers"
    num_workers = 5

    def __init__(self, driver_module, driver_class, refdes, event_url, particle_url, init_params):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.refdes = refdes
        self.event_url = event_url
        self.particle_url = particle_url
        self.driver = None
        self.events = Queue.Queue()
        self.messaging_started = False
        self.int_time = 0
        self.port = None
        self.init_params = init_params

        self.load_balancer = None
        self.status_thread = None
        self.running = False
        self.particle_count = 0

        headers = {'sensor': self.refdes, 'deliveryType': 'streamed'}
        self.event_publisher = Publisher.from_url(self.event_url, headers)
        self.particle_publisher = Publisher.from_url(self.particle_url, headers)

    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        module = importlib.import_module(self.driver_module)
        driver_class = getattr(module, self.driver_class)
        self.driver = driver_class(self.send_event, self.refdes)
        self.driver.set_init_params(self.init_params)
        log.info('Imported and created driver from module: %r class: %r driver: %r refdes: %r',
                 module, driver_class, self.driver, self.refdes)
        return True

    def send_event(self, evt):
        """
        Append an event to the list to be sent by the event thread.
        """
        self.events.put(evt)

        if isinstance(evt[EventKeys.VALUE], Exception):
            evt[EventKeys.VALUE] = encode_exception(evt[EventKeys.VALUE])

        if evt[EventKeys.TYPE] == DriverAsyncEvent.ERROR:
            log.error(evt)

        if evt[EventKeys.TYPE] == DriverAsyncEvent.SAMPLE:
            if evt[EventKeys.VALUE].get('stream_name') == 'raw':
                # don't publish raw
                return

            self.particle_publisher.enqueue(evt)
        else:
            self.event_publisher.enqueue(evt)

    def run(self):
        """
        Process entry point. Construct driver and start messaging loops.
        Periodically check messaging is going and parent exists if
        specified.
        """
        log.info('Driver process started.')

        # noinspection PyUnusedLocal
        def shand(signum, frame):
            self.stop_messaging()

        signal.signal(signal.SIGINT, shand)

        if self.driver is not None or self.construct_driver():
            self.start_threads()
            while self.running:
                time.sleep(1)

        os._exit(0)

    def start_threads(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. This ZMQ implementation starts and
        joins command and event threads, managing nonblocking send/recv calls
        on REP and PUB sockets, respectively. Terminate loops and close
        sockets when stop flag is set in driver process.
        """
        self.running = True

        self.event_publisher.start()
        self.particle_publisher.start()

        self.load_balancer = LoadBalancer(self, self.num_workers)
        self.port = self.load_balancer.port

        # now that we have a port, start our status thread
        self.status_thread = StatusThread(self)
        self.status_thread.start()

        self.load_balancer.run()

        self.event_publisher.enqueue(build_event(DriverAsyncEvent.DRIVER_CONFIG,
                                                 'started on port %d' % self.port))

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Set flags to cause
        command and event threads to close sockets and conclude.
        """
        self.event_publisher.stop()
        self.particle_publisher.stop()

        if self.status_thread:
            self.status_thread.running = False
        if self.load_balancer:
            self.load_balancer.running = False

        self.running = False

        zmq.Context.instance().term()


def main():
    options = docopt(__doc__)

    module = options['<module>']
    event_url = options['<event_url>']
    particle_url = options['<particle_url>']
    klass = options.get('<driver_class>')
    refdes = options['<refdes>']
    config_file = options['<config_file>']

    if config_file is not None:
        init_params = yaml.load(open(config_file))
    else:
        init_params = {}

    wrapper = DriverWrapper(module, klass, refdes, event_url, particle_url, init_params)
    wrapper.run()


if __name__ == '__main__':
    main()
