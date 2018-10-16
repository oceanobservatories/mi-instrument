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
import base64

import importlib
import json
import os
import signal
import threading
import time

import yaml
import zmq

from docopt import docopt
from logging import _levelNames
from mi.core.common import BaseEnum
from mi.core.exceptions import UnexpectedError, InstrumentCommandException, InstrumentException
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.publisher import Publisher
from mi.core.log import get_logger, get_logging_metaclass
from mi.core.service_registry import ConsulServiceRegistry

log = get_logger()

META_LOGGER = get_logging_metaclass('trace')

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


def _transform(value):
    flag = '_base64:'
    if isinstance(value, basestring):
        if value.startswith(flag):
            data = value.split(flag, 1)[1]
            return base64.b64decode(data)
        return value

    elif isinstance(value, (list, tuple)):
        return [_transform(x) for x in value]

    elif isinstance(value, dict):
        return {k: _transform(value[k]) for k in value}

    return value


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


class CommandHandler(threading.Thread):
    def __init__(self, wrapper, worker_url):
        super(CommandHandler, self).__init__()
        self.wrapper = wrapper
        self.driver = wrapper.driver
        self.send_event = wrapper.send_event
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

    def _execute(self, raw_command, raw_args, raw_kwargs):
        # check for b64 encoded values
        # decode them prior to processing this command
        command = _transform(raw_command)
        args = _transform(raw_args)
        kwargs = _transform(raw_kwargs)

        # lookup the function to be executed
        _func = self._routes.get(command, self._send_command)
        # ensure args is iterable
        if not isinstance(args, (list, tuple)):
            args = (args,)

        # Attempt to execute this command
        try:
            reply = _func(command, *args, **kwargs)
            event_type = DriverAsyncEvent.RESULT
        except Exception as e:
            log.error('Exception in command handler: %r', e)
            reply = encode_exception(e)
            event_type = DriverAsyncEvent.ERROR

        # Build the response event. Use the raw values, if something was
        # base64 encoded, we may not be able to send the decoded value back raw
        event = build_event(event_type, reply, raw_command, raw_args, raw_kwargs)
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
            self.send_event(e)

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
        direct_config = {}
        if hasattr(self.driver, 'get_direct_config'):
            direct_config = self.driver.get_direct_config()
        return {'capabilities': self.driver.get_resource_capabilities(),
                'state': self.driver.get_resource_state(),
                'metadata': self.driver.get_config_metadata(),
                'parameters': self.driver.get_cached_config(),
                'direct_config': direct_config,
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

    def _start_workers(self):
        for _ in xrange(self.num_workers):
            t = CommandHandler(self.wrapper, self.worker_url)
            t.setDaemon(True)
            t.start()

    def stop(self):
        self.running = False


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
        self.messaging_started = False
        self.int_time = 0
        self.port = None
        self.init_params = init_params

        self.load_balancer = None
        self.status_thread = None
        self.particle_count = 0
        self.version = self.get_version(driver_module)

        headers = {'sensor': self.refdes, 'deliveryType': 'streamed', 'version': self.version, 'module': driver_module}
        log.info('Publish headers set to: %r', headers)
        self.event_publisher = Publisher.from_url(self.event_url, headers=headers)
        self.particle_publisher = Publisher.from_url(self.particle_url, headers=headers)

    @staticmethod
    def get_version(driver_module):
        module = importlib.import_module(driver_module)
        dirname = os.path.dirname(module.__file__)
        metadata_file = os.path.join(dirname, 'metadata.yml')
        if os.path.exists(metadata_file):
            metadata = yaml.load(open(metadata_file))
            return metadata.get('driver_metadata', {}).get('version')
        return 'UNVERSIONED'

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

    def start_threads(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. This ZMQ implementation starts and
        joins command and event threads, managing nonblocking send/recv calls
        on REP and PUB sockets, respectively. Terminate loops and close
        sockets when stop flag is set in driver process.
        """
        self.event_publisher.start()
        self.particle_publisher.start()

        self.load_balancer = LoadBalancer(self, self.num_workers)
        self.port = self.load_balancer.port

        # now that we have a port, start our status thread
        self.status_thread = ConsulServiceRegistry.create_health_thread(self.refdes, self.port)
        self.status_thread.setDaemon(True)
        self.status_thread.start()

        self.load_balancer.run()

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Set flags to cause
        command and event threads to close sockets and conclude.
        """
        self.load_balancer.stop()
        self.status_thread.stop()
        self.event_publisher.stop()
        self.particle_publisher.stop()


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
