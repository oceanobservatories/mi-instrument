import json
import consul
import time

from threading import Thread
from collections import MutableMapping
from requests import ConnectionError

from mi.core.exceptions import InstrumentParameterException
from ooi.logging import log


DRIVER_SERVICE_NAME = 'instrument_driver'
DRIVER_SERVICE_TTL = 60
CONSUL = consul.Consul()


class ConsulServiceRegistry(object):
    @staticmethod
    def register_driver(reference_designator, port):
        service_id = '%s_%s' % (DRIVER_SERVICE_NAME, reference_designator)
        CONSUL.agent.service.register(DRIVER_SERVICE_NAME, service_id=service_id,
                                      port=port, tags=[reference_designator],
                                      check=consul.Check.ttl('%ds' % DRIVER_SERVICE_TTL))

    @staticmethod
    def locate_port_agent(reference_designator):
        try:
            _, data_port = CONSUL.health.service('port-agent', passing=True, tag=reference_designator)
            _, cmd_port = CONSUL.health.service('command-port-agent', passing=True, tag=reference_designator)

            if data_port and cmd_port:
                port = data_port[0]['Service']['Port']
                addr = data_port[0]['Node']['Address']
                cmd_port = cmd_port[0]['Service']['Port']
                port_agent_config = {'port': port, 'cmd_port': cmd_port, 'addr': addr}
                return port_agent_config
        except ConnectionError:
            return None

    @staticmethod
    def create_health_thread(reference_designator, port):
        return ConsulServiceRegistry.ConsulHealthThread(reference_designator, port)

    class ConsulHealthThread(Thread):
        def __init__(self, reference_designator, port):
            super(ConsulServiceRegistry.ConsulHealthThread, self).__init__()

            self.reference_designator = reference_designator
            self.port = port
            self.service_id = '%s_%s' % (DRIVER_SERVICE_NAME, reference_designator)
            self.check_id = 'service:%s' % self.service_id
            self.running = False
            self.registered = False

        def run(self):
            self.running = True

            while self.running and not self.registered:
                try:
                    ConsulServiceRegistry.register_driver(self.reference_designator, self.port)
                    self.registered = True
                except ConnectionError:
                    log.error('Unable to register with Consul, will attempt again in %d secs', DRIVER_SERVICE_TTL / 2)
                    time.sleep(DRIVER_SERVICE_TTL / 2)

            while self.running:
                CONSUL.agent.check.ttl_pass(self.check_id)
                time.sleep(DRIVER_SERVICE_TTL / 2)

        def stop(self):
            self.running = False


class ConsulPersistentStore(MutableMapping):
    def __init__(self, reference_designator, prefix='instrument_driver/persist'):
        self.refdes = reference_designator
        self.prefix = prefix

    def __getitem__(self, key):
        return self._get_one(key)

    def __iter__(self):
        return self._get_iter()

    def __delitem__(self, key):
        self._delete(key)

    def __setitem__(self, key, value):
        self._put(key, value)

    def __len__(self):
        return len(list(self._get_iter()))

    def __repr__(self):
        return str(dict(self.iteritems()))

    def _make_key(self, key=None):
        if key is None:
            return '/'.join((self.refdes, self.prefix))

        if not isinstance(key, basestring):
            raise InstrumentParameterException('Persistent store keys MUST be strings')
        return '/'.join((self.refdes, self.prefix, key))

    def _put(self, key, value):
        try:
            my_key = self._make_key(key)
            json_value = json.dumps(value)
            CONSUL.kv.put(my_key, json_value)
        except ConnectionError:
            raise InstrumentParameterException('Unable to connect to Consul')

    def _delete(self, key):
        try:
            my_key = self._make_key(key)
            # fetch first, as consul will not raise KeyError
            # when deleting a KV that does not exist.
            self._get_one(key)
            CONSUL.kv.delete(my_key)
        except ConnectionError:
            raise InstrumentParameterException('Unable to connect to Consul')

    def _get_one(self, key):
        key = self._make_key(key)
        try:
            _, value = CONSUL.kv.get(key)
            if value is None:
                raise KeyError
            return json.loads(value['Value'])
        except ConnectionError:
            raise InstrumentParameterException('Unable to connect to Consul')

    def _get_iter(self):
        key = self._make_key()
        try:
            _, value = CONSUL.kv.get(key, recurse=True)
            if value is None:
                return iter(())
            return (each['Key'].split(key, 1)[1][1:] for each in value)
        except ConnectionError:
            raise InstrumentParameterException('Unable to connect to Consul')

