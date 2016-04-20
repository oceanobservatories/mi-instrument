#!/usr/bin/env python
import datetime
import functools
import time
from threading import Lock
from xmlrpclib import ServerProxy
from pkg_resources import resource_string

import ntplib
import yaml
from concurrent.futures import ThreadPoolExecutor
from toolz import concat, pluck, keyfilter

import mi.platform.rsn
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.publisher import Publisher
from mi.core.log import get_logger
from mi.platform.exceptions import PlatformException
from mi.platform.util.node_configuration import NodeConfiguration

log = get_logger()

__license__ = 'Apache 2.0'
DEFAULT_POOL_SIZE = 5
DEFAULT_STREAM_DEF_FILENAME = 'node_config_files/stream_defs.yml'


class stopwatch(object):
    """
    Easily measure elapsed time
    """
    def __init__(self, label=None, logger=None):
        self.start_time = datetime.datetime.now()
        self.label = label
        self.logger = logger if logger else get_logger().debug

    def __repr__(self):
        stop = datetime.datetime.now()
        r = str(stop - self.start_time)
        # like a lap timer, reset the clock with every repr
        self.start_time = stop
        if self.label:
            return '%s %s' % (self.label, r)
        return r

    def __enter__(self):
        self.logger('enter: %s', self.label)

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        # must explicity build the string here
        # or log will call __repr__ multiple times
        # and log incorrect times to all but the first handler
        self.logger('exit: %r' % self)

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            self.start_time = datetime.datetime.now()
            if self.label is None:
                self.label = 'function: %s' % f.func_name
            with self:
                return f(*args, **kwargs)
        return decorated


class PlatformParticle(DataParticle):
    """
    The contents of the parameter dictionary, published at the start of a scan
    """
    def _build_parsed_values(self):
        return [{DataParticleKey.VALUE_ID: a, DataParticleKey.VALUE: b} for a, b in self.raw_data]


class OmsExtractor(object):
    headers = {'deliveryType': 'streamed'}

    def __init__(self, config):
        self.oms_uri = config.get('oms_uri')
        self.pool_size = config.get('pool_size', DEFAULT_POOL_SIZE)
        self.thread_pool = ThreadPoolExecutor(self.pool_size)
        self.publisher = Publisher.from_url(config.get('publish_uri', 'log://'),
                                            headers=self.headers, max_events=1000, publish_interval=1)
        self.publisher.start()
        self.node_configs = []
        self._get_nodes(config)
        self.times_lock = Lock()
        self._last_times = {}

    @stopwatch(label='fetch_all', logger=log.warn)
    def fetch_all(self):
        log.info('Begin fetching all data')
        ntp_time = ntplib.system_to_ntp_time(time.time())
        max_time = ntp_time - 90

        futures = []
        for nc in self.node_configs:
            with self.times_lock:
                t = max(max_time, self._last_times.get(nc.platform_id))
            proxy = ServerProxy(self.oms_uri)
            futures.append(self.thread_pool.submit(self._fetch, proxy, nc, t))

        for f in futures:
            result = f.result()
            if result is not None:
                for event in result:
                    self.publisher.enqueue(event)

    # INTERNAL METHODS
    def _get_nodes(self, config, stream_definition_filename=DEFAULT_STREAM_DEF_FILENAME):
        stream_config_string = resource_string(mi.platform.rsn.__name__, stream_definition_filename)
        stream_definitions = yaml.load(stream_config_string)
        for node_config_file in config.get('node_config_files', []):
            self.node_configs.append(NodeConfiguration(node_config_file, stream_definitions))

    @staticmethod
    def _group_by_timestamp(attr_dict):
        return_dict = {}
        # go through all of the returned values and get the unique timestamps. Each
        # particle will have data for a unique timestamp
        for attr_id, attr_vals in attr_dict.iteritems():
            for value, timestamp in attr_vals:
                return_dict.setdefault(timestamp, []).append((attr_id, value))

        return return_dict

    @staticmethod
    def _convert_attrs_to_ion(stream, attrs):
        attrs_return = []
        # convert back to ION parameter name and scale from OMS to ION
        for key, v in attrs:
            param = stream[key]
            v = v * param.scale_factor if v else v
            attrs_return.append((param.ion_parameter_name, v))

        return attrs_return

    @staticmethod
    def _fetch_attrs(proxy, platform_id, attrs):
        with stopwatch(label='get_platform_attribute_values: %s' % platform_id, logger=log.info):
            response = proxy.attr.get_platform_attribute_values(platform_id, attrs).get(platform_id, {})

        return_dict = {}
        count = 0
        for key, value_list in response.iteritems():
            if value_list == 'INVALID_ATTRIBUTE_ID':
                continue
            if not isinstance(value_list, list):
                raise PlatformException(msg="Error in getting values for attribute %s.  %s" % (key, value_list))
            if value_list and value_list[0][0] == "ERROR_DATA_REQUEST_TOO_FAR_IN_PAST":
                raise PlatformException(msg="Time requested for %s too far in the past" % key)
            return_dict[key] = value_list
            count += len(value_list)
        log.info('_fetch_attrs %s returning %d items', platform_id, count)
        return return_dict

    @staticmethod
    def _build_particle(stream_name, timestamp, attrs):
        particle = PlatformParticle(attrs, preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP)
        particle.set_internal_timestamp(timestamp)
        particle._data_particle_type = stream_name
        return particle

    @stopwatch(label='_fetch', logger=log.debug)
    def _fetch(self, proxy, node_config, last_time):
        log.info('_fetch: %r %r', node_config.platform_id, last_time)
        base_refdes = node_config.node_meta_data['reference_designator']
        attrs = [(k, last_time) for k in node_config.attributes]
        fetched = OmsExtractor._fetch_attrs(proxy, node_config.platform_id, attrs)
        self._set_last_times(node_config.platform_id, fetched)
        for stream_name, stream_instances in node_config.node_streams.iteritems():
            for key, parameters in stream_instances.iteritems():
                particles = OmsExtractor._build_particles(stream_name, parameters, fetched)
                for particle in particles:
                    self.publisher.enqueue(self._asevent(particle, base_refdes, key))

    def _set_last_times(self, platform_id, fetched):
        with self.times_lock:
            try:
                new_max = max(pluck(1, concat(fetched.itervalues())))
                if new_max > self._last_times.get(platform_id, 0):
                    self._last_times[platform_id] = new_max
            except ValueError:
                pass

    @staticmethod
    def _asevent(particle, base_refdes, key):
        return {
            'type': DriverAsyncEvent.SAMPLE,
            'value': particle.generate(),
            'time': time.time(),
            'instance': '-'.join((base_refdes, key)),
        }

    @staticmethod
    def _build_particles(stream_name, parameters, data):
        subset = keyfilter(lambda k: k in parameters, data)
        grouped = OmsExtractor._group_by_timestamp(subset)
        particles = []
        for timestamp, attrs in grouped.iteritems():
            attrs = OmsExtractor._convert_attrs_to_ion(parameters, attrs)
            particles.append(OmsExtractor._build_particle(stream_name, timestamp, attrs))
        return particles


def main():
    import sys
    config_file = sys.argv[1]
    extractor = OmsExtractor(yaml.load(open(config_file)))
    while True:
        extractor.fetch_all()

if __name__ == '__main__':
    main()
