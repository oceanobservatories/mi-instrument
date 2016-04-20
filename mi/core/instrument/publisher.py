"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/publisher.py
@author Peter Cable
@brief Event publisher
Release notes:

initial release
"""
import time
import json
import urllib
import urlparse
from collections import deque
from threading import Thread

from mi.core.instrument.instrument_driver import DriverAsyncEvent
from ooi.logging import log


def extract_param(param, query):
    params = urlparse.parse_qsl(query, keep_blank_values=True)
    return_value = None
    new_params = []

    for name, value in params:
        if name == param:
            return_value = value
        else:
            new_params.append((name, value))

    return return_value, urllib.urlencode(new_params)


class Publisher(object):
    DEFAULT_MAX_EVENTS = 500
    DEFAULT_PUBLISH_INTERVAL = 5

    def __init__(self, allowed, max_events=None, publish_interval=None):
        self._allowed = allowed
        self._deque = deque()
        self._max_events = max_events if max_events else self.DEFAULT_MAX_EVENTS
        self._publish_interval = publish_interval if publish_interval else self.DEFAULT_PUBLISH_INTERVAL
        self._running = False

    def _run(self):
        self._running = True
        last_publish = time.time()
        while self._running:
            now = time.time()
            if now > last_publish + self._publish_interval:
                last_publish = now
                self.publish()
            time.sleep(.1)

    def start(self):
        t = Thread(target=self._run)
        t.setDaemon(True)
        t.start()

    def stop(self):
        self._running = False

    def enqueue(self, event):
        try:
            json.dumps(event)
            self._deque.append(event)
        except Exception as e:
            log.error('Unable to encode event as JSON: %r', e)

    def requeue(self, events):
        self._deque.extendleft(reversed(events))

    @staticmethod
    def group_events(events):
        group_dict = {}
        for event in events:
            group = event.pop('instance', None)
            group_dict.setdefault(group, []).append(event)
        return group_dict

    def publish(self):
        events = []
        for _ in xrange(self._max_events):
            try:
                events.append(self._deque.popleft())
            except IndexError:
                break

        if events:
            events = self.filter_events(events)
            groups = self.group_events(events)
            for instance in groups:
                if instance is None:
                    failed = self._publish(groups[instance], instance)
                    if failed:
                        self.requeue(failed)
                else:
                    failed = self._publish(groups[instance], {'sensor': instance})
                    if failed:
                        self.requeue(failed)

    def _publish(self, events, headers):
        raise NotImplemented

    def filter_events(self, events):
        if self._allowed is not None and isinstance(self._allowed, list):
            log.info('Filtering %d events with: %r', len(events), self._allowed)
            new_events = []
            dropped = 0
            for event in events:
                if event.get('type') == DriverAsyncEvent.SAMPLE:
                    if event.get('value', {}).get('stream_name') in self._allowed:
                        new_events.append(event)
                    else:
                        dropped += 1
                else:
                    new_events.append(event)
            log.info('Dropped %d unallowed particles', dropped)
            return new_events
        return events

    @staticmethod
    def from_url(url, headers=None, allowed=None):
        if headers is None:
            headers = {}

        result = urlparse.urlsplit(url)
        queue, query = extract_param('queue', result.query)
        url = result.scheme + '://' + result.netloc

        username = password = 'guest'
        if '@' in result.netloc:
            auth, base = result.netloc.split('@', 1)

            if ':' in auth:
                username, password = auth.split(':', 1)
            else:
                username = auth

        publisher = None
        if result.scheme == 'qpid':
            from qpid_publisher import QpidPublisher
            publisher = QpidPublisher

        elif result.scheme == 'amqp' or result.scheme == 'pyamqp':
            from kombu_publisher import KombuPublisher
            publisher = KombuPublisher

        elif result.scheme == 'log':
            return LogPublisher(allowed)

        elif result.scheme == 'count':
            return CountPublisher(allowed)

        elif result.scheme == 'csv':
            from file_publisher import CsvPublisher
            return CsvPublisher(allowed)

        elif result.scheme == 'pandas':
            from file_publisher import PandasPublisher
            return PandasPublisher(allowed)

        elif result.scheme == 'xarray':
            from file_publisher import XarrayPublisher
            return XarrayPublisher(allowed)

        if publisher:
            if queue is None:
                raise Exception('No queue provided!')
            return publisher(url, queue, headers, allowed, username, password)


class LogPublisher(Publisher):

    def _publish(self, events, headers):
        for e in events:
            log.info('Publish event: %r', e)


class CountPublisher(Publisher):
    def __init__(self, allowed):
        super(CountPublisher, self).__init__(allowed)
        self.total = 0

    def _publish(self, events, headers):
        count = len(events)
        self.total += count
        log.info('Publish %d events (%d total)', count, self.total)
