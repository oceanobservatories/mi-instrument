"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/publisher.py
@author Peter Cable
@brief Event publisher
Release notes:

initial release
"""
import json
import urllib
from mi.core.instrument.instrument_driver import DriverAsyncEvent
import qpid.messaging as qm
import time
import urlparse
import pika

from ooi.exception import ApplicationException

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
    def __init__(self, allowed):
        self.allowed = None

    def jsonify(self, events):
        try:
            return json.dumps(events)
        except UnicodeDecodeError as e:
            temp = []
            for each in events:
                try:
                    json.dumps(each)
                    temp.append(each)
                except UnicodeDecodeError as e:
                    log.error('Unable to encode event as JSON: %r', e)
            return json.dumps(temp)

    @staticmethod
    def group_events(events):
        group_dict = {}
        for event in events:
            group = event.pop('instance', None)
            group_dict[group].setdefault(group, []).append(event)
        return group_dict

    def publish(self, events, headers=None):
        if not isinstance(events, list):
            events = [events]

        events = self.filter_events(events)
        groups = self.group_events(events)
        for instance, group in groups:
            if instance is None:
                self._publish(group, instance)
            else:
                self._publish(group, {'sensor': instance})
        self._publish(events, headers)

    def _publish(self, events, headers):
        raise NotImplemented

    def filter_events(self, events):
        if self.allowed is not None and isinstance(self.allowed, list):
            log.info('Filtering %d events with: %r', len(events), self.allowed)
            new_events = []
            dropped = 0
            for event in events:
                if event.get('type') == DriverAsyncEvent.SAMPLE:
                    if event.get('value', {}).get('stream_name') in self.allowed:
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
        if result.scheme == 'qpid':
            # remove the queue from the url
            queue, query = extract_param('queue', result.query)

            if queue is None:
                raise ApplicationException('No queue provided in qpid url!')

            new_url = urlparse.urlunsplit((result.scheme, result.netloc, result.path,
                                           query, result.fragment))
            return QpidPublisher(new_url, queue, headers, allowed)

        elif result.scheme == 'rabbit':
            queue, query = extract_param('queue', result.query)

            if queue is None:
                raise ApplicationException('No queue provided in qpid url!')

            new_url = urlparse.urlunsplit(('amqp', result.netloc, result.path,
                                           query, result.fragment))
            return RabbitPublisher(new_url, queue, headers, allowed)

        elif result.scheme == 'log':
            return LogPublisher(allowed)

        elif result.scheme == 'count':
            return CountPublisher(allowed)


class QpidPublisher(Publisher):
    def __init__(self, url, queue, headers, allowed, username='guest', password='guest'):
        super(QpidPublisher, self).__init__(allowed)
        self.connection = qm.Connection(url, reconnect=True, username=username, password=password)
        self.queue = queue
        self.session = None
        self.sender = None
        self.headers = headers
        self.connect()

    def connect(self):
        self.connection.open()
        self.session = self.connection.session()
        self.sender = self.session.sender('%s; {create: always, node: {type: queue, durable: true}}' % self.queue)

    def _publish(self, events, headers):
        msg_headers = self.headers
        if headers is not None:
            # apply any new header values
            msg_headers.update(headers)

        # HACK!
        self.connection.error = None

        now = time.time()
        message = qm.Message(content=self.jsonify(events), content_type='text/plain', durable=True,
                             properties=msg_headers, user_id='guest')
        self.sender.send(message, sync=True)
        elapsed = time.time() - now
        log.info('Published %d messages to QPID in %.2f secs', len(events), elapsed)


class RabbitPublisher(Publisher):
    def __init__(self, url, queue, headers, allowed):
        super(RabbitPublisher, self).__init__(allowed)
        self._url = url
        self.queue = queue
        self.session = None
        self.sender = None
        self.headers = headers
        self.connect()

    def connect(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue, durable=True)

    def _publish(self, events, headers=None):
        # TODO: add headers to message
        now = time.time()
        self.channel.basic_publish('', self.queue, self.jsonify(events),
                                   pika.BasicProperties(content_type='text/plain', delivery_mode=2))

        log.info('Published %d messages to RABBIT in %.2f secs', len(events), time.time()-now)


class LogPublisher(Publisher):

    def _publish(self, events, headers):
        for e in events:
            log.info('Publish event: %r', e)


class CountPublisher(Publisher):
    def __init__(self, allowed):
        super(CountPublisher, self).__init__(allowed)
        self.total = 0

    def _publish(self, events, headers):
        for e in events:
            try:
                json.dumps(e)
            except (ValueError, UnicodeDecodeError) as err:
                log.exception('Unable to publish event: %r %r', e, err)
        count = len(events)
        self.total += count
        log.info('Publish %d events (%d total)', count, self.total)
