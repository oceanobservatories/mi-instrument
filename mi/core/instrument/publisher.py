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
    def from_url(url, headers=None):
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
            return QpidPublisher(new_url, queue, headers)

        elif result.scheme == 'rabbit':
            queue, query = extract_param('queue', result.query)

            if queue is None:
                raise ApplicationException('No queue provided in qpid url!')

            new_url = urlparse.urlunsplit(('amqp', result.netloc, result.path,
                                           query, result.fragment))
            return RabbitPublisher(new_url, queue, headers)

        elif result.scheme == 'log':
            return LogPublisher()

        elif result.scheme == 'count':
            return CountPublisher()


class QpidPublisher(Publisher):
    def __init__(self, url, queue, headers):
        self.connection = qm.Connection(url, reconnect=True, ssl=False)
        self.queue = queue
        self.session = None
        self.sender = None
        self.headers = headers
        self.connect()
        super(QpidPublisher, self).__init__()

    def connect(self):
        self.connection.open()
        self.session = self.connection.session()
        self.sender = self.session.sender('%s; {create: always, node: {type: queue, durable: False}}' % self.queue)

    def publish(self, events, headers=None):
        msg_headers = self.headers
        if headers is not None:
            # apply any new header values
            msg_headers.update(headers)

        if not isinstance(events, list):
            events = [events]

        # HACK!
        self.connection.error = None

        now = time.time()
        message = qm.Message(content=self.jsonify(events), content_type='text/plain', durable=True,
                             properties=msg_headers, user_id='guest')
        self.sender.send(message, sync=True)
        elapsed = time.time() - now
        log.info('Published %d messages to QPID in %.2f secs', len(events), elapsed)


class RabbitPublisher(Publisher):
    def __init__(self, url, queue, headers):
        self._url = url
        self.queue = queue
        self.session = None
        self.sender = None
        self.headers = headers
        self.connect()
        super(RabbitPublisher, self).__init__()

    def connect(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue, durable=True)

    def publish(self, events, headers=None):
        # TODO: add headers to message
        now = time.time()
        self.channel.basic_publish('', self.queue, self.jsonify(events),
                                   pika.BasicProperties(content_type='text/plain', delivery_mode=2))

        log.info('Published %d messages to RABBIT in %.2f secs', len(events), time.time()-now)


class LogPublisher(Publisher):
    def publish(self, events):
        log.info('Publish events: %r', events)


class CountPublisher(Publisher):
    def __init__(self):
        super(CountPublisher, self).__init__()
        self.total = 0

    def publish(self, events):
        for e in events:
            try:
                json.dumps(e)
            except (ValueError, UnicodeDecodeError) as err:
                log.exception('Unable to publish event: %r %r', e, err)
        count = len(events)
        self.total += count
        log.info('Publish %d events (%d total)', count, self.total)
