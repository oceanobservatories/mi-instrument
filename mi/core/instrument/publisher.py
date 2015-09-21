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
        for event in events:
            message = qm.Message(content=json.dumps(event), content_type='text/plain', durable=False,
                                 properties=msg_headers, user_id='guest')
            self.sender.send(message, sync=False)

        self.sender.sync()
        elapsed = time.time() - now
        log.info('Published %d messages to QPID in %.2f secs', len(events), elapsed)


class LogPublisher(Publisher):
    def publish(self, events):
        log.info('Publish events: %r', events)


class CountPublisher(Publisher):
    def __init__(self):
        super(CountPublisher, self).__init__()
        self.total = 0

    def publish(self, events):
        count = len(events)
        self.total += count
        log.info('Publish %d events (%d total)', count, self.total)
