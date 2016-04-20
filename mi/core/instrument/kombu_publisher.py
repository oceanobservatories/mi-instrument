"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/kombu_publisher.py
@author Peter Cable
@brief Event kombu publisher
Release notes:

initial release
"""
import json
import time
import kombu

from mi.core.instrument.publisher import Publisher
from ooi.logging import log


class KombuPublisher(Publisher):
    def __init__(self, url, queue, headers, allowed, username='guest', password='guest', **kwargs):
        super(KombuPublisher, self).__init__(allowed, **kwargs)
        self._url = url
        self.queue = queue
        self.headers = headers
        self.username = username
        self.password = password
        self.exchange = kombu.Exchange(name='amq.direct', type='direct')
        self._queue = kombu.Queue(name=queue, exchange=self.exchange, routing_key=queue)
        self.connection = kombu.Connection(self._url, userid=self.username, password=self.password)
        self.producer = kombu.Producer(self.connection, routing_key=self.queue, exchange=self.exchange)

    def _publish(self, events, headers):
        msg_headers = self.headers
        if headers is not None:
            # apply any new header values
            msg_headers.update(headers)

        now = time.time()
        try:
            publish = self.connection.ensure(self.producer, self.producer.publish, max_retries=4)
            publish(json.dumps(events), headers=msg_headers, user_id=self.username,
                    declare=[self._queue], content_type='text/plain')
            log.info('Published %d messages using KOMBU in %.2f secs', len(events), time.time() - now)
        except Exception as e:
            log.error('Exception attempting to publish events: %r', e)
            return events





