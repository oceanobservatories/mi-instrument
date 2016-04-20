"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/qpid_publisher.py
@author Peter Cable
@brief Event qpid publisher
Release notes:

initial release
"""
import json
import time

import qpid.messaging as qm

from mi.core.instrument.publisher import Publisher
from ooi.logging import log


class QpidPublisher(Publisher):
    def __init__(self, url, queue, headers, allowed, username='guest', password='guest', **kwargs):
        super(QpidPublisher, self).__init__(allowed, **kwargs)
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
        message = qm.Message(content=json.dumps(events), content_type='text/plain', durable=True,
                             properties=msg_headers, user_id='guest')
        self.sender.send(message, sync=True)
        elapsed = time.time() - now
        log.info('Published %d messages to QPID in %.2f secs', len(events), elapsed)
