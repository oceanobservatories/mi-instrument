#!/usr/bin/env python
"""
@package mi.core.shovel
@file mi/core/shovel.py
@author Peter Cable
@brief Move messages from rabbitMQ to QPID

Usage:
    shovel <rabbit_url> <rabbit_queue> <qpid_url> <qpid_queue>

Options:
    -h, --help          Show this screen.

"""
import time

import os
from docopt import docopt
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange
import qpid.messaging as qm

from ooi.logging import log
from mi.core.log import LoggerManager

LoggerManager()


class QpidProducer(object):
    def __init__(self, url, queue, username='guest', password='guest'):
        self.url = url
        self.username = username
        self.password = password
        self.queue = queue
        self.sender = None

    def connect(self):
        delay = 1
        max_delay = 60
        while True:
            try:
                connection = qm.Connection(self.url, reconnect=False, username=self.username, password=self.password)
                connection.open()
                session = connection.session()
                self.sender = session.sender('%s; {create: always, node: {type: queue, durable: true}}' % self.queue)
                log.info('Shovel connected to QPID')
                return
            except qm.ConnectError:
                log.error('Shovel QPID connection error. Sleep %d seconds', delay)
                time.sleep(delay)
                delay = min(max_delay, delay*2)

    def send(self, message, headers):
        if self.sender is None:
            self.connect()
        message = qm.Message(content=message, content_type='text/plain', durable=True,
                             properties=headers, user_id='guest')
        self.sender.send(message, sync=False)


class RabbitConsumer(ConsumerMixin):
    def __init__(self, url, queue, qpid):
        self.connection = Connection(hostname=url)
        self.exchange = Exchange(name='amq.direct', type='direct', channel=self.connection)
        self.queue = Queue(name=queue, exchange=self.exchange, routing_key=queue,
                           channel=self.connection, durable=True)
        self.qpid = qpid

    def get_consumers(self, Consumer, channel):
        c = Consumer([self.queue], callbacks=[self.on_message])
        c.qos(prefetch_count=100)
        return [c]

    def on_message(self, body, message):
        try:
            self.qpid.send(str(body), message.headers)
            message.ack()
        except Exception as e:
            log.exception('Exception while publishing message to QPID, requeueing')
            message.requeue()
            self.qpid.sender = None


def main():
    options = docopt(__doc__)
    qpid_url = options['<qpid_url>']
    qpid_queue = options['<qpid_queue>']
    rabbit_url = options['<rabbit_url>']
    rabbit_queue = options['<rabbit_queue>']

    qpid = QpidProducer(qpid_url, qpid_queue)
    rabbit = RabbitConsumer(rabbit_url, rabbit_queue, qpid)
    rabbit.run()


if __name__ == '__main__':
    main()