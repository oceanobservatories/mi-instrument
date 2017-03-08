#!/usr/bin/env python
"""
@package mi.core.shovel
@file mi/core/shovel.py
@author Peter Cable
@brief Move messages from rabbitMQ to QPID

Usage:
    shovel <rabbit_url> <rabbit_queue> <rabbit_key> <qpid_url> <qpid_queue>

Options:
    -h, --help          Show this screen.

"""
import time
from threading import Thread

import qpid.messaging as qm
from docopt import docopt
from kombu import Connection, Queue, Exchange
from kombu.mixins import ConsumerMixin
from librabbitmq import ChannelError
from mi.core.log import LoggerManager
from mi.logging import log

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
    def __init__(self, url, queue, routing_key, qpid):
        self.connection = Connection(hostname=url)
        self.exchange = Exchange(name='amq.direct', type='direct', channel=self.connection)
        self.qpid = qpid
        self.count = 0

        kwargs = {
            'exchange': self.exchange,
            'routing_key': routing_key,
            'channel': self.connection,
            'name': queue,
        }

        # If the requested queue already exists, attach to it
        # Otherwise, declare a new queue which will be deleted upon disconnect
        try:
            self.queue = Queue(**kwargs)
            self.queue.queue_declare(passive=True)
        except ChannelError:
            self.queue = Queue(auto_delete=True, **kwargs)

    def get_consumers(self, Consumer, channel):
        c = Consumer([self.queue], callbacks=[self.on_message])
        c.qos(prefetch_count=100)
        return [c]

    def on_message(self, body, message):
        try:
            self.qpid.send(str(body), message.headers)
            message.ack()
            self.count += 1
        except Exception as e:
            log.exception('Exception while publishing message to QPID, requeueing')
            message.requeue()
            self.qpid.sender = None

    def get_current_queue_depth(self):
        try:
            result = self.queue.queue_declare(passive=True)
            name = result.queue
            count = result.message_count
        except ChannelError:
            if self.count > 0:
                log.exception('Exception getting queue count')
            name = 'UNK'
            count = 0
        return name, count, self.count


class StatsReporter(Thread):
    def __init__(self, rabbit, report_interval=60):
        self.rabbit = rabbit
        self.report_interval = report_interval
        self.last_time = None
        self.last_count = 0
        super(StatsReporter, self).__init__()

    def run(self):
        while True:
            queue_name, queue_depth, sent_count = self.rabbit.get_current_queue_depth()
            now = time.time()
            if self.last_time is not None:
                elapsed = now - self.last_time
                if elapsed > 0:
                    rate = float(sent_count - self.last_count) / elapsed
                else:
                    rate = -1
                log.info('Queue: %s Depth: %d Sent Count: %d Rate: %.2f/s', queue_name, queue_depth, sent_count, rate)

            self.last_time = now
            self.last_count = sent_count

            time.sleep(self.report_interval)



def main():
    options = docopt(__doc__)
    qpid_url = options['<qpid_url>']
    qpid_queue = options['<qpid_queue>']
    rabbit_url = options['<rabbit_url>']
    rabbit_queue = options['<rabbit_queue>']
    rabbit_key = options['<rabbit_key>']
    log.info('Starting shovel: %r', options)

    qpid = QpidProducer(qpid_url, qpid_queue)
    rabbit = RabbitConsumer(rabbit_url, rabbit_queue, rabbit_key, qpid)
    reporter = StatsReporter(rabbit)
    reporter.daemon = True
    reporter.start()
    rabbit.run()


if __name__ == '__main__':
    main()