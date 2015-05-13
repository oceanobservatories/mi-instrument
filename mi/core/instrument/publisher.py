"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/publisher.py
@author Peter Cable
@brief Event publisher
Release notes:

initial release
"""
import qpid.messaging as qm
import time_tools


class Publisher(object):
    def __init__(self):
        pass


class QpidPublisher(Publisher):
    def __init__(self, addr, port, user, password):
        self.connection = qm.Connection(host=addr, port=port, username=user, password=password, reconnect=True)
        self.session = None
        self.particle_sender = None
        self.event_sender = None
        self.connect()
        super(QpidPublisher, self).__init__()

    def connect(self):
        self.connection.open()
        self.session = self.connection.session()
        self.particle_sender = self.session.sender('particle_data; {create: always, node: {type: queue, durable: True}}')
        self.event_sender = self.session.sender('driver_events; {create: always, node: {type: queue, durable: True}}')

    def publish_event(self, event):
        message = qm.Message(content=event, durable=True)
        self.particle_sender.send(message, sync=False)


if __name__ == '__main__':
    publisher = QpidPublisher('uf.local', 5672, 'guest', 'guest')
    # publisher = RabbitPublisher()
    # publisher = KafkaPublisher()
    # publisher = ZmqPublisher()
    now = time_tools.time_tools()
    count = 10000
    for _ in xrange(count):
        publisher.publish_event({'hello': 'world'})

    elapsed = time_tools.time_tools() - now
    print 'sent %d messages in %.3f secs (%.2f/s)' % (count, elapsed, count/elapsed)