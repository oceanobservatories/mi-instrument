#!/usr/bin/env python
import logging
import random
import string

import qpid.messaging as qm
import json
import time
import sqlite3
import sys
from parse_preload import load_paramdefs, load_paramdicts

HOST = 'uframe'
PORT = 5672
USER = 'guest'
QUEUE = 'particle_data'
NTP_OFFSET = 2208988800l
DBFILE = 'preload.db'
IGNORE_PARAMS = 'PD7,PD10,PD11,PD12,PD863'.split(',')

SAMPLE = '{"type":"DRIVER_ASYNC_EVENT_SAMPLE",' \
         '"value":"{\\"quality_flag\\": \\"ok\\", \\"preferred_timestamp\\": \\"port_timestamp\\",' \
         '\\"values\\": [{\\"value_id\\": \\"serial_number\\",\\"value\\": \\"4278190306\\"},' \
         '               {\\"value_id\\": \\"elapsed_time\\", \\"value\\": 4165.05},' \
         '               {\\"value_id\\": \\"par\\", \\"value\\": 2157006272},' \
         '               {\\"value_id\\": \\"checksum\\", \\"value\\": 70}],' \
         '\\"stream_name\\": \\"parad_sa_sample\\",' \
         '\\"port_timestamp\\": 3618862452.335417,' \
         '\\"driver_timestamp\\": 3618862453.114037,' \
         '\\"pkt_format_id\\": \\"JSON_Data\\",' \
         '\\"pkt_version\\": 1}",' \
         '"time":1410648826.227000}'



def get_logger():
    logger = logging.getLogger('driver_control')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger()


def get_connection(dbfile):
    return sqlite3.connect(dbfile)


class StreamGenerator(object):
    def __init__(self, stream_name, rate, param_defs, param_dicts):
        self.stream_name = stream_name
        self.param_defs = param_defs
        self.param_dicts = param_dicts
        self.rate = rate
        self.sender = self.connect()

    @staticmethod
    def get_timestamp():
        return time.time() + NTP_OFFSET

    def create_event(self):
        ts = self.get_timestamp()
        values = []
        event = {'type': 'DRIVER_ASYNC_EVENT_SAMPLE',
                 'time': ts}
        stream = {'driver_timestamp': ts,
                  'port_timestamp': ts,
                  'preferred_timestamp': 'port_timestamp',
                  'stream_name': self.stream_name,
                  'pkt_format_id': 'JSON_Data',
                  'pkt_version': 1,
                  'values': values}
        stream_def = self.param_dicts.get(self.stream_name)
        params = stream_def.parameter_ids.split(',')
        for param in params:
            if param in IGNORE_PARAMS:
                continue
            p = self.param_defs.get(param)
            print p
            val = None
            if p.value_encoding in ['str', 'string']:
                val = self.random_string(20)
            elif p.value_encoding == 'int8':
                val = random.randint(-2**7, 2**7)
            elif p.value_encoding == 'int16':
                val = random.randint(-2**15, 2**15)
            elif p.value_encoding == 'int32':
                val = random.randint(-2**31, 2**31)
            elif p.value_encoding == 'int32':
                val = random.randint(-2**63, 2**63)
            elif p.value_encoding == 'uint8':
                val = random.randint(0, 2**8)
            elif p.value_encoding == 'uint16':
                val = random.randint(0, 2**16)
            elif p.value_encoding == 'uint32':
                val = random.randint(0, 2**32)
            elif p.value_encoding == 'uint64':
                val = random.randint(0, 2**64)
            elif p.value_encoding in ['float32', 'float64']:
                val = random.random()
            else:
                log.debug('Unhandled parameter value encoding: %s', p)
            if val is not None:
                if 'array' in p.parameter_type and not p.value_encoding in ['str', 'string']:
                    val = [val] * 2
                values.append({'value_id': p.name, 'value': val})
        particle = json.dumps(stream)
        event['value'] = particle
        return json.dumps(event)

    def create_message(self, content):
        return qm.Message(content=content, content_type='application/json', user_id=USER)

    @staticmethod
    def connect():
        conn = qm.Connection(host=HOST, port=PORT, username=USER, password=USER)
        conn.open()
        return conn.session().sender(QUEUE)

    @staticmethod
    def random_string(size):
        return ''.join([random.choice(string.ascii_letters) for x in range(size)])

    def send_one(self):
        self.sender.send(self.create_message(self.create_event()))

    def go(self):
        while True:
            message = self.create_message(self.create_event())
            log.info('Sending message: %s', message)
            self.sender.send(message)
            time.sleep(1.0/self.rate)


def main():
    stream_name = sys.argv[1]
    rate = int(sys.argv[2])

    db = get_connection(DBFILE)
    param_defs = load_paramdefs(db)
    param_dicts = load_paramdicts(db)[1]  # (param dicts by id, param dicts by name)

    sg = StreamGenerator(stream_name, rate, param_defs, param_dicts)
    sg.go()

if __name__ == '__main__':
    main()
