"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/publisher.py
@author Peter Cable
@brief Event publisher
Release notes:

initial release
"""
import csv
import json
import urllib

import pandas as pd
import xarray as xr
import numpy as np

from mi.core.instrument.instrument_driver import DriverAsyncEvent
import qpid.messaging as qm
import time
import urlparse
import pika

from ooi.exception import ApplicationException

from ooi.logging import log

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle


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
        self.allowed = allowed

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
            group_dict.setdefault(group, []).append(event)
        return group_dict

    def publish(self, events, headers=None):
        if not isinstance(events, list):
            events = [events]

        events = self.filter_events(events)
        groups = self.group_events(events)
        for instance in groups:
            if instance is None:
                self._publish(groups[instance], instance)
            else:
                self._publish(groups[instance], {'sensor': instance})

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

        elif result.scheme == 'csv':
            return CsvPublisher(allowed)

        elif result.scheme == 'pandas':
            return PandasPublisher(allowed)

        elif result.scheme == 'xarray':
            return XarrayPublisher(allowed)


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


class FilePublisher(Publisher):
    def __init__(self, allowed):
        super(FilePublisher, self).__init__(allowed)
        self.samples = {}

    @staticmethod
    def _flatten(sample):
        values = sample.pop('values')
        for each in values:
            sample[each['value_id']] = each['value']
        return sample

    def _publish(self, events, headers):
        for event in events:
            # file publisher only applicable to particles
            if event.get('type') != 'DRIVER_ASYNC_EVENT_SAMPLE':
                continue

            particle = event.get('value', {})
            stream = particle.get('stream_name')
            if stream:
                particle = self._flatten(particle)
                self.samples.setdefault(stream, []).append(particle)

    def to_dataframes(self):
        data_frames = {}
        for particle_type in self.samples:
            data_frames[particle_type] = self.fix_arrays(pd.DataFrame(self.samples[particle_type]))
        return data_frames

    def to_datasets(self):
        datasets = {}
        for particle_type in self.samples:
            datasets[particle_type] = self.fix_arrays(pd.DataFrame(self.samples[particle_type]), return_as_xr=True)
        return datasets

    @staticmethod
    def fix_arrays(data_frame, return_as_xr=False):
        # round-trip the dataframe through xray to get the multidimensional indexing correct
        new_ds = xr.Dataset()
        for each in data_frame:
            if data_frame[each].dtype == 'object' and isinstance(data_frame[each].values[0], list):
                data = np.array([np.array(x) for x in data_frame[each].values])
                new_ds[each] = xr.DataArray(data)
            else:
                new_ds[each] = data_frame[each]
        if return_as_xr:
            return new_ds
        return new_ds.to_dataframe()

    def write(self):
        log.info('Writing output files...')
        self._write()
        log.info('Done writing output files...')

    def _write(self):
        raise NotImplemented


class CsvPublisher(FilePublisher):
    def _write(self):
        dataframes = self.to_dataframes()
        for particle_type in dataframes:
            file_path = '%s.csv' % particle_type
            dataframes[particle_type].to_csv(file_path)


class PandasPublisher(FilePublisher):
    def _write(self):
        dataframes = self.to_dataframes()
        for particle_type in dataframes:
            # very large dataframes don't work with pickle
            # split if too large
            df = dataframes[particle_type]
            max_size = 5000000
            if len(df) > max_size:
                num_slices = len(df) / max_size
                slices = np.array_split(df, num_slices)
                for index, df_slice in enumerate(slices):
                    file_path = '%s_%d.pd' % (particle_type, index)
                    df_slice.to_pickle(file_path)
            else:
                log.info('length of dataframe: %d', len(df))
                file_path = '%s.pd' % particle_type
                dataframes[particle_type].to_pickle(file_path)


class XarrayPublisher(FilePublisher):
    def _write(self):
        datasets = self.to_datasets()
        for particle_type in datasets:
            file_path = '%s.xr' % particle_type
            with open(file_path, 'w') as fh:
                pickle.dump(datasets[particle_type], fh, protocol=-1)
