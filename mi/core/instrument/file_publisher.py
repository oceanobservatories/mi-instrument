"""
@package mi.core.instrument.publisher
@file /mi-instrument/mi/core/instrument/file_publisher.py
@author Peter Cable
@brief Event file publisher
Release notes:

initial release
"""
import cPickle as pickle
import json

import numpy as np
import pandas as pd
import xarray as xr
from mi.core.instrument.publisher import Publisher

from ooi.logging import log


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
    def __init__(self, *args, **kwargs):
        super(FilePublisher, self).__init__(*args, **kwargs)
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
