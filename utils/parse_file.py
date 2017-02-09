#!/usr/bin/env python

import importlib
import json
import os
from functools import wraps

import click as click
import datetime
import pandas as pd
import xarray as xr
import numpy as np

from mi.core.log import get_logger, LoggerManager

try:
    import cPickle as pickle
except ImportError:
    import pickle


lm = LoggerManager()
log = get_logger()
base_path = os.path.dirname(os.path.dirname(__file__))


class StopWatch(object):
    """
    Easily measure elapsed time
    """
    def __init__(self, message=None):
        self.start_time = datetime.datetime.now()
        self.message = message

    def __repr__(self):
        stop = datetime.datetime.now()
        r = str(stop - self.start_time)
        if self.message:
            return self.message + ' ' + r
        return r

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.info(self)


def monkey_patch_particles():
    """
    Monkey patch DataParticle.generate to skip the JSON-encoding
    :return:
    """
    log.info('Monkey patching DataParticle.generate')
    import mi.core.instrument.dataset_data_particle

    def _generate(self, sorted=False):
        return self.generate_dict()

    mi.core.instrument.dataset_data_particle.DataParticle.generate = _generate


def log_timing(func):
    """
    Decorator which will log the time elapsed while executing a function call
    :param func: function to be wrapped
    :return: wrapped function
    """
    @wraps(func)
    def inner(*args, **kwargs):
        with StopWatch('Function %s took:' % func):
            return func(*args, **kwargs)
    return inner


class ParticleHandler(object):
    """
    Particle handler which flattens all data particle "values" lists to key: value pairs in the parent dictionary
    Also contains a method to output the particle data as a dictionary of pandas dataframes
    """
    def __init__(self, output_path=None, formatter=None):
        self.samples = {}
        self.failure = False
        if output_path is None:
            output_path = os.getcwd()
        self.output_path = output_path
        self.formatter = formatter
        self.check_output_path()

    def check_output_path(self):
        op = self.output_path
        if os.path.isdir(op):
            return
        if os.path.isfile(op):
            raise OSError('output path is a file!')
        else:
            os.makedirs(op)

    @staticmethod
    def flatten(sample):
        values = sample.pop('values')
        for each in values:
            sample[each['value_id']] = each['value']
        return sample

    def addParticleSample(self, sample_type, sample):
        sample = self.flatten(sample)
        self.samples.setdefault(sample_type, []).append(sample)

    def setParticleDataCaptureFailure(self):
        self.failure = True

    @log_timing
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
    @log_timing
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

    @log_timing
    def to_csv(self):
        dataframes = self.to_dataframes()
        for particle_type in dataframes:
            file_path = os.path.join(self.output_path, '%s.csv' % particle_type)
            dataframes[particle_type].to_csv(file_path)

    @log_timing
    def to_json(self):
        for particle_type in self.samples:
            file_path = os.path.join(self.output_path, '%s.json' % particle_type)
            with open(file_path, 'w') as fh:
                json.dump(self.samples[particle_type], fh)

    @log_timing
    def to_pd_pickle(self):
        dataframes = self.to_dataframes()
        for particle_type in dataframes:
            file_path = os.path.join(self.output_path, '%s.pd' % particle_type)
            with open(file_path, 'w') as fh:
                pickle.dump(dataframes[particle_type], fh, protocol=-1)

    @log_timing
    def to_xr_pickle(self):
        datasets = self.to_datasets()
        for particle_type in datasets:
            file_path = os.path.join(self.output_path, '%s.xr' % particle_type)
            with open(file_path, 'w') as fh:
                pickle.dump(datasets[particle_type], fh, protocol=-1)

    def write(self):
        option_map = {
            'csv': self.to_csv,
            'json': self.to_json,
            'pd-pickle': self.to_pd_pickle,
            'xr-pickle': self.to_xr_pickle
        }
        formatter = option_map[self.formatter]
        formatter()


def find_driver(driver_string):
    try:
        return importlib.import_module(driver_string)
    except ImportError:
        if os.sep in driver_string:
            driver_string = driver_string.replace('.py', '')
            driver_string = driver_string.replace(os.sep, '.')
            return importlib.import_module(driver_string)
    raise Exception('Unable to locate driver: %r', driver_string)


def run(driver, files, fmt, out):
    monkey_patch_particles()
    log.info('Importing driver: %s', driver)
    module = find_driver(driver)
    particle_handler = ParticleHandler(output_path=out, formatter=fmt)
    for file_path in files:
        log.info('Begin parsing: %s', file_path)
        with StopWatch('Parsing file: %s took' % file_path):
            module.parse(base_path, file_path, particle_handler)

    particle_handler.write()


@click.command()
@click.option('--fmt', type=click.Choice(['csv', 'json', 'pd-pickle', 'xr-pickle']), default='csv')
@click.option('--out', type=click.Path(exists=False), default=None)
@click.argument('driver', nargs=1)
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def main(driver, files, fmt, out):
    run(driver, files, fmt, out)


if __name__ == '__main__':
    main()
