#!/usr/bin/env python
"""
run_driver

Usage:
  run_driver.py <instruments.csv> <classes.csv> <reference_designator>
"""
import json

import docopt
from csv import DictReader
from mi.core.instrument.zmq_driver_process import ZmqDriverProcess


def build_port_agent_config(config_dict):
    if config_dict.get('port_agent_config', '') != '':
        port_agent_config = json.loads(config_dict['port_agent_config'])

    elif any([config_dict['port_agent_host'] == '',
              config_dict['port_agent_data'] == '',
              config_dict['port_agent_command'] == '']):
        port_agent_config = {}

    else:
        port_agent_config = {
            'addr': config_dict['port_agent_host'],
            'port': int(config_dict['port_agent_data']),
            'cmd_port': int(config_dict['port_agent_command']),
        }

    config_dict['port_agent_config'] = port_agent_config


def build_startup_config(config_dict):
    startup_config = config_dict.get('startup_config', '')
    if startup_config == '':
        config_dict['startup_config'] = {}

    else:
        config_dict['startup_config'] = json.loads(startup_config)


def parse_config(instruments_file, classes_file, refdes):
    inst_config, base_config = None, None

    for row in DictReader(open(instruments_file)):
        if row.get('reference_designator') == refdes:
            inst_config = row
            break

    if inst_config is not None:
        for row in DictReader(open(classes_file)):
            if row.get('instrument_class') == inst_config.get('instrument_class'):
                base_config = row
                base_config.update(inst_config)
                build_port_agent_config(base_config)
                build_startup_config(base_config)
                base_config['event_port'] = int(base_config['event_port'])
                base_config['command_port'] = int(base_config['command_port'])
                return base_config

    raise Exception()


if __name__ == '__main__':
    options = docopt.docopt(__doc__)
    config = parse_config(options['<instruments.csv>'], options['<classes.csv>'], options['<reference_designator>'])
    print config
    module = config['module']
    klass = config['klass']
    command_port = config['command_port']
    event_port = config['event_port']

    dp = ZmqDriverProcess(module, klass, command_port, event_port)
    dp.construct_driver()
    dp.driver.configure(config['port_agent_config'])
    dp.driver.set_init_params(config['startup_config'])
    dp.run()
