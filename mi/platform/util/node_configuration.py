#!/usr/bin/env python

"""
@package ion.agents.platform.util.node_configuration
@file    ion/agents/platform/util/node_configuration.py
@author  Mike Harrington
@brief   read node configuration files
"""
import pprint

__author__ = 'Mike Harrington'
__license__ = 'Apache 2.0'

from ooi.logging import log
from mi.platform.exceptions import NodeConfigurationFileException

from mi.platform.util.NodeYAML import NodeYAML

import yaml
import logging


DEFAULT_STREAM_DEF_FILENAME = 'mi/platform/rsn/node_config_files/stream_defs.yml'

class NodeConfiguration(object):
    """
    Various utilities utilities for reading in node configuration yaml files.
    """

    def __init__(self):
        self._platform_id = None
        self._node_yaml = NodeYAML.factory(None, None)

    @property
    def node_meta_data(self):
        return self._node_yaml.node_meta_data

    @property
    def node_streams(self):
        return self._node_yaml.node_streams

    @property
    def node_port_info(self):
        return self._node_yaml.node_port_info

    def openNode(self, platform_id, node_config_filename, stream_definition_filename=DEFAULT_STREAM_DEF_FILENAME):
        """
        Opens up and parses the node configuration files.
        @param platform_id - id to associate with this set of Node Configuration Files
        @param nc_file - yaml file with information about the platform
        @raise NodeConfigurationException
        """
        self._platform_id = platform_id

        log.debug("%r: Open: %s", self._platform_id, node_config_filename)

        with open(node_config_filename, 'r') as nc_file, open(stream_definition_filename, 'r') as sc_file:
            try:
                node_config = yaml.load(nc_file)
                stream_definitions = yaml.load(sc_file)
                self._node_yaml = NodeYAML.factory(node_config, stream_definitions)
                self._node_yaml.validate()
            except Exception as e:
                import traceback
                traceback.print_exc()
                msg = "%s Cannot parse yaml node specific config file  : %s" % (e, node_config_filename)
                raise NodeConfigurationFileException(msg=msg)
            except IOError as e:
                msg = "%s Cannot open node specific config file  : %s" % (e, node_config_filename)
                raise NodeConfigurationFileException(msg=msg)

    def Print(self):
        log.debug("%r  Print Config File Information for: %s\n\n", self._platform_id,
                  self.node_meta_data['node_id_name'])
        log.debug("%r  Node Meta data", self._platform_id)
        for meta_data_key, meta_data_item in sorted(self.node_meta_data.iteritems()):
            log.debug("%r   %r = %r", self._platform_id, meta_data_key, meta_data_item)

        log.debug("%r  Node Port Info", self._platform_id)
        for port_data_key, port_data_item in sorted(self.node_port_info.iteritems()):
            log.debug("%r   %r = %r", self._platform_id, port_data_key, port_data_item)

        log.debug("%r  Node stream Info", self._platform_id)
        for stream_data_key, stream_data_item in sorted(self.node_streams.iteritems()):
            log.debug("%r   %r", self._platform_id, stream_data_key)
            for name in stream_data_item:
                log.debug("%r       %s", self._platform_id, name)
                for param_name, values in stream_data_item[name].iteritems():
                    log.debug("%r           %r = %r", self._platform_id, param_name, values)
