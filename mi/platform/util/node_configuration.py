#!/usr/bin/env python

"""
@package ion.agents.platform.util.node_configuration
@file    ion/agents/platform/util/node_configuration.py
@author  Mike Harrington
@brief   read node configuration files
"""
import yaml
from pkg_resources import resource_string

import mi.platform.rsn
from mi.platform.exceptions import NodeConfigurationFileException
from mi.platform.util.NodeYAML import NodeYAML

__author__ = 'Mike Harrington'
__license__ = 'Apache 2.0'


class NodeConfiguration(object):
    """
    Various utilities utilities for reading in node configuration yaml files.
    """

    def __init__(self, node_config_filename, stream_definitions=None):
        self._attributes = None
        try:
            node_config_string = resource_string(mi.platform.rsn.__name__, node_config_filename)
            node_config = yaml.load(node_config_string)
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

    @property
    def platform_id(self):
        return self._node_yaml.node_meta_data['node_id_name']

    @property
    def node_meta_data(self):
        return self._node_yaml.node_meta_data

    @property
    def node_streams(self):
        return self._node_yaml.node_streams

    @property
    def node_port_info(self):
        return self._node_yaml.node_port_info

    @property
    def attributes(self):
        if self._attributes is None:
            self._attributes = self._get_attrs()
        return self._attributes

    def _get_attrs(self):
        attrs = []
        for stream, stream_instances in self.node_streams.iteritems():
            for instance, params in stream_instances.iteritems():
                attrs.extend(params.keys())
        return attrs

    def __repr__(self):
        return '%r %r %r' % (self.node_meta_data, self.node_streams, self.node_port_info)
