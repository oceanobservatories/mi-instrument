#!/usr/bin/env python

"""
@package ion.agents.platform.util.node_configuration
@file    ion/agents/platform/util/node_configuration.py
@author  Mike Harrington
@brief   read node configuration files
"""

__author__ = 'Mike Harrington'
__license__ = 'Apache 2.0'

from ooi.logging import log
from mi.platform.exceptions import NodeConfigurationFileException

from mi.platform.util.NodeYAML import NodeYAML

import yaml
import logging


class NodeConfiguration(object):
    """
    Various utilities utilities for reading in node configuration yaml files.
    """
    
    def __init__(self):
        self._node_yaml = NodeYAML.factory(None)

    @property
    def node_meta_data(self):
        return self._node_yaml.node_meta_data

    @property
    def node_streams(self):
        return self._node_yaml.node_streams

    @property
    def node_port_info(self):
        return self._node_yaml.node_port_info

    def openNode(self,platform_id,node_config_filename):
        """
        Opens up and parses the node configuration files.
   

        @param platform_id - id to associate with this set of Node Configuration Files
        @param node_config_file - yaml file with information about the platform

        @raise NodeConfigurationException
        """
 
        self._platform_id = platform_id
        
        log.debug("%r: Open: %s", self._platform_id, node_config_filename)

        try:
            with open(node_config_filename, 'r') as node_config_file:
                try:
                    node_config = yaml.load(node_config_file)
                except Exception as e:
                    raise NodeConfigurationFileException(msg="%s Cannot parse yaml node specific config file  : %s" % (str(e),node_config_filename))
        except Exception as e:
            raise NodeConfigurationFileException(msg="%s Cannot open node specific config file  : %s" % (str(e),node_config_filename))

        self._node_yaml = NodeYAML.factory(node_config)
        self._node_yaml.validate()
    
    
    def Print(self):
        log.debug("%r  Print Config File Information for: %s\n\n", self._platform_id, self.node_meta_data['node_id_name'])
        
        
        log.debug("%r  Node Meta data", self._platform_id)
        for meta_data_key,meta_data_item in sorted(self.node_meta_data.iteritems()):
            log.debug("%r   %r = %r", self._platform_id, meta_data_key,meta_data_item)

  
        log.debug("%r  Node Port Info", self._platform_id)
        for port_data_key,port_data_item in sorted(self.node_port_info.iteritems()):
            log.debug("%r   %r = %r", self._platform_id, port_data_key,port_data_item)

        log.debug("%r  Node stream Info", self._platform_id)
        for stream_data_key,stream_data_item in sorted(self.node_streams.iteritems()):
            log.debug("%r   %r = %r", self._platform_id, stream_data_key,stream_data_item)

          
   
   
        
