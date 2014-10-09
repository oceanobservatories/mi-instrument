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

import yaml
import copy
import logging


class NodeConfiguration(object):
    """
    Various utilities utilities for reading in node configuration yaml files.
    """
    
    def __init__(self):
        self.meta_data = {}
        self.attrs = {}

 

    def Open(self,platform_id,default_filename,node_config_filename):
        """
        Opens up and parses the node configuration files.
        Combines the information in defaultFile with the Node
        Specific information in the NodeConfigFile to create
        a dictionary of attributes and meta data used to read
        and parse data from the OMS Agent

        @param platform_id - id to associate with this set of Node Configuration Files
        @param default_file - yaml file with generic information used by many platforms
        @param node_config_file - yaml file with specific information about the platform

        @raise NodeConfigurationException
        """
 
        self._platform_id = platform_id
        
        log.debug("%r: Open: %s %s", self._platform_id, default_filename,node_config_filename)

 
 
        try:
            default_config_file = open(default_filename, 'r')
        except Exception as e:
            raise NodeConfigurationFileException(msg="%s Cannot open default node config file : %s" % (str(e),default_filename))
        
        try:
            default_config = yaml.load(default_config_file)
        except Exception as e:
            raise NodeConfigurationFileException(msg="%s Cannot parse yaml  node config file : %s" % (str(e),default_filename))

        
        try:
            node_config_file = open(node_config_filename, 'r')
        except Exception as e:
            raise NodeConfigurationFileException(msg="%s Cannot open node specific config file  : %s" % (str(e),node_config_filename))

        try:
            node_config = yaml.load(node_config_file)
        except Exception as e:
            raise NodeConfigurationFileException(msg="%s Cannot parse yaml node specific config file  : %s" % (str(e),node_config_filename))
   

        self.node_meta_data = copy.deepcopy(node_config["node_meta_data"])
         
#this will be the list of all monitored attributes for the node and active ports
#first just load the node attributes directly
        self.attrs = copy.deepcopy(default_config["node_attributes"])
        
        self.port_configurations = copy.deepcopy(node_config["port_configs"])
       
        temp_port_attr = {}
    
    
        for portKey,port in self.port_configurations.iteritems():
            temp_port_attr = copy.deepcopy(default_config["port_attributes"])

# go through and update the default port attributes for this port with specifics 
            for port_attr_key,port_attribute in temp_port_attr.iteritems():
                port_attribute['attr_id']=port['port_oms_prefix']+' '+port_attribute['attr_id']
                port_attribute['ion_parameter_name']=port['port_ion_prefix']+'_'+port_attribute['ion_parameter_name']
                self.attrs[portKey+'_'+port_attr_key]=port_attribute
                                  
 
        self._parmLookup = {}
        self._attrLookup = {}
        self._scaleLookup = {}
                
         
        for attrKey,attr in self.attrs.iteritems():
            self._parmLookup[attr['attr_id']]=attr['ion_parameter_name']
            self._attrLookup[attr['ion_parameter_name']]=attr['attr_id']
            self._scaleLookup[attr['attr_id']]=attr['scale_factor']


    def GetOMSPortId(self,ui_port_name):
        for portKey,port in self.port_configurations.iteritems():
            if(port['port_ui_name']==ui_port_name):
                return(port['port_oms_port_cntl_id'])
        raise NodeConfigurationFileException(msg="GetOMSPortId Cannot find ui_port_name  : %s" % ui_port_name)


    
    def Print(self):
        log.debug("%r  Print Config File Information for: %s\n\n", self._platform_id, self.node_meta_data['node_id_name'])
        
        
        log.debug("%r  Node Meta data", self._platform_id)
        for meta_data_key,meta_data_item in self.node_meta_data.iteritems():
            log.debug("%r   %r = %r", self._platform_id, meta_data_key,meta_data_item)


        log.debug("\n%r  Port data", self._platform_id)
        for portKey,port in self.port_configurations.iteritems():
            log.debug("%r %s", self._platform_id,portKey)
            for portAttrKey,portAttr in port.iteritems():
                log.debug("%r     %r = %r", self._platform_id, portAttrKey,portAttr)
           
           
        log.debug("\n%r  Attr data", self._platform_id)
        for attrItemKey,attrItem in self.attrs.iteritems():
            log.debug("%r %s", self._platform_id,attrItemKey)
            for attrKey,attr in attrItem.iteritems():
                log.debug("%r     %r = %r", self._platform_id, attrKey,attr)

        log.debug("\n%r  CI-OMS Parameter Lookup", self._platform_id)
        for attrItemKey,attrItem in self._parmLookup.iteritems():
            log.debug("%r     %r = %r", self._platform_id, attrItemKey,attrItem)

        log.debug("\n%r  OMS-CI Parameter Lookup", self._platform_id)
        for attrItemKey,attrItem in self._attrLookup.iteritems():
            log.debug("%r     %r = %r", self._platform_id, attrItemKey,attrItem)

    
    def GetNodeCommonName(self):
        return(self.meta_data['node_id_name'])
    
    def GetMetaData(self):
        return(self.meta_data)


    def GetNodeAttrDict(self):
        return(self.attrs)   
    
    def GetScaleFactorFromAttr(self,attr_id):
        if attr_id in self._scaleLookup:
            return self._scaleLookup[attr_id]
        return 1

    
    def GetParameterFromAttr(self,attr_id):
        if attr_id in self._parmLookup:
            return self._parmLookup[attr_id]
        else :
            raise NodeConfigurationFileException(msg="GetParameterFromAttr Cannot find attr_id  : %s" % attr_id)

        
    def GetAttrFromParameter(self,parameter_name):
        if parameter_name in self._attrLookup:
            return self._attrLookup[parameter_name]
        else :
            raise NodeConfigurationFileException(msg="GetAttrFromParameter Cannot find parameter_name  : %s" % parameter_name)

        