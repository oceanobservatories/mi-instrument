#!/usr/bin/env python

"""
@package ion.agents.platform.util.test.test_NodeConfiguration
@file    ion/agents/platform/util/test/test_NodeConfiguration.py
@author  Mike Harrington
@brief   Test cases for node_configuration.
"""

__author__ = 'Mike Harrington'
__license__ = 'Apache 2.0'

#
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_read_files
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_bad_default_file
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_bad_node_config_file
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_scale_factors
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_attr_lookup
# bin/nosetests -sv ion.agents.platform.util.test.test_node_configuration:Test.test_oms_port_id
#

from pyon.public import log
import logging

from ion.agents.platform.util.node_configuration import NodeConfiguration
from ion.agents.platform.exceptions import NodeConfigurationFileException

from pyon.util.containers import DotDict

from pyon.util.unit_test import IonUnitTestCase

from nose.plugins.attrib import attr


import unittest

import pprint as pp


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):

    def test_read_files(self):


        nodeConfig = NodeConfiguration()
  
#        nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
        nodeConfig.Open('DeepProfilerSim','/tmp/node_config_files/default_dp_node.yaml','/tmp/node_config_files/DeepProfilerSim.yaml')
        
        nodeConfig.Print()
 
        
        
    def test_bad_default_file(self):


        nodeConfig = NodeConfiguration()
  
        try:
            nodeConfig.Open('LPJBox_CI','junk.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
            self.assert_(False,'Did not Catch Exception')
        except NodeConfigurationFileException as e:
            log.debug("Correctly Caught Exception %s",e)

    def test_bad_node_config_file(self):


        nodeConfig = NodeConfiguration()
  
        try:
            nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','junk.yaml')
            self.assert_(False,'Did not Catch Exception')
        except NodeConfigurationFileException as e:
            log.debug("Correctly Caught Exception %s",e)


    def test_scale_factors(self):

        nodeConfig = NodeConfiguration()
  
        nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
        
        self.assertEquals(nodeConfig.GetScaleFactorFromAttr('Instrument Port 0 Unit Temperature'),0.001)

        self.assertEquals(nodeConfig.GetScaleFactorFromAttr('junk'),1)
        
        self.assertEquals(nodeConfig.GetScaleFactorFromAttr('CIB 5V Current'),0.001)


    def test_attr_lookup(self):

        nodeConfig = NodeConfiguration()
  
        nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
        
        self.assertEquals(nodeConfig.GetParameterFromAttr('Instrument Port 0 Unit Temperature'),'sec_node_port_output_temperature')

        try:
            nodeConfig.GetParameterFromAttr('junk')
            self.assert_(False,'Did not Catch Exception')
        except NodeConfigurationFileException as e:
            log.debug("Correctly Caught Exception %s",e)

        self.assertEquals(nodeConfig.GetParameterFromAttr('CIB 5V Current'),'sec_node_cib_5v_current')

    def test_param_lookup(self):

        nodeConfig = NodeConfiguration()
  
        nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
        
        self.assertEquals(nodeConfig.GetAttrFromParameter('sec_node_port_output_temperature'),'Instrument Port 0 Unit Temperature')

        try:
            nodeConfig.GetAttrFromParameter('junk')
            self.assert_(False,'Did not Catch Exception')
        except NodeConfigurationFileException as e:
            log.debug("Correctly Caught Exception %s",e)

        self.assertEquals(nodeConfig.GetAttrFromParameter('sec_node_cib_5v_current'),'CIB 5V Current')

    def test_oms_port_id(self):
        nodeConfig = NodeConfiguration()
  
        nodeConfig.Open('LPJBox_CI','/tmp/node_config_files/default_node.yaml','/tmp/node_config_files/LPJBox_LJ0CI.yaml')
        
        self.assertEquals(nodeConfig.GetOMSPortId('J05-IP1'),'0')

        try:
            nodeConfig.GetOMSPortId('junk')
            self.assert_(False,'Did not Catch Exception')
        except NodeConfigurationFileException as e:
            log.debug("Correctly Caught Exception %s",e)

        self.assertEquals(nodeConfig.GetOMSPortId('J06-IP2'),'1')

      
