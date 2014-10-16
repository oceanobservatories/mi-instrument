#! /usr/bin/env python

"""
@file coi-services/ion/idk/unit_test.py
@author Bill French
@brief Base classes for instrument driver tests.
"""

import subprocess
import re
import gc
import os
import time
import ntplib
import unittest
import datetime
from sets import Set

# Set testing to false because the capability container tries to clear out
# couchdb if we are testing. Since we don't care about couchdb for the most
# part we can ignore this. See initialize_ion_int_tests() for implementation.
# If you DO care about couch content make sure you do a force_clean when needed.


# Import pyon first for monkey patching.
from mi.core.log import get_logger

log = get_logger()

import gevent

from pprint import PrettyPrinter


from mock import Mock
from mi.core.unit_test import MiIntTestCase
from mi.core.unit_test import MiUnitTest
from mi.core.unit_test import ParticleTestMixin
from mi.core.port_agent_simulator import TCPSimulatorServer
from mi.core.instrument.instrument_driver import InstrumentDriver
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ConfigMetadataKey
from mi.core.instrument.instrument_protocol import InstrumentProtocol
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.instrument.protocol_param_dict import ParameterDictVisibility
from mi.core.instrument.protocol_param_dict import ParameterDictKey
from mi.core.instrument.protocol_param_dict import ParameterDictType
from mi.core.instrument.protocol_cmd_dict import CommandDictKey
from mi.core.instrument.driver_dict import DriverDictKey

# from interface.objects import AgentCapability
# from interface.objects import CapabilityType

from mi.core.driver_process import DriverProcess, DriverProcessType

# from interface.objects import AgentCommandResult
# from interface.objects import AgentCommand

from mi.idk.util import convert_enum_to_dict
from mi.idk.util import get_dict_value
from mi.idk.comm_config import CommConfig
from mi.idk.comm_config import ConfigTypes
from mi.idk.config import Config
from mi.idk.common import Singleton
from mi.idk.instrument_agent_client import InstrumentAgentClient
from mi.idk.instrument_agent_client import InstrumentAgentDataSubscribers
from mi.idk.instrument_agent_client import InstrumentAgentEventSubscribers

from mi.idk.exceptions import IDKException
from mi.idk.exceptions import TestNotInitialized
from mi.idk.exceptions import TestNoCommConfig

from mi.core.exceptions import InstrumentException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import InstrumentStateException
from mi.core.instrument.port_agent_client import PortAgentClient
from mi.core.instrument.port_agent_client import PortAgentPacket
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.data_particle import DataParticle
from mi.core.instrument.data_particle import DataParticleKey
from mi.core.instrument.data_particle import DataParticleValue
from mi.core.instrument.data_particle import RawDataParticleKey
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverConnectionState
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.tcp_client import TcpClient
from mi.core.common import BaseEnum
from mi.core.driver_scheduler import DriverSchedulerConfigKey, DriverScheduler
from mi.core.driver_scheduler import TriggerType


from mi.core.exceptions import Conflict
from mi.core.exceptions import ResourceError, BadRequest, Timeout, ServerError
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.instrument_driver import ResourceAgentEvent

from ooi.logging import log

# Do not remove this import.  It is for package building.
from mi.core.instrument.zmq_driver_process import ZmqDriverProcess

AGENT_DISCOVER_TIMEOUT = 900
GO_ACTIVE_TIMEOUT = 900
GET_TIMEOUT = 900
SET_TIMEOUT = 900
EXECUTE_TIMEOUT = 900

SAMPLE_RAW_DATA = "Iam Apublished Message"

LOCALHOST = 'localhost'


class DriverStartupConfigKey(BaseEnum):
    PARAMETERS = 'parameters'
    SCHEDULER = 'scheduler'


class AgentCapabilityType(BaseEnum):
    AGENT_COMMAND = 'agent_command'
    AGENT_PARAMETER = 'agent_parameter'
    RESOURCE_COMMAND = 'resource_command'
    RESOURCE_INTERFACE = 'resource_interface'
    RESOURCE_PARAMETER = 'resource_parameter'


class ParameterTestConfigKey(BaseEnum):
    """
    Defines the dict keys used in the data particle parameter config used in unit tests of data particles
    """
    TYPE = 'type'
    REQUIRED = 'required'
    NAME = 'name'
    DIRECT_ACCESS = 'directaccess'
    STARTUP = 'startup'
    READONLY = 'readonly'
    DEFAULT = 'default'
    STATES = 'states'
    VALUE = 'value'


class InstrumentDriverTestConfig(Singleton):
    """
    Singleton driver test config object.
    """
    driver_module = None
    driver_class = None

    working_dir = "/tmp/"  # requires trailing / or it messes up the path. should fix.

    delimeter = ['<<', '>>']
    logger_timeout = 15

    driver_process_type = DriverProcessType.PYTHON_MODULE
    agent_resource_id = None
    agent_name = None
    agent_module = 'mi.idk.instrument_agent'
    agent_class = 'InstrumentAgent'
    data_instrument_agent_module = 'mi.idk.instrument_agent'
    data_instrument_agent_class = 'PublisherInstrumentAgent'
    agent_packet_config = None
    agent_stream_encoding = 'ION R2'
    agent_stream_definition = None

    driver_startup_config = {}

    container_deploy_file = 'deploy/r2qual.yml'
    publisher_deploy_file = 'deploy/r2pub.yml'

    initialized = False

    def initialize(self, *args, **kwargs):
        self.driver_module = kwargs.get('driver_module')
        self.driver_class = kwargs.get('driver_class')
        if kwargs.get('working_dir'):
            self.working_dir = kwargs.get('working_dir')
        if kwargs.get('delimeter'):
            self.delimeter = kwargs.get('delimeter')

        self.agent_preload_id = get_dict_value(kwargs, ['instrument_agent_preload_id', 'agent_preload_id'])
        self.agent_resource_id = get_dict_value(kwargs, ['instrument_agent_resource_id', 'agent_resource_id'],
                                                self.agent_resource_id)
        self.agent_name = get_dict_value(kwargs, ['instrument_agent_name', 'agent_name'], self.agent_name)
        self.agent_packet_config = self._build_packet_config(
            get_dict_value(kwargs, ['instrument_agent_packet_config', 'agent_packet_config']))
        self.agent_stream_definition = get_dict_value(kwargs,
                                                      ['instrument_agent_stream_definition', 'agent_stream_definition'])
        self.agent_module = get_dict_value(kwargs, ['instrument_agent_module', 'agent_module'], self.agent_module)
        self.agent_class = get_dict_value(kwargs, ['instrument_agent_class', 'agent_class'], self.agent_class)
        self.agent_stream_encoding = get_dict_value(kwargs,
                                                    ['instrument_agent_stream_encoding', 'agent_stream_encoding'],
                                                    self.agent_stream_encoding)

        if kwargs.get('container_deploy_file'):
            self.container_deploy_file = kwargs.get('container_deploy_file')

        if kwargs.get('logger_timeout'):
            self.logger_timeout = kwargs.get('logger_timeout')

        if kwargs.get('driver_process_type'):
            self.driver_process_type = kwargs.get('driver_process_type')

        self.driver_startup_config = get_dict_value(kwargs, ['startup_config', 'driver_startup_config'])

        log.info("Startup Config: %s", self.driver_startup_config)
        log.info("Preload Startup Config: %s", self.config_for_preload(self.driver_startup_config))

        self.initialized = True

    def config_for_preload(self, adict):
        def helper(prefix, dict):
            buffer = ""
            if dict is None or 0 == len(dict):
                return "%s: {}," % ".".join(prefix)

            newprefix = prefix[:]

            for k, v in dict.iteritems():
                if type(v) == type({}):
                    buffer += helper(newprefix + [k], v)
                elif type(v) == type([]):
                    # can't handle lists
                    assert 0 == len(v)
                    buffer += "%s: []," % ".".join(newprefix)
                else:
                    newprefix.append(k)
                    buffer += "%s: %s," % (".".join(newprefix), v)

            return buffer

        return helper([], adict)

    def _build_packet_config(self, param_config):
        """
        Build a packet config from various data types.
        @param packet_config: packet config object. Can be enum, dict or list
        @return list of stream names to create
        """
        params = []
        if isinstance(param_config, list):
            params = param_config

        elif isinstance(param_config, BaseEnum):
            params = param_config.list()

        elif isinstance(param_config, dict):
            params = [value for (key, value) in param_config.items()]

        else:
            log.error("Unknown param_config type")
            return []

        result = []
        for i in params:
            if isinstance(i, tuple):
                log.debug("BLAMMM")
                result.append(i[0])
            else:
                result.append(i)

        return result


class DriverTestMixin(MiUnitTest, ParticleTestMixin):
    """
    Base class for data particle mixin.  Used for data particle validation.
    """
    _raw_sample_parameters = {
        RawDataParticleKey.PAYLOAD: {'type': unicode, 'value': u'SWFtIEFwdWJsaXNoZWQgTWVzc2FnZQ=='},
        RawDataParticleKey.LENGTH: {'type': int, 'value': 22},
        RawDataParticleKey.TYPE: {'type': int, 'value': 2},
        RawDataParticleKey.CHECKSUM: {'type': int, 'value': 2757}
    }

    def assert_particle_raw(self, data_particle, verify_values=False):
        """
        Verify a raw data particles
        @param data_particle SBE26plusTideSampleDataParticle data particle
        @param verify_values bool, should we verify parameter values
        """
        self.assert_data_particle_header(data_particle, CommonDataParticleType.RAW)
        self.assert_data_particle_parameters(data_particle, self._raw_sample_parameters, verify_values)

    def assert_data_particle_parameters(self, data_particle, param_dict, verify_values=False):
        """
        Verify data particle parameters.  Does a quick conversion of the values to a dict
        so that common methods can operate on them.

        @param data_particle the data particle to examine
        @param param_dict dict with parameter names and types
        @param verify_values bool should ve verify parameter values
        """
        sample_dict = self.get_data_particle_values_as_dict(data_particle)
        self.assert_parameters(sample_dict, param_dict, verify_values)

    def assert_driver_parameter_definition(self, driver, param_dict):
        """
        Verify the parameters have been defined as expected in the driver protocol.

        parameter_dict_examples:

         startup: Verifies the parameter is defined as a startup parameter
         directaccess: Verifies the parameter is defined as a direct access parameter
         readonly: Verifies the parameter is defined as a readonly parameter
         verify: Verifies the default value of the parameter

         {
             'some_key': {
                 startup: False
                 directaccess: False
                 readonly: False
                 default: some_value
             }
         }

        @param driver: driver to inspect, must have a protocol defined
        @param param_dict: dict with parameter names and types
        """
        self.assertIsInstance(driver, InstrumentDriver)
        self.assertIsInstance(driver._protocol, InstrumentProtocol)
        self.assertIsInstance(driver._protocol._param_dict, ProtocolParameterDict)
        self.assertIsInstance(param_dict, dict)

        pd = driver._protocol._param_dict

        for (name, config) in param_dict.items():
            log.debug("Verify parameter: %s", name)
            self.assertIsInstance(config, dict)

            startup = config.get(ParameterTestConfigKey.STARTUP)
            da = config.get(ParameterTestConfigKey.DIRECT_ACCESS)
            readonly = config.get(ParameterTestConfigKey.READONLY)
            default = config.get(ParameterTestConfigKey.DEFAULT)

            if da:
                self.assertIn(name, pd.get_direct_access_list(),
                              msg="%s not a direct access parameters %s" % (name, pd.get_direct_access_list()))
            else:
                self.assertNotIn(name, pd.get_direct_access_list(),
                                 msg="%s is a direct access parameters %s" % (name, pd.get_direct_access_list()))

            if startup:
                self.assertIn(name, pd.get_startup_list(), msg="%s is not a startup parameter" % name)
            else:
                self.assertNotIn(name, pd.get_startup_list(), msg="%s is a startup parameter" % name)

            if readonly:
                ro_params = pd.get_visibility_list(ParameterDictVisibility.READ_ONLY) + \
                            pd.get_visibility_list(ParameterDictVisibility.IMMUTABLE)
                self.assertIn(name, ro_params, msg="%s is not a read only parameter" % name)
            else:
                self.assertIn(name, pd.get_visibility_list(ParameterDictVisibility.READ_WRITE),
                              msg="%s is a read only parameter" % name)

            if default:
                self.assertEqual(default, pd.get_default_value(name),
                                 "%s default value incorrect: %s != %s" % (name, default, pd.get_default_value(name)))

    def assert_parameters(self, current_parameters, param_dict, verify_values=False):
        """
        Verify the parameters contain all parameters in the parameter enum and verify the
        types match those defined in the enum.

        parameter_dict_examples:

        There is one record for each parameter. with a dict describing the parameter.

         type: the data type of the parameter.  REQUIRED
         required: is the parameter required, False == not required
         name: Name of the parameter, used for validation if we use constants when defining keys
         value: value of the parameter being validated.  This is useful for unit tests where
                parameters are known.

         Following only used for driver parameter verification
         startup: Verifies the parameter is defined as a startup parameter
         directaccess: Verifies the parameter is defined as a direct access parameter
         readonly: Verifies the parameter is defined as a readonly parameter
         verify: Verifies the default value of the parameter

         {
             'some_key': {
                 type: float,
                 required: False,
                 name: 'some_key',
                 value: 1.1

                 startup: False
                 directaccess: False
                 readonly: False
                 default: some_value
             }
         }

        required defaults to True
        name: defaults to None

        As a short cut you can define the dict with just the type.  The other fields will default
        {
            'some_key': float
        }

        This will verify the type is a float, it is required and we will not validate the key.

        @param current_parameters: list of parameters to examine
        @param param_dict: dict with parameter names and types
        @param verify_values: bool should ve verify parameter values
        """
        self.assertIsInstance(current_parameters, dict)
        self.assertIsInstance(param_dict, dict)

        self.assert_parameter_names(param_dict)
        self.assert_parameter_set(current_parameters, param_dict)
        self.assert_parameter_types(current_parameters, param_dict)

        if verify_values:
            self.assert_parameter_value(current_parameters, param_dict)

    def assert_parameter_names(self, param_dict):
        """
        Verify that the names of the parameter dictionary keys match the parameter value 'name'.  This
        is useful when the parameter dict is built using constants.  If name is none then ignore.

        param_dict:

        {
             'some_key': {
                 type: float,
                 required: False,
                 name: 'some_key'
             }
         }

        @param param_dict: dictionary containing data particle parameter validation values
        """
        for key, param_def in param_dict.items():
            if isinstance(param_def, dict):
                name = param_def.get(ParameterTestConfigKey.NAME)
                if name is not None:
                    self.assertEqual(key, name)

    def assert_parameter_set(self, sample_values, param_dict):
        """
        Verify all required parameters appear in sample_dict as described in param_dict.  Also verify
        that there are no extra values in the sample dict that are not listed as optional in the
        param_dict
        @param sample_values: parsed data particle to inspect
        @param param_dict: dictionary containing parameter validation information
        """
        self.assertIsInstance(sample_values, dict)
        self.assertIsInstance(param_dict, dict)

        required_keys = []
        optional_keys = []

        # get all the sample parameter names
        sample_keys = sample_values.keys()
        log.info("Sample Keys: %s", sample_keys)

        # split the parameters into optional and required
        for key, param in param_dict.items():
            if isinstance(param, dict):
                required = param.get(ParameterTestConfigKey.REQUIRED, True)
                if required:
                    required_keys.append(key)
                else:
                    optional_keys.append(key)
            else:
                required_keys.append(key)

        log.info("Required Keys: %s", required_keys)
        log.info("Optional Keys: %s", optional_keys)

        # Lets verify all required parameters are there
        for required in required_keys:
            self.assertTrue(required in sample_keys, msg="particle missing parameter '%s', a required key" % required)
            sample_keys.remove(required)

        # Now lets look for optional fields and removed them from the parameter list
        for optional in optional_keys:
            if optional in sample_keys:
                sample_keys.remove(optional)

        log.info("Unknown Keys: %s", sample_keys)

        # If there is anything left in the sample keys then it's a problem
        self.assertEqual(len(sample_keys), 0)

    def assert_parameter_value(self, sample_values, param_dict):
        """
        Verify the value in the data particle parameter with the value in the param dict.  This test
        is useful in unit testing when the values are known.
        @param sample_values: parsed data particle to inspect
        @param param_dict: dictionary containing parameter validation information
        """
        for (param_name, param_value) in sample_values.items():
            # get the parameter type
            param_def = param_dict.get(param_name)
            log.debug("Particle Def (%s) ", param_def)
            self.assertIsNotNone(param_def)
            if isinstance(param_def, dict):
                param_type = param_def.get(ParameterTestConfigKey.TYPE)
                self.assertIsNotNone(param_type)
            else:
                param_type = param_def

            try:
                required_value = param_def[ParameterTestConfigKey.VALUE]
                # Only test the equality if the parameter has a value.  Test for required parameters
                # happens in assert_parameter_set
                if param_value is not None:
                    self.assertEqual(param_value, required_value, msg="%s value not equal: %s != %s" % (
                        param_name, repr(param_value), repr(required_value)))
            except KeyError:
                # Ignore key errors
                pass

    def assert_parameter_types(self, sample_values, param_dict):
        """
        Verify all parameters in the sample_dict are of the same type as described in the param_dict
        @param sample_values: parsed data particle to inspect
        @param param_dict: dictionary containing parameter validation information
        """
        for (param_name, param_value) in sample_values.items():
            log.debug("Data Particle Parameter (%s): %s", param_name, type(param_value))

            # get the parameter type
            param_def = param_dict.get(param_name)
            self.assertIsNotNone(param_def)
            if isinstance(param_def, dict):
                param_type = param_def.get(ParameterTestConfigKey.TYPE)
                self.assertIsNotNone(param_type)
            else:
                param_type = param_def

            # is this a required parameter
            if isinstance(param_def, dict):
                required = param_def.get(ParameterTestConfigKey.REQUIRED, True)
            else:
                required = param_def

            if required:
                self.assertIsNotNone(param_value, msg="%s required field None" % param_name)

            if param_value:
                # It looks like one of the interfaces between services converts unicode to string
                # and vice versa.  So if the type is string it can be promoted to unicode so it
                # is still valid.
                if param_type == long and isinstance(param_value, int):
                    # we want type Long, but it is a int instance.  All good
                    pass
                elif param_type == unicode and isinstance(param_value, str):
                    # we want type unicode, but it is a string instance.  All good
                    pass
                else:
                    self.assertIsInstance(param_value, param_type)

    def assert_driver_schema(self, driver, parameters, capabilities, options=None):
        """
        Verify that our driver schema returns the correct values.  If it does not then there is a
        mismatch in the param dict.
        @param driver driver object
        @param parameters dictionary containing information about driver parameters
        @param capabilities dictionary containing information about driver capabilities
        @param options dictionary containing information about driver options
        """
        # This has to come from the protocol so None is returned until we
        # initialize
        self.assert_initialize_driver(driver)
        config = driver.get_config_metadata()
        self.assertIsNotNone(config)

        pp = PrettyPrinter()
        log.debug("Config: %s", pp.pformat(config))

        self.assert_driver_schema_parameters(config, parameters)
        self.assert_driver_schema_capabilities(config, capabilities)
        self.assert_driver_schema_options(config, options)

    def assert_driver_schema_parameters(self, config, parameters):
        """
        verify the parameters returned in the config match the expected parameters passed in.
        @param config driver schema dictionary
        @param parameters dictionary from test mixin describing expected parameters.
        """
        log.debug("Verify driver schema - parameters")
        parameter_dict = config.get(ConfigMetadataKey.PARAMETERS)
        self.assertIsNotNone(parameter_dict)
        self.assertIsInstance(parameters, dict)

        self.assert_driver_schema_parameters_keys(parameter_dict, parameters)

        for key in parameter_dict.keys():
            log.debug("verify driver parameter %s", key)

            config_parameter = parameter_dict.get(key)
            expected_parameter = parameters.get(key)
            self.assertIsNotNone(config_parameter)
            self.assertIsNotNone(expected_parameter)

            self.assert_schema_parameter_type(key, config_parameter, expected_parameter)
            self.assert_schema_parameter_read_only(key, config_parameter, expected_parameter)
            self.assert_schema_parameter_metadata(key, config_parameter)
            self.assert_schema_value_metadata(key, config_parameter)

    def assert_driver_schema_parameters_keys(self, config, parameters):
        """
        verify config returned has the same keys as the expected parameters
        @param config driver schema dictionary
        @param parameters dictionary from test mixin describing expected parameters.
        """
        log.debug("Verify driver parameter sets match")

        self.assertIsInstance(config, dict)
        self.assertIsInstance(parameters, dict)
        self.assertEqual(sorted(config.keys()), sorted(parameters.keys()))

    def assert_schema_parameter_type(self, name, config_parameter, expected_parameter):
        """
        verify config returned describes the type properly
        @param config_parameter parameter as returned from the schema
        @param expected_parameter dictionary with expected parameter info
        """
        log.debug("Verify driver parameter type is defined correctly")
        value_dict = config_parameter.get(ParameterDictKey.VALUE)
        self.assertIsNotNone(value_dict)

        value_type = value_dict.get(ParameterDictKey.TYPE)
        self.assertIsNotNone(value_type, 'value type for %s not define' % name)

        log.debug("parameter '%s' type: %s", name, value_type)
        if value_type == ParameterDictType.FLOAT:
            param_type = float
        elif value_type == ParameterDictType.INT:
            param_type = int
        elif value_type == ParameterDictType.LIST:
            param_type = list
        elif value_type == ParameterDictType.STRING:
            param_type = str
        elif value_type == ParameterDictType.BOOL:
            param_type = bool
        else:
            self.fail("Unknown parameter type: %s" % type)

        expected_type = expected_parameter.get(ParameterTestConfigKey.TYPE)
        self.assertIsNotNone(expected_type)

        self.assertEqual(expected_type, param_type,
                         msg="Type mismatch: %s expected type %s, defined type %s" % (name, expected_type, param_type))

    def assert_schema_parameter_read_only(self, name, config_parameter, expected_parameter):
        """
        verify config returned describes the read only parameters properly
        @param name parameter name
        @param config_parameter parameter as returned from the schema
        @param expected_parameter dictionary with expected parameter info
        """
        param_visibility = config_parameter.get(ParameterDictKey.VISIBILITY)
        self.assertIsNotNone(param_visibility)

        read_only = expected_parameter.get(ParameterTestConfigKey.READONLY, False)

        log.debug("Key: %s, Expected Read-Only: %s, schema visibility: %s", name, read_only, param_visibility)
        if (param_visibility == ParameterDictVisibility.READ_ONLY or
                    param_visibility == ParameterDictVisibility.IMMUTABLE):
            self.assertTrue(read_only, "%s is NOT defined as read-only" % name)
        else:
            self.assertFalse(read_only, "%s is defined as read-only" % name)

    def assert_schema_parameter_metadata(self, name, config_parameter):
        """
        verify parameter has required metadata
        @param name parameter name
        @param config_parameter parameter as returned from the schema
        """
        display_name = config_parameter.get(ParameterDictKey.DISPLAY_NAME)
        self.assertIsNotNone(display_name, "%s has no name defined" % name)

    def assert_schema_value_metadata(self, name, config_parameter):
        """
        verify value has required metadata
        @param name parameter name
        @param config_parameter parameter as returned from the schema
        """
        value = config_parameter.get(ParameterDictKey.VALUE)
        self.assertIsNotNone(value, "%s has no value dict defined")
        self.assertIsInstance(value, dict)

        # nothing more to check here at the moment.  Type has already been verified

    def assert_driver_schema_capabilities(self, config, capabilities):
        """
        verify the parameters returned in the config match the expected capabilities passed in.
        @param config driver schema dictionary
        @param capabilities dictionary from test mixin describing expected capabilities.
        """
        log.debug("Verify driver schema - capabilities")
        capability_dict = config.get(ConfigMetadataKey.COMMANDS)
        self.assertIsNotNone(capability_dict)

        self.assertEqual(sorted(capability_dict.keys()), sorted(capabilities.keys()))

        for key in capability_dict.keys():
            log.debug("verify driver capability %s", key)
            capability = capability_dict.get(key)
            self.assertIsInstance(capability, dict)
            self.assert_driver_schema_capability_metadata(key, capability)

    def assert_driver_schema_capability_metadata(self, name, capability):
        """
        verify required values exist in the schema
        @param name capability name we are checking
        @param capability schema record
        """
        log.debug("Verify capability metadata - %s", name)
        display_name = capability.get(CommandDictKey.DISPLAY_NAME)
        self.assertIsNotNone(display_name, "%s display name not defined in the command dict" % name)

        timeout = capability.get(CommandDictKey.TIMEOUT)
        self.assertIsNotNone(timeout)
        self.assertIsInstance(timeout, int)

    def assert_driver_schema_options(self, config, options):
        """
        verify the parameters returned in the config match the expected options passed in.
        @param config driver schema dictionary
        @param options dictionary from test mixin describing expected options
        """
        log.debug("Verify driver schema - options")
        option_dict = config.get(ConfigMetadataKey.DRIVER)
        self.assertIsNotNone(option_dict)

        vendor_da_support = option_dict.get(DriverDictKey.VENDOR_SW_COMPATIBLE)
        self.assertIsNotNone(vendor_da_support, "%s not defined in driver options" % DriverDictKey.VENDOR_SW_COMPATIBLE)

    def assert_metadata_generation(self, instrument_params=None, commands=None):
        """
        Test that we can generate metadata information for the driver,
        commands, and parameters. Needs a driver to exist first. Metadata
        can come from any source (file or code).
        @param instrument_params The list of parameters to compare with the parameter
        metadata being generated. Could be from an enum class's list() method.
        @param commands The list of commands to compare with the command
        metadata being generated. Could be from an enum class's list() method
        """
        metadata = self.driver_client.cmd_dvr("get_config_metadata")
        self.assert_(metadata is not None)
        log.debug("Metadata response: %s", metadata)
        self.assert_(isinstance(metadata, dict))

        # simple driver metadata check
        self.assertTrue(metadata[ConfigMetadataKey.DRIVER])
        self.assertTrue(metadata[ConfigMetadataKey.DRIVER][DriverDictKey.VENDOR_SW_COMPATIBLE])

        # param metadata check
        self.assertTrue(metadata[ConfigMetadataKey.PARAMETERS])
        keys = metadata[ConfigMetadataKey.PARAMETERS].keys()
        keys.append(DriverParameter.ALL)  # toss that in there to match up
        keys.sort()
        enum_list = instrument_params
        enum_list.sort()
        self.assertEqual(keys, enum_list)

        # command metadata check 
        self.assertTrue(metadata[ConfigMetadataKey.COMMANDS])
        keys = metadata[ConfigMetadataKey.COMMANDS].keys()
        keys.sort()
        enum_list = commands
        enum_list.sort()
        self.assertEqual(keys, enum_list)


class InstrumentDriverTestCase(MiIntTestCase):
    """
    Base class for instrument driver tests
    """

    # configuration singleton
    test_config = InstrumentDriverTestConfig()

    @classmethod
    def initialize(cls, *args, **kwargs):
        """
        Initialize the test_configuration singleton
        """
        cls.test_config.initialize(*args, **kwargs)

    # Port agent process object.
    port_agent = None

    def setUp(self):
        """
        @brief Setup test cases.
        """
        log.debug("*********************************************************************")
        log.debug("Starting Test %s", self._testMethodName)
        log.debug("*********************************************************************")
        log.debug("ID: %s", self.id())
        log.debug("InstrumentDriverTestCase setUp")

        # Test to ensure we have initialized our test config
        if not self.test_config.initialized:
            return TestNotInitialized(msg="Tests non initialized. Missing InstrumentDriverTestCase.initialize(...)?")

        self.clear_events()

    def tearDown(self):
        """
        @brief Test teardown
        """
        log.debug("InstrumentDriverTestCase tearDown")

    def get_ntp_timestamp(self, unix_time=None):
        """
        Get an ntp timestamp using the passed in unix_time
        or the current time if not passed
        @param unix_time: unix timestamp to create
        @return: ntp timestamp
        """
        if unix_time is None:
            unix_time = time.time()

        return ntplib.system_to_ntp_time(unix_time)

    def clear_events(self):
        """
        @brief Clear the event list.
        """
        self.events = []

    def get_events(self, event_type=None):
        """
        return a list of events received.  If a type is passed then the list
        will only contain events of that type.
        @param event_type: type of event we are looking for
        """
        if event_type is None:
            return self.events
        else:
            return [evt for evt in self.events if evt['type'] == event_type]

    def get_sample_events(self, event_type=None):
        """
        Get a list of sample events, potentially of a passed in type
        @param event_type: what type of data particle are we looking for
        @return: list of data sample events
        """
        samples = self.get_events(DriverAsyncEvent.SAMPLE)
        if event_type is None:
            return samples
        else:
            result = []
            for evt in samples:
                particle = evt.get('value')
                if particle and particle.get('stream_name') == event_type:
                    result.append(evt)

            return result

    def event_received(self, evt):
        """
        @brief Simple callback to catch events from the driver for verification.
        """
        self.events.append(evt)

    @staticmethod
    def create_serial_comm_config(comm_config):
        return {
            'instrument_type': ConfigTypes.SERIAL,
            'port_agent_addr': comm_config.host,
            'device_os_port': comm_config.device_os_port,
            'device_baud': comm_config.device_baud,
            'device_data_bits': comm_config.device_data_bits,
            'device_stop_bits': comm_config.device_stop_bits,
            'device_flow_control': comm_config.device_flow_control,
            'device_parity': comm_config.device_parity,
            'command_port': comm_config.command_port,
            'data_port': comm_config.data_port,
            'telnet_sniffer_port': comm_config.sniffer_port,
     #       'process_type': PortAgentProcessType.UNIX,
            'log_level': 5,
        }

    @staticmethod
    def create_ethernet_comm_config(comm_config):
        config = {
            'instrument_type': ConfigTypes.ETHERNET,
            'port_agent_addr': comm_config.host,
            'device_addr': comm_config.device_addr,
            'device_port': comm_config.device_port,
            'command_port': comm_config.command_port,
            'data_port': comm_config.data_port,
            'telnet_sniffer_port': comm_config.sniffer_port,
        #    'process_type': PortAgentProcessType.UNIX,
            'log_level': 5,
        }
        return config

    @staticmethod
    def create_rsn_comm_config(comm_config):
        config = {
            'instrument_type': ConfigTypes.RSN,
            'port_agent_addr': comm_config.host,
            'device_addr': comm_config.device_addr,
            'device_port': comm_config.device_port,
            'command_port': comm_config.command_port,
            'instrument_command_port': comm_config.instrument_command_port,
            'data_port': comm_config.data_port,
            'telnet_sniffer_port': comm_config.sniffer_port,
        #    'process_type': PortAgentProcessType.UNIX,
            'log_level': 8,
        }
        return config

    def create_botpt_comm_config(self, comm_config):
        config = {
            'instrument_type': ConfigTypes.BOTPT,
            'port_agent_addr': comm_config.host,
            'device_addr': comm_config.device_addr,
            'command_port': comm_config.command_port,
            'data_port': comm_config.data_port,
            'telnet_sniffer_port': comm_config.sniffer_port,
     #       'process_type': PortAgentProcessType.UNIX,
            'device_tx_port': comm_config.device_tx_port,
            'device_rx_port': comm_config.device_rx_port,
            'log_level': 5,
        }
        return config

    def create_multi_comm_config(self, comm_config):
        result = {}
        for name, config in comm_config.configs.items():
            if config.method() == ConfigTypes.ETHERNET:
                result[name] = self.create_ethernet_comm_config(config)
            elif config.method() == ConfigTypes.SERIAL:
                result[name] = self.create_serial_comm_config(config)
        return result

    @classmethod
    def comm_config_file(cls):
        """
        @brief Return the path the the driver comm config yaml file.
        @return if comm_config.yml exists return the full path
        """
        repo_dir = Config().get('working_repo')
        driver_path = cls.test_config.driver_module.replace('.', '/')
        abs_path = "%s/%s/%s" % (repo_dir, os.path.dirname(driver_path), CommConfig.config_filename())

        log.debug(abs_path)
        return abs_path

    @classmethod
    def get_comm_config(cls):
        """
        @brief Create the comm config object by reading the comm_config.yml file.
        """
        log.info("get comm config")
        config_file = cls.comm_config_file()

        log.debug(" -- reading comm config from: %s" % config_file)
        if not os.path.exists(config_file):
            raise TestNoCommConfig(msg="Missing comm config.  Try running start_driver or switch_driver")

        return CommConfig.get_config_from_file(config_file)

    def port_agent_config(self):
        """
        return the port agent configuration
        """
        comm_config = self.get_comm_config()
        log.debug('comm_config = %r', comm_config.__dict__)
        method = comm_config.method()
        config = {}

        if method == ConfigTypes.SERIAL:
            config = self.create_serial_comm_config(comm_config)
        elif method == ConfigTypes.ETHERNET:
            config = self.create_ethernet_comm_config(comm_config)
        elif method == ConfigTypes.RSN:
            config = self.create_rsn_comm_config(comm_config)
        elif method == ConfigTypes.BOTPT:
            config = self.create_botpt_comm_config(comm_config)
        elif method == ConfigTypes.MULTI:
            config = self.create_multi_comm_config(comm_config)

        config['instrument_type'] = comm_config.method()

        if comm_config.sniffer_prefix:
            config['telnet_sniffer_prefix'] = comm_config.sniffer_prefix
        if comm_config.sniffer_suffix:
            config['telnet_sniffer_suffix'] = comm_config.sniffer_suffix

        return config

    def init_port_agent(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @retval return the pid to the logger process
        """
        # if self.port_agent:
        #     log.error("Port agent already initialized")
        #     return
        #
        # log.debug("Startup Port Agent")
        #
        # config = self.port_agent_config()
        # log.debug("port agent config: %s", config)
        #
        # port_agent = PortAgentProcess.launch_process(config, timeout=60, test_mode=True)
        #
        # port = port_agent.get_data_port()
        # pid = port_agent.get_pid()
        #
        # if self.get_comm_config().host == LOCALHOST:
        #     log.info('Started port agent pid %s listening at port %s' % (pid, port))
        # else:
        #     log.info("Connecting to port agent on host: %s, port: %s", self.get_comm_config().host, port)
        #
        # self.addCleanup(self.stop_port_agent)
        # self.port_agent = port_agent
        # return port

    def stop_port_agent(self):
        """
        Stop the port agent.
        """
        log.info("Stop port agent")
        if self.port_agent:
            log.debug("found port agent, now stop it")
            self.port_agent.stop()
        self.port_agent = None

    def init_driver_process_client(self):
        """
        @brief Launch the driver process and driver client
        @retval return driver process and driver client object
        """
        log.info("Startup Driver Process")

        driver_config = {
            'dvr_mod': self.test_config.driver_module,
            'dvr_cls': self.test_config.driver_class,
            'workdir': self.test_config.working_dir,
            'comms_config': self.port_agent_comm_config(),
            'process_type': (self.test_config.driver_process_type,),
            'startup_config': self.test_config.driver_startup_config
        }

        self.driver_process = DriverProcess.get_process(driver_config, True)
        self.driver_process.launch()

        # Verify the driver has started.
        if not self.driver_process.getpid():
            log.error('Error starting driver process.')
            raise InstrumentException('Error starting driver process.')

        try:
            driver_client = self.driver_process.get_client()
            driver_client.start_messaging(self.event_received)
            log.debug("before 'process_echo'")
            retval = driver_client.cmd_dvr('process_echo', 'Test.')  # data=? RU
            log.debug("after 'process_echo'")

            startup_config = driver_config.get('startup_config')
            log.debug("Setting Startup Params: %s", startup_config)
            retval = driver_client.cmd_dvr('set_init_params', startup_config)

            self.driver_client = driver_client
        except Exception, e:
            self.driver_process.stop()
            log.error('Error starting driver client. %s', e)
            raise InstrumentException('Error starting driver client.')

        log.info('started its driver.')

    def stop_driver_process_client(self):
        """
        Stop the driver_process.
        """
        if self.driver_process:
            self.driver_process.stop()

    def port_agent_comm_config(self):
        port = self.port_agent.get_data_port()
        cmd_port = self.port_agent.get_command_port()

        return {
            'addr': self.get_comm_config().host,
            'port': port,
            'cmd_port': cmd_port
        }

    #####
    # Custom assert methods
    #####

    def assert_enum_complete(self, subset, superset):
        """
        Assert that every item in an enum is in superset
        """
        self.assert_set_complete(convert_enum_to_dict(subset),
                                 convert_enum_to_dict(superset))

    def assert_set_complete(self, subset, superset):
        """
        Assert that every item in subset is in superset
        """
        # use assertTrue here intentionally because it's easier to unit test
        # this method.
        if len(superset):
            self.assertTrue(len(subset) > 0)

        for item in subset:
            self.assertTrue(item in superset)

            # This added so the unit test can set a true flag.  If we have made it
            # this far we should pass the test.
            #self.assertTrue(True)

    def assert_enum_has_no_duplicates(self, obj):
        dic = convert_enum_to_dict(obj)
        occurrences = {}
        for k, v in dic.items():
            #v = tuple(v)
            occurrences[v] = occurrences.get(v, 0) + 1

        for k in occurrences:
            if occurrences[k] > 1:
                log.error(str(obj) + " has ambiguous duplicate values for '" + str(k) + "'")
                self.assertEqual(1, occurrences[k])

    def assert_chunker_sample(self, chunker, sample):
        """
        Verify the chunker can parse a sample that comes in a single string
        @param chunker: Chunker to use to do the parsing
        @param sample: raw sample
        """
        ts = self.get_ntp_timestamp()
        chunker.add_chunk(sample, ts)
        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(ts, timestamp)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

    def assert_chunker_fragmented_sample(self, chunker, sample, fragment_size=1):
        """
        Verify the chunker can parse a sample that comes in fragmented.  It sends bytes to the chunker
        one at a time.  This very slow for large chunks (>4k) so we don't want to increase the sample
        part size
        @param chunker: Chunker to use to do the parsing
        @param sample: raw sample
        """
        sample_length = len(sample)
        self.assertGreater(fragment_size, 0)

        # If the sample length is less then the fragment size then we aren't going to fragment the sample and
        # this test isn't verifying what we are trying to verify.
        self.assertGreater(sample_length, fragment_size, msg="Fragment size must be greater than sample length")
        timestamps = []

        i = 0
        while i < sample_length:
            ts = self.get_ntp_timestamp()
            timestamps.append(ts)
            end = i + fragment_size
            chunker.add_chunk(sample[i:end], ts)
            (timestamp, result) = chunker.get_next_data()
            if result: break
            i += fragment_size

        self.assertEqual(result, sample)
        self.assertEqual(timestamps[0], timestamp)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

    def assert_chunker_combined_sample(self, chunker, sample):
        """
        Verify the chunker can parse a sample that comes in combined
        @param chunker: Chunker to use to do the parsing
        @param sample: raw sample
        """
        ts = self.get_ntp_timestamp()
        chunker.add_chunk(sample + sample, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

    def assert_chunker_sample_with_noise(self, chunker, sample):
        """
        Verify the chunker can parse a sample with noise on the
        front or back of sample data
        @param chunker: Chunker to use to do the parsing
        @param sample: raw sample
        """
        noise = "this is a bunch of noise to add to the sample\r\n"
        ts = self.get_ntp_timestamp()

        # Try a sample with noise in the front
        chunker.add_chunk(noise + sample, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

        # Now some noise in the back
        chunker.add_chunk(sample + noise, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

        # There should still be some noise in the buffer, make sure
        # we can still take a sample.
        chunker.add_chunk(sample + noise, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)


class InstrumentDriverUnitTestCase(InstrumentDriverTestCase):
    """
    Base class for instrument driver unit tests
    """
    _data_particle_received = []

    def tearDown(self):
        # schedulers need to be shutdown before GC will occur
        for obj in gc.get_objects():
            if type(obj) == DriverScheduler:
                if hasattr(obj, '_scheduler'):
                    obj._scheduler.shutdown()
                    del obj._scheduler

    def clear_data_particle_queue(self):
        """
        Reset the queue which stores data particles received via our
        custome event callback.
        """
        self._data_particle_received = []

    def _got_data_event_callback(self, event):
        """
        Event call back method sent to the driver.  It simply grabs a sample event and pushes it
        into the data particle queue
        """
        event_type = event['type']
        if event_type == DriverAsyncEvent.SAMPLE:
            sample_value = event['value']
            self._data_particle_received.append(sample_value)

    def compare_parsed_data_particle(self, particle_type, raw_input, happy_structure):
        """
        Compare a data particle created with the raw input string to the structure
        that should be generated.
        
        @param The data particle class to create
        @param raw_input The input string that is instrument-specific
        @param happy_structure The structure that should result from parsing the
            raw input during DataParticle creation
        """
        port_timestamp = happy_structure[DataParticleKey.PORT_TIMESTAMP]
        if DataParticleKey.INTERNAL_TIMESTAMP in happy_structure:
            internal_timestamp = happy_structure[DataParticleKey.INTERNAL_TIMESTAMP]
            test_particle = particle_type(raw_input, port_timestamp=port_timestamp,
                                          internal_timestamp=internal_timestamp)
        else:
            test_particle = particle_type(raw_input, port_timestamp=port_timestamp)

        parsed_result = test_particle.generate(sorted=True)

        driver_time = parsed_result[DataParticleKey.DRIVER_TIMESTAMP]
        happy_structure[DataParticleKey.DRIVER_TIMESTAMP] = driver_time

        self.assertEqual(parsed_result, happy_structure)

    def assert_force_state(self, driver, protocol_state):
        """
        For the driver state to protocol_state
        @param driver: Instrument driver instance.
        @param protocol_state: State to transistion to
        """
        driver.test_force_state(state=protocol_state)
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, protocol_state)

    def assert_driver_connected(self, driver, initial_protocol_state=DriverProtocolState.AUTOSAMPLE):
        """
        Check to see if the driver is connected, if it isn't then initialize the driver.  Finally
        force the instrument state to the initial_protocol_state.
        @param driver: Instrument driver instance.
        @param initial_protocol_state: the state to force the driver too
        """
        current_state = driver.get_resource_state()
        if current_state == DriverConnectionState.UNCONFIGURED:
            self.assert_initialize_driver(driver, initial_protocol_state)
        else:
            self.assert_force_state(driver, initial_protocol_state)

    def assert_initialize_driver(self, driver, initial_protocol_state=DriverProtocolState.AUTOSAMPLE):
        """
        Initialize an instrument driver with a mock port agent.  This will allow us to test the
        got data method.  Will the instrument, using test mode, through it's connection state
        machine.  End result, the driver will be in test mode and the connection state will be
        connected.
        @param driver: Instrument driver instance.
        @param initial_protocol_state: the state to force the driver too
        """
        mock_port_agent = Mock(spec=PortAgentClient)

        # Put the driver into test mode
        driver.set_test_mode(True)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.UNCONFIGURED)

        # Now configure the driver with the mock_port_agent, verifying
        # that the driver transitions to that state
        config = {'mock_port_agent': mock_port_agent}
        driver.configure(config=config)

        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverConnectionState.DISCONNECTED)

        # Invoke the connect method of the driver: should connect to mock
        # port agent.  Verify that the connection FSM transitions to CONNECTED,
        # (which means that the FSM should now be reporting the ProtocolState).
        driver.connect()
        current_state = driver.get_resource_state()
        self.assertEqual(current_state, DriverProtocolState.UNKNOWN)

        # Force the instrument into a known state
        self.assert_force_state(driver, initial_protocol_state)

    def assert_raw_particle_published(self, driver, assert_callback, verify_values=False):
        """
        Verify that every call to got_data publishes a raw data particle

        Create a port agent packet, send it through got_data, then finally grab the data particle
        from the data particle queue and verify it using the passed in assert method.
        @param driver: instrument driver with mock port agent client
        @param verify_values: Should we validate values?
        """
        sample_data = SAMPLE_RAW_DATA
        log.debug("Sample to publish: %s", sample_data)

        # Create and populate the port agent packet.
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(sample_data)
        port_agent_packet.pack_header()

        self.clear_data_particle_queue()

        # Push the data into the driver
        driver._protocol.got_raw(port_agent_packet)
        self.assertEqual(len(self._data_particle_received), 1)
        particle = self._data_particle_received.pop()
        log.debug("Raw Particle: %s", particle)

        # Verify the data particle
        self.assert_particle_raw(particle, verify_values)

    def assert_particle_published(self, driver, sample_data, particle_assert_method, verify_values=False):
        """
        Verify that we can send data through the port agent and the the correct particles
        are generated.

        Create a port agent packet, send it through got_data, then finally grab the data particle
        from the data particle queue and verify it using the passed in assert method.
        @param driver: instrument driver with mock port agent client
        @param sample_data: the byte string we want to send to the driver
        @param particle_assert_method: assert method to validate the data particle.
        @param verify_values: Should we validate values?
        """
        ts = ntplib.system_to_ntp_time(time.time())

        log.debug("Sample to publish: %s", sample_data)
        # Create and populate the port agent packet.
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(sample_data)
        port_agent_packet.attach_timestamp(ts)
        port_agent_packet.pack_header()

        self.clear_data_particle_queue()

        # Push the data into the driver
        driver._protocol.got_data(port_agent_packet)

        # Find all particles of the correct data particle types (not raw)
        particles = []
        for p in self._data_particle_received:
            stream_type = p.get('stream_name')
            self.assertIsNotNone(stream_type)
            if stream_type != CommonDataParticleType.RAW:
                particles.append(p)

        log.debug("Non raw particles: %s ", particles)
        self.assertEqual(len(particles), 1)

        # Verify the data particle
        particle_assert_method(particles.pop(), verify_values)

    def assert_capabilities(self, driver, capabilities):
        """
        Verify all capabilities expected are in the FSM.  Then verify that the capabilities
        available for each state.
        @param driver: a mocked up driver
        @param capabilities: dictionary with protocol state as the key and a list as expected capabilities
        """
        self.assert_protocol_states(driver, capabilities)
        self.assert_all_capabilities(driver, capabilities)
        self.assert_state_capabilities(driver, capabilities)

    def assert_protocol_states(self, driver, capabilities):
        """
        Verify that the protocol states defined in the driver match the list of states in the
        capabilities dictionary.
        @param driver: a mocked up driver
        @param capabilities: dictionary with protocol state as the key and a list as expected capabilities
        """
        self.assert_driver_connected(driver)
        fsm_states = sorted(driver._protocol._protocol_fsm.states.list())
        self.assertTrue(fsm_states)

        expected_states = sorted(capabilities.keys())

        log.debug("Defined Protocol States: %s", fsm_states)
        log.debug("Expected Protocol States: %s", expected_states)

        self.assertEqual(fsm_states, expected_states)

    def assert_all_capabilities(self, driver, capabilities):
        """
        Build a list of all the capabilities in the passed in dict and verify they are all available
        in the FSM
        @param driver: a mocked up driver
        @param capabilities: dictionary with protocol state as the key and a list as expected capabilities
        """
        self.maxDiff = None
        self.assert_driver_connected(driver)
        all_capabilities = sorted(driver._protocol._protocol_fsm.get_events(current_state=False))
        expected_capabilities = []

        for (state, capability_list) in capabilities.items():
            for s in capability_list:
                if not s in expected_capabilities:
                    expected_capabilities.append(s)

        expected_capabilities.sort()

        log.debug("All Reported Capabilities: %s", all_capabilities)
        log.debug("All Expected Capabilities: %s", expected_capabilities)

        self.assertEqual(all_capabilities, expected_capabilities)

    def assert_state_capabilities(self, driver, capabilities):
        """
        Walk through all instrument states and verify fsm capabilities available
        as reported by the driver.
        @param driver: a mocked up driver
        @param capabilities: dictionary with protocol state as the key and a list as expected capabilities
        """
        self.assert_driver_connected(driver)
        driver.set_test_mode(True)

        # Verify state specific capabilities
        for (state, capability_list) in capabilities.items():
            self.assert_force_state(driver, state)
            reported_capabilities = sorted(driver._protocol._protocol_fsm.get_events(current_state=True))
            expected_capabilities = sorted(capability_list)

            log.debug("Current Driver State: %s", state)
            log.debug("Expected Capabilities: %s", expected_capabilities)
            log.debug("Reported Capabilities: %s", reported_capabilities)

            self.assertEqual(reported_capabilities, expected_capabilities)


class InstrumentDriverIntegrationTestCase(InstrumentDriverTestCase):  # Must inherit from here to get _start_container
    def setUp(self):
        """
        @brief Setup test cases.
        """
        log.debug("InstrumentDriverIntegrationTestCase.setUp")
        self.init_port_agent()
        InstrumentDriverTestCase.setUp(self)

        log.debug("InstrumentDriverIntegrationTestCase setUp")
        self.init_driver_process_client()
        self.clear_events()

    def tearDown(self):
        """
        @brief Test teardown
        """
        log.debug("InstrumentDriverIntegrationTestCase tearDown")
        self.stop_driver_process_client()

        InstrumentDriverTestCase.tearDown(self)

    ###
    #   Common assert methods
    ###
    def assert_current_state(self, target_state):
        """
        Verify the driver state
        @param state:
        @return:
        """
        state = self.driver_client.cmd_dvr('get_resource_state')
        self.assertEqual(state, target_state)

    def assert_state_change(self, target_state, timeout):
        """
        Verify the driver state changes within a given timeout period.
        Fail if the state doesn't change to the expected state.
        @param target_state: State we expect the protocol to be in
        @param timeout: how long to wait for the driver to change states
        """
        end_time = time.time() + timeout

        while time.time() <= end_time:
            state = self.driver_client.cmd_dvr('get_resource_state')
            if state == target_state:
                log.debug("Current state match: %s", state)
                return
            log.debug("state mismatch %s != %s, sleep for a bit", state, target_state)
            time.sleep(2)

        log.error("Failed to transition state to %s, current state: %s", target_state, state)
        self.fail("Failed to transition state to %s, current state: %s" % (target_state, state))

    def assert_initialize_driver(self, final_state=DriverProtocolState.COMMAND):
        """
        Walk an uninitialized driver through it's initialize process.  Verify the final
        state is command mode.  If the final state is auto sample then we will stop
        which should land us in autosample
        """
        # Test the driver is in state unconfigured.
        self.assert_current_state(DriverConnectionState.UNCONFIGURED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('configure', self.port_agent_comm_config())

        # Test the driver is configured for comms.
        self.assert_current_state(DriverConnectionState.DISCONNECTED)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('connect')

        # Test the driver is in unknown state.
        self.assert_current_state(DriverProtocolState.UNKNOWN)

        # Configure driver for comms and transition to disconnected.
        reply = self.driver_client.cmd_dvr('discover_state')

        # If we are in streaming mode then stop streaming
        state = self.driver_client.cmd_dvr('get_resource_state')
        if state == DriverProtocolState.AUTOSAMPLE and final_state != DriverProtocolState.AUTOSAMPLE:
            log.debug("Stop autosample because we want to be in command mode")
            reply = self.driver_client.cmd_dvr('execute_resource', DriverEvent.STOP_AUTOSAMPLE)
            state = self.driver_client.cmd_dvr('get_resource_state')

        if state == DriverProtocolState.COMMAND and final_state != DriverProtocolState.COMMAND:
            log.debug("Start autosample because we don't want to be in command mode")
            reply = self.driver_client.cmd_dvr('execute_resource', DriverEvent.START_AUTOSAMPLE)
            state = self.driver_client.cmd_dvr('get_resource_state')

        log.debug("initialize final state: %s", state)
        # Test the driver is in the correct mode
        if final_state == DriverProtocolState.AUTOSAMPLE:
            self.assertEqual(state, DriverProtocolState.AUTOSAMPLE)
        else:
            self.assertEqual(state, DriverProtocolState.COMMAND)

    def assert_startup_parameters(self, parameter_assert, new_values=None, get_values=None):
        """
        Verify that driver startup parameters are set properly.  To
        Do this we first test all parameters using the mixin class.
        This assumes that you have values in the driver parameter
        config structure.  This is defined in the mixin class.

        After we have checked the parameters we will force the driver to
        re-apply startup params

        @param parameter_assert: callback to parameter assert method
        @param new_values: values to change on the instrument
        @param get_values: optional values to explicitly check after discover
        """
        if get_values:
            reply = self.driver_client.cmd_dvr('get_resource', DriverParameter.ALL)
            parameter_assert(reply, True)

        if get_values is not None:
            for (key, val) in get_values.iteritems():
                self.assert_get(key, val)

        if new_values:
            self.assert_set_bulk(new_values)

        # Force a reapply
        reply = self.driver_client.cmd_dvr('apply_startup_params')

        ###
        #   Rinse and repeat
        ###

        # Should be back to our startup parameters.
        reply = self.driver_client.cmd_dvr('get_resource', DriverParameter.ALL)
        parameter_assert(reply, True)

        if get_values is not None:
            for (key, val) in get_values.iteritems():
                self.assert_get(key, val)

    def assert_get(self, param, value=None, pattern=None):
        """
        Verify we can get a parameter and compare the fetched value
        with the expected value.
        @param param: parameter to set
        @param value: expected parameter value
        @param pattern: expected parameter pattern
        @return value of the parameter
        """
        reply = self.driver_client.cmd_dvr('get_resource', [param])
        self.assertIsInstance(reply, dict)
        return_value = reply.get(param)

        if value is not None:
            self.assertEqual(return_value, value, msg="%s no value match (%s != %s)" % (param, return_value, value))
        elif pattern is not None:
            self.assertRegexpMatches(str(return_value), pattern,
                                     msg="%s no value match (%s != %s)" % (param, return_value, value))

        return return_value

    def assert_set(self, param, value, no_get=False, startup=False):
        """
        Verify we can set a parameter and then do a get to confirm. Also, unless
        no_get is specified, verify a config change event is sent when values
        change and none when they aren't changed.
        @param param: parameter to set
        @param param: no_get if true don't verify set with a get.
        @param param: startup is this simulating a startup set?
        @param value: what to set the parameter too
        """
        if no_get:
            reply = self.driver_client.cmd_dvr('set_resource', {param: value}, startup)
            self.assertIsNone(reply, None)
        else:
            log.debug("assert_set, check values and events")
            self.clear_events()
            current_value = self.assert_get(param)
            config_change = current_value != value

            log.debug("current value: %s new value: %s, config_change: %s", current_value, value, config_change)
            self.assert_set(param, value, True)
            self.assert_get(param, value)

            time.sleep(1)
            events = self.get_events(DriverAsyncEvent.CONFIG_CHANGE)

            log.debug("got config change events: %d", len(events))
            if config_change:
                self.assertTrue(len(events) > 0)
            else:
                self.assertEqual(len(events), 0)

            # Let's set it again.  This time we know it shouldn't generate a
            # config change event
            self.clear_events()
            self.assert_set(param, value, True)
            time.sleep(1)
            events = self.get_events(DriverAsyncEvent.CONFIG_CHANGE)
            log.debug("pass #2 got config change events: %d", len(events))
            self.assertEqual(len(events), 0)

            self.assert_get(param, value)

    def assert_set_bulk(self, param_dict):
        """
        Verify we can bulk set parameters.  First bulk set the parameters and
        then verify with individual gets.
        @param param_dict: dictionary with parameter as key with it's value.
        """
        self.assertIsInstance(param_dict, dict)

        reply = self.driver_client.cmd_dvr('set_resource', param_dict)
        self.assertIsNone(reply, None)

        for (key, value) in param_dict.items():
            self.assert_get(key, value)

    def assert_set_bulk_exception(self, param_dict, error_regex=None, exception_class=InstrumentParameterException):
        """
        Verify a bulk set raises an exception
        then verify with individual gets.
        @param param_dict: dictionary with parameter as key with it's value.
        @param error_regex: error message pattern to match
        @param exception_class: class of the exception raised
        """
        try:
            self.assert_set_bulk(param_dict)
        except Exception as e:
            if self._driver_exception_match(e, exception_class, error_regex):
                log.debug("Expected exception raised: %s", e)
                return
            else:
                self.fail("Unexpected exception: %s" % e)

        # If we have made it this far then no exception was raised
        self.fail("No exception raised")

    def assert_set_exception(self, param, value='dummyvalue', error_regex=None,
                             exception_class=InstrumentParameterException):
        """
        Verify that a set command raises an exception on set.
        @param param: parameter to set
        @param value: what to set the parameter too
        @param error_regex: error message pattern to match
        @param exception_class: class of the exception raised
        """
        try:
            reply = self.driver_client.cmd_dvr('set_resource', {param: value})
            self.assert_set(param, value)
        except Exception as e:
            if self._driver_exception_match(e, exception_class, error_regex):
                log.debug("Expected exception raised: %s", e)
                return
            else:
                self.fail("Unexpected exception: %s" % e)

        # If we have made it this far then no exception was raised
        self.fail("No exception raised")

    def assert_ion_exception(self, ex, call, *args):
        """
        Take a generic ION exception that comes in as a BadRequest, parse it
        and verify that it is the correct type of exception. This gets around
        the hiding of exceptions that the ION system does.
        @param ex The exception we are expecting
        @param call The method to call
        @param args The args to add to that call to get it to go
        """
        try:
            call(*args)
        except BadRequest as badreq:
            if self._driver_exception_match(badreq, ex):
                log.debug("Expected exception raised: %s", ex)
                return
        except ResourceError as e:
            if self._driver_exception_match(e, ex):
                log.debug("Expected exception raised as ResourceError: %s", ex)
                return
        except Timeout as e:
            if self._driver_exception_match(e, ex):
                log.debug("Expected exception raised as Timeout: %s", ex)
                return
        except ServerError as e:
            if self._driver_exception_match(e, ex):
                log.debug("Expected exception raised as ServerError: %s", ex)
                return
        except Conflict as e:
            if self._driver_exception_match(e, ex):
                log.debug("Expected exception raised as Conflict: %s", ex)
                return
        except Exception as e:
            log.debug("Exception type: %s", type(e))
            self.fail("Call returned bad exception: %s of type %s" % (e, type(e)))

    def _driver_exception_match(self, ion_exception, expected_exception, error_regex=None):
        """
        This will attempt to verify the ion exception raised was generated from the
        instrument driver exception we expected to see.  We do this by examining the
        value of the exception and attempt to parse it.  The format should be:
        code - DriverExceptionName: error message
        @param ion_exception: The ion exception that was raised
        @param expected_exception:  The instrument driver exception we should see
        @param regex: regular express to match the error message
        @return: True if a match otherwise False
        """
        pattern = r'(\d{3}) - (\w+): (.*)'
        matcher = re.compile(pattern)
        match = re.search(matcher, str(ion_exception))

        if match:
            log.debug("Exception code: %s, type: %s, value: %s", match.group(1), match.group(2), match.group(3))
            log.debug("Expected exception type: %s", expected_exception.__name__)

            if expected_exception.__name__ != match.group(2):
                log.error("Exception type mismatch %s != %s", expected_exception.__name__, match.group(2))
                return False

            if error_regex is not None:
                log.debug("Checking for a value match: %s", error_regex)
                matcher = re.compile(error_regex)
                if not matcher.search(match.group(3)):
                    log.error("value pattern mismatch %s", match.group(3))
                    return False
        else:
            log.error("Failed to match driver exception pattern")
            return False

        return True

    def assert_set_readonly(self, param, value='dummyvalue', exception_class=InstrumentParameterException):
        """
        Verify that a set command raises an exception on set.
        @param param: parameter to set
        @param value: what to set the parameter too
        """
        self.assert_set_exception(param, value, exception_class=exception_class)

    def assert_driver_command(self, command, expected=None, regex=None, value_function=None, state=None, delay=0,
                              regex_options=re.DOTALL, assert_function=None):
        """
        Verify that we can run a command and that the reply matches if we have
        passed on in.  If we couldn't execute a command we assume an exception
        will be thrown.
        @param command: driver command to execute
        @param expected: expected reply from the command
        @param regex: regex to match reply
        @param value_function: function that will return a value to be tested
        @param state: desired protocol state after the command is run.
        @param delay: how long to wait, in seconds, after the command is executed before we can run tests
        @param regex_options: options to pass to the regular expression compile
        @param assert_function: assert method to call
        """
        # Execute the command
        reply = value = self.driver_client.cmd_dvr('execute_resource', command, )
        log.debug("Execute driver command: %s", command)
        log.debug("Reply type: %s", type(reply))

        if delay:
            log.debug("sleeping for a bit: %d", delay)
            time.sleep(delay)

        # Get the value to check in the reply
        if reply is not None:
            if value_function is None:
                self.assertIsInstance(reply, tuple)
                value = reply[1]
            else:
                value = value_function(reply)

        if expected is not None:
            log.debug("command reply: %s", value)
            self.assertIsNotNone(value)
            self.assertEqual(value, expected)

        if regex is not None:
            log.debug("command reply: %s", value)
            self.assertIsNotNone(value)
            compiled = re.compile(regex, regex_options)
            self.assertRegexpMatches(value, compiled)

        if state is not None:
            self.assert_current_state(state)

    def assert_driver_command_exception(self, command, error_regex=None, exception_class=InstrumentStateException):
        """
        Verify a driver command throws an exception
        then verify with individual gets.
        @param command: driver command to execute
        @param error_regex: error message pattern to match
        @param exception_class: class of the exception raised
        """
        try:
            self.driver_client.cmd_dvr(command)
        except Exception as e:
            if self._driver_exception_match(e, exception_class, error_regex):
                log.debug("Expected exception raised: %s", e)
                return
            else:
                self.fail("Unexpected exception: %s" % e)

        # If we have made it this far then no exception was raised
        self.fail("No exception raised")

    def assert_particle_generation(self, command, particle_type, particle_callback, delay=1, timeout=None):
        """
        Verify we can generate a particle via a command.
        @param command: command used to generate the particle
        @param particle_type: particle type we are looking for
        @param particle_callback: callback used to validate the particle
        @param timeout: command timeout
        """
        self.assert_driver_command(command, delay=delay)

        samples = self.get_sample_events(particle_type)
        self.assertGreaterEqual(len(samples), 1)

        sample = samples.pop()
        self.assertIsNotNone(sample)

        value = sample.get('value')
        self.assertIsNotNone(value)

        particle_callback(value)

    def assert_async_particle_generation(self, particle_type, particle_callback, particle_count=1, timeout=10):
        """
        Watch the event queue for a published data particles.
        @param particle_type: particle type we are looking for
        @param particle_callback: callback used to validate the particle
        @param timeout: how long should we wait for a particle
        """
        end_time = time.time() + timeout

        while True:
            samples = self.get_sample_events(particle_type)
            if len(samples) >= particle_count:
                for sample in samples:
                    self.assertIsNotNone(sample)

                    value = sample.get('value')
                    self.assertIsNotNone(value)

                    self.assertIsNotNone(value)

                    # So we have found one particle and verified it.  We are done here!
                    particle_callback(value)
                log.debug('Found %d particles and all particles verified', len(samples))
                return

            log.trace("Only found %d samples, looking for %d", len(samples), particle_count)
            self.assertGreater(end_time, time.time(), msg="Timeout waiting for sample")
            time.sleep(.3)

    def assert_scheduled_event(self, job_name, assert_callback=None, autosample_command=None, delay=5):
        """
        Verify that a scheduled event can be triggered and use the
        assert callback to verify that it worked. If an auto sample command
        is passed then put the driver in streaming mode.  When transitioning
        to streaming you may need to increase the delay.
        @param job_name: name of the job to schedule
        @param assert_callback: verification callback
        @param autosample_command: command to put us in autosample mode
        @param delay: time to wait before the event is triggered in seconds
        """
        # Build a scheduled job for 'delay' seconds from now
        dt = datetime.datetime.now() + datetime.timedelta(0, delay)

        scheduler_config = {
            DriverSchedulerConfigKey.TRIGGER: {
                DriverSchedulerConfigKey.TRIGGER_TYPE: TriggerType.ABSOLUTE,
                DriverSchedulerConfigKey.DATE: "%s" % dt
            }
        }
        # get the current driver configuration
        config = self.driver_client.cmd_dvr('get_init_params')

        # Add the scheduled job
        if not config:
            config = {}
        if not config.get(DriverStartupConfigKey.SCHEDULER):
            config[DriverStartupConfigKey.SCHEDULER] = {}
        config[DriverStartupConfigKey.SCHEDULER][job_name] = scheduler_config

        # Currently the only way to setup schedulers is via the startup config
        # mechanism.  We may need to expose scheduler specific capability in the
        # future, but this should work in the interim.
        self.driver_client.cmd_dvr('set_init_params', config)
        log.debug("SET CONFIG, set_init_params: %s", config)

        # Walk the driver to command mode.
        self.assert_initialize_driver()

        # We explicitly call apply startup params because we don't know if the
        # driver does it for us.  It should, but it is tested in another test.
        self.driver_client.cmd_dvr('apply_startup_params')
        # Transition to autosample if command supplied
        if autosample_command:
            self.assert_driver_command(autosample_command)

        # Ensure we have at least 5 seconds before the event should fire
        safe_time = datetime.datetime.now() + datetime.timedelta(0, 5)
        self.assertGreaterEqual(dt, safe_time, msg="Trigger time already in the past. Increase your delay")
        time.sleep(2)

        # Now verify that the job is triggered and it does what we think it should
        if assert_callback:
            log.debug("Asserting callback now")
            assert_callback()

    ###
    #   Common Integration Tests
    ###
    def test_driver_process(self):
        """
        @Brief Test for correct launch of driver process and communications, including asynchronous driver events.
        """
        log.info("Ensuring driver process was started properly ...")

        # Verify processes exist.
        self.assertNotEqual(self.driver_process, None)
        drv_pid = self.driver_process.getpid()
        self.assertTrue(isinstance(drv_pid, int))

        self.assertNotEqual(self.port_agent, None)
        pagent_pid = self.port_agent.get_pid()
        self.assertTrue(isinstance(pagent_pid, int))

        # Send a test message to the process interface, confirm result.
        log.debug("before 'process_echo'")
        reply = self.driver_client.cmd_dvr('process_echo')
        log.debug("after 'process_echo'")
        self.assert_(reply.startswith('ping from resource ppid:'))

        reply = self.driver_client.cmd_dvr('driver_ping', 'foo')
        self.assert_(reply.startswith('driver_ping: foo'))

        # Test the driver is in state unconfigured.
        # TODO: Add this test back in after driver code is merged from coi-services
        #state = self.driver_client.cmd_dvr('get_current_state')
        #self.assertEqual(state, DriverConnectionState.UNCONFIGURED)

        # Test the event thread publishes and client side picks up events.
        events = [
            'I am important event #1!',
            'And I am important event #2!'
        ]
        reply = self.driver_client.cmd_dvr('test_events', events=events)
        gevent.sleep(1)

        # Confirm the events received are as expected.
        self.assertEqual(self.events, events)

        # Test the exception mechanism.
        with self.assertRaises(ResourceError):
            exception_str = 'Oh no, something bad happened!'
            reply = self.driver_client.cmd_dvr('test_exceptions', exception_str)

    def test_disconnect(self):
        """
        Test that we can disconnect from a driver
        """
        self.assert_initialize_driver()

        reply = self.driver_client.cmd_dvr('disconnect')
        self.assertEqual(reply, None)

        self.assert_current_state(DriverConnectionState.DISCONNECTED)


class InstrumentDriverQualificationTestCase(InstrumentDriverTestCase):
    pass




class InstrumentDriverPublicationTestCase(InstrumentDriverTestCase):
    """
    Test driver publication.  These test are not include in general driver
    qualification because publication definitions could change.
    """
    pass

