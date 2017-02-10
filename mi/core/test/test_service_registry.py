#!/usr/bin/env python

"""
@package mi.core.test.test_service_registry
@file mi/core/test/test_service_registry.py
@author Peter Cable
@brief Unit tests for ConsulServiceRegistry module
"""
import threading

import time

from mock import mock
from nose.plugins.attrib import attr
from mi.core.unit_test import MiUnitTest
import sys

from mi.core.exceptions import InstrumentParameterException
from mi.core.service_registry import ConsulPersistentStore, ConsulServiceRegistry, CONSUL, DRIVER_SERVICE_NAME

__author__ = 'Peter Cable'
__license__ = 'Apache 2.0'


@attr('UNIT', group='mi')
class TestConsulServiceRegistry(MiUnitTest):
    PA_SERVICE_NAME = 'port-agent'
    PA_NAME = 'test-test-test'
    PA_PORT = 11111
    PA_SERVICE_ID = '-'.join((PA_SERVICE_NAME, PA_NAME))

    PA_COMMAND_SERVICE_NAME = 'command-port-agent'
    PA_COMMAND_NAME = 'test-test-test'
    PA_COMMAND_PORT = 22222
    PA_COMMAND_SERVICE_ID = '-'.join((PA_COMMAND_SERVICE_NAME, PA_NAME))

    def setUp(self):
        CONSUL.agent.service.register(self.PA_SERVICE_NAME, service_id=self.PA_SERVICE_ID,
                                      port=self.PA_PORT, tags=[self.PA_NAME])
        CONSUL.agent.service.register(self.PA_COMMAND_SERVICE_NAME, service_id=self.PA_COMMAND_SERVICE_ID,
                                      port=self.PA_COMMAND_PORT, tags=[self.PA_NAME])

    def tearDown(self):
        CONSUL.agent.service.deregister('_'.join((DRIVER_SERVICE_NAME, self.PA_NAME)))
        CONSUL.agent.service.deregister(self.PA_SERVICE_ID)
        CONSUL.agent.service.deregister(self.PA_COMMAND_SERVICE_ID)

    def test_register_driver(self):
        ConsulServiceRegistry.register_driver(self.PA_NAME, self.PA_PORT)
        _, result = CONSUL.health.service(DRIVER_SERVICE_NAME, tag=self.PA_NAME)

        self.assertEqual(len(result), 1)
        result = result[0]

        self.assertIn('Service', result)
        service = result['Service']

        self.assertIn('Service', service)
        self.assertEqual(service['Service'], DRIVER_SERVICE_NAME)

        self.assertIn('Tags', service)
        self.assertEqual(service['Tags'], [self.PA_NAME])

        self.assertIn('Port', service)
        self.assertEqual(service['Port'], self.PA_PORT)

        self.assertIn('Checks', result)
        checks = result['Checks']

        self.assertEqual(len(checks), 2)
        driver_check = checks[1]

        self.assertIn('Status', driver_check)
        self.assertEqual(driver_check['Status'], 'critical')

    def test_locate_port_agent(self):
        config = ConsulServiceRegistry.locate_port_agent(self.PA_NAME)
        self.assertIn('port', config)
        self.assertEqual(config['port'], self.PA_PORT)
        self.assertIn('cmd_port', config)
        self.assertEqual(config['cmd_port'], self.PA_COMMAND_PORT)
        self.assertIn('addr', config)

    def test_health_thread(self):
        real_sleep = time.sleep

        def sleep(*args):
            real_sleep(0.1)

        with mock.patch('mi.core.service_registry.time.sleep') as sleep_mock:
            sleep_mock.side_effect = sleep
            thread = ConsulServiceRegistry.create_health_thread(self.PA_NAME, self.PA_PORT)
            thread.start()

            self.assertIsInstance(thread, threading.Thread)
            self.assertTrue(thread.is_alive())
            # Sleep just long enough to send a passing check to consul
            sleep()

            thread.running = False
            thread.join()
            self.assertFalse(thread.is_alive())

            # verify we have registered our driver and we are in a passing state
            _, result = CONSUL.health.service(DRIVER_SERVICE_NAME, tag=self.PA_NAME, passing=True)
            self.assertEqual(len(result), 1)


@attr('UNIT', group='mi')
class TestConsulPersistentStore(MiUnitTest):
    def setUp(self):
        self.UNICODE_KEY = "UNICODE_KEY" # Test 'str' type key
        self.UNICODE_VALUES = [u"this is a unicode string", u"this is another unicode string"]
        self.INT_KEY = u"INT_KEY"
        self.INT_VALUES = [1234, 5678]
        self.LONG_KEY = "LONG_KEY" # Test 'str' type key
        self.LONG_VALUES = [sys.maxint + 1, sys.maxint + 2]
        self.FLOAT_KEY = u"FLOAT_KEY"
        self.FLOAT_VALUES = [56.78, 12.34]
        self.BOOL_KEY = "BOOL_KEY" # Test 'str' type key
        self.BOOL_VALUES = [True, False]
        self.DICT_KEY = u"DICT_KEY"
        self.DICT_VALUES = [{u"KEY_1":1, u"KEY_2":2, u"KEY_3":3}, {u"KEY_4":4, u"KEY_5":5, u"KEY_6":6}]
        self.LIST_KEY = "LIST_KEY" # Test 'str' type key
        self.LIST_VALUES = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]
        self.persistentStoreDict = ConsulPersistentStore("unit_test", "GI01SUMO-00001")

    def tearDown(self):
        self.persistentStoreDict.clear() # NOTE: This technically assumes the delete functionality works.

    def helper_get(self, key, expectedValue, expectedValueType):
        self.assertIn(type(key), [str, unicode])
        value = self.persistentStoreDict[key]
        self.assertIs(type(value), expectedValueType)
        self.assertEqual(value, expectedValue)

    def helper_set(self, key, value, valueType, shouldAddKey):
        self.assertIn(type(key), [str, unicode])
        self.assertIs(type(value), valueType)
        self.assertIs(type(shouldAddKey), bool)
        initialKeyCount = len(self.persistentStoreDict.keys())
        self.persistentStoreDict[key] = value
        self.assertEqual(len(self.persistentStoreDict.keys()), (initialKeyCount + 1) if shouldAddKey else initialKeyCount)

    def helper_del(self, key):
        self.assertIn(type(key), [str, unicode])
        initialKeyCount = len(self.persistentStoreDict.keys())
        del self.persistentStoreDict[key]
        self.assertEqual(len(self.persistentStoreDict.keys()), initialKeyCount - 1)

    def test_createRecords_success_unicode(self):
        self.helper_set(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode, True)

    def test_createRecords_success_int(self):
        self.helper_set(self.INT_KEY, self.INT_VALUES[0], int, True)

    def test_createRecords_success_long(self):
        self.helper_set(self.LONG_KEY, self.LONG_VALUES[0], long, True)

    def test_createRecords_success_float(self):
        self.helper_set(self.FLOAT_KEY, self.FLOAT_VALUES[0], float, True)

    def test_createRecords_success_bool(self):
        self.helper_set(self.BOOL_KEY, self.BOOL_VALUES[0], bool, True)

    def test_createRecords_success_dict(self):
        self.helper_set(self.DICT_KEY, self.DICT_VALUES[0], dict, True)

    def test_createRecords_success_list(self):
        self.helper_set(self.LIST_KEY, self.LIST_VALUES[0], list, True)

    def test_createRecords_fail_badKeyType(self):
        key = 0
        value = u"this will fail"
        self.assertNotIn(type(key), [str, unicode])
        self.assertIn(type(value), [unicode, int, long, float, bool, dict, list])
        with self.assertRaises(InstrumentParameterException):
            self.persistentStoreDict[key] = value

    def test_createRecords_fail_badItemType(self):
        key = u"this will fail"
        value = 2+3j
        self.assertIn(type(key), [str, unicode])
        self.assertNotIn(type(value), [unicode, int, long, float, bool, dict, list])
        with self.assertRaises(TypeError):
            self.persistentStoreDict[key] = value

    def test_createRecords_fail_badItemType_nested(self):
        key = u"this will fail"
        value = {u"KEY_1":[1, 2, 3], u"KEY_2":[1+2j, 3+4j, 5+6j]}
        self.assertIn(type(key), [str, unicode])
        self.assertIn(type(value), [unicode, int, long, float, bool, dict, list])
        self.assertNotIn(type(value[u'KEY_2'][0]), [unicode, int, long, float, bool, dict, list])
        with self.assertRaises(TypeError):
            self.persistentStoreDict[key] = value

    def test_getRecords_success_unicode(self):
        self.helper_set(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode, True)
        self.helper_get(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode)

    def test_getRecords_success_int(self):
        self.helper_set(self.INT_KEY, self.INT_VALUES[0], int, True)
        self.helper_get(self.INT_KEY, self.INT_VALUES[0], int)

    def test_getRecords_success_long(self):
        self.helper_set(self.LONG_KEY, self.LONG_VALUES[0], long, True)
        self.helper_get(self.LONG_KEY, self.LONG_VALUES[0], long)

    def test_getRecords_success_float(self):
        self.helper_set(self.FLOAT_KEY, self.FLOAT_VALUES[0], float, True)
        self.helper_get(self.FLOAT_KEY, self.FLOAT_VALUES[0], float)

    def test_getRecords_success_bool(self):
        self.helper_set(self.BOOL_KEY, self.BOOL_VALUES[0], bool, True)
        self.helper_get(self.BOOL_KEY, self.BOOL_VALUES[0], bool)

    def test_getRecords_success_dict(self):
        self.helper_set(self.DICT_KEY, self.DICT_VALUES[0], dict, True)
        self.helper_get(self.DICT_KEY, self.DICT_VALUES[0], dict)

    def test_getRecords_success_list(self):
        self.helper_set(self.LIST_KEY, self.LIST_VALUES[0], list, True)
        self.helper_get(self.LIST_KEY, self.LIST_VALUES[0], list)

    def test_getRecords_fail_badKeyType(self):
        key = 0
        self.assertNotIn(type(key), [str, unicode])
        with self.assertRaises(InstrumentParameterException):
            value = self.persistentStoreDict[key]

    def test_getRecords_fail_keyNotFound(self):
        key = u"this will fail"
        self.assertIn(type(key), [str, unicode])
        with self.assertRaises(KeyError):
            value = self.persistentStoreDict[key]

    def test_updateRecords_success_unicode(self):
        self.helper_set(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode, True)
        self.helper_get(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode)
        self.helper_set(self.UNICODE_KEY, self.UNICODE_VALUES[1], unicode, False)
        self.helper_get(self.UNICODE_KEY, self.UNICODE_VALUES[1], unicode)

    def test_updateRecords_success_int(self):
        self.helper_set(self.INT_KEY, self.INT_VALUES[0], int, True)
        self.helper_get(self.INT_KEY, self.INT_VALUES[0], int)
        self.helper_set(self.INT_KEY, self.INT_VALUES[1], int, False)
        self.helper_get(self.INT_KEY, self.INT_VALUES[1], int)

    def test_updateRecords_success_long(self):
        self.helper_set(self.LONG_KEY, self.LONG_VALUES[0], long, True)
        self.helper_get(self.LONG_KEY, self.LONG_VALUES[0], long)
        self.helper_set(self.LONG_KEY, self.LONG_VALUES[1], long, False)
        self.helper_get(self.LONG_KEY, self.LONG_VALUES[1], long)

    def test_updateRecords_success_float(self):
        self.helper_set(self.FLOAT_KEY, self.FLOAT_VALUES[0], float, True)
        self.helper_get(self.FLOAT_KEY, self.FLOAT_VALUES[0], float)
        self.helper_set(self.FLOAT_KEY, self.FLOAT_VALUES[1], float, False)
        self.helper_get(self.FLOAT_KEY, self.FLOAT_VALUES[1], float)

    def test_updateRecords_success_bool(self):
        self.helper_set(self.BOOL_KEY, self.BOOL_VALUES[0], bool, True)
        self.helper_get(self.BOOL_KEY, self.BOOL_VALUES[0], bool)
        self.helper_set(self.BOOL_KEY, self.BOOL_VALUES[1], bool, False)
        self.helper_get(self.BOOL_KEY, self.BOOL_VALUES[1], bool)

    def test_updateRecords_success_dict(self):
        self.helper_set(self.DICT_KEY, self.DICT_VALUES[0], dict, True)
        self.helper_get(self.DICT_KEY, self.DICT_VALUES[0], dict)
        self.helper_set(self.DICT_KEY, self.DICT_VALUES[1], dict, False)
        self.helper_get(self.DICT_KEY, self.DICT_VALUES[1], dict)

    def test_updateRecords_success_list(self):
        self.helper_set(self.LIST_KEY, self.LIST_VALUES[0], list, True)
        self.helper_get(self.LIST_KEY, self.LIST_VALUES[0], list)
        self.helper_set(self.LIST_KEY, self.LIST_VALUES[1], list, False)
        self.helper_get(self.LIST_KEY, self.LIST_VALUES[1], list)

    def test_removeRecords_success_unicode(self):
        self.helper_set(self.UNICODE_KEY, self.UNICODE_VALUES[0], unicode, True)
        self.helper_del(self.UNICODE_KEY)

    def test_removeRecords_success_int(self):
        self.helper_set(self.INT_KEY, self.INT_VALUES[0], int, True)
        self.helper_del(self.INT_KEY)

    def test_removeRecords_success_long(self):
        self.helper_set(self.LONG_KEY, self.LONG_VALUES[0], long, True)
        self.helper_del(self.LONG_KEY)

    def test_removeRecords_success_float(self):
        self.helper_set(self.FLOAT_KEY, self.FLOAT_VALUES[0], float, True)
        self.helper_del(self.FLOAT_KEY)

    def test_removeRecords_success_bool(self):
        self.helper_set(self.BOOL_KEY, self.BOOL_VALUES[0], bool, True)
        self.helper_del(self.BOOL_KEY)

    def test_removeRecords_success_dict(self):
        self.helper_set(self.DICT_KEY, self.DICT_VALUES[0], dict, True)
        self.helper_del(self.DICT_KEY)

    def test_removeRecords_success_list(self):
        self.helper_set(self.LIST_KEY, self.LIST_VALUES[0], list, True)
        self.helper_del(self.LIST_KEY)

    def test_removeRecords_fail_badKeyType(self):
        key = 0
        self.assertNotIn(type(key), [str, unicode])
        with self.assertRaises(InstrumentParameterException):
            del self.persistentStoreDict[key]

    def test_removeRecords_fail_keyNotFound(self):
        key = u"this will fail"
        self.assertIn(type(key), [str, unicode])
        with self.assertRaises(KeyError):
            del self.persistentStoreDict[key]

