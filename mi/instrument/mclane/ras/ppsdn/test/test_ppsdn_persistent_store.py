# Note: No shebang since uframe python location is non-standard.

"""
@package mi.instrument.mclane.ras.ppsdn.test.test_ppsdn_persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/instrument/mclane/ras/ppsdn/test/test_ppsdn_persistent_store.py
@author Johnathon Rusk
@brief Unit tests for PpsdnPersistentStoreDict module
"""

# Note: Execute via, "nosetests -a UNIT -v mi/instrument/mclane/ras/ppsdn/test/test_ppsdn_persistent_store.py"

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from mi.core.unit_test import MiUnitTest

from mi.instrument.mclane.ras.ppsdn.ppsdn_persistent_store import PpsdnPersistentStoreDict

@attr('UNIT', group='mi')
class TestPpsdnPersistentStoreDict(MiUnitTest):
    def setUp(self):
        self.TOTAL_FILTERS_VALUE = 5
        self.ppsdnPersistentStoreDict = PpsdnPersistentStoreDict("GI01SUMO-00001")

    def tearDown(self):
        self.ppsdnPersistentStoreDict.clear() # NOTE: This technically assumes the delete functionality works.

    def test_initialize_success(self):
        self.assertFalse(self.ppsdnPersistentStoreDict.isInitialized())
        totalFilters = self.TOTAL_FILTERS_VALUE
        self.assertIs(type(totalFilters), int)
        self.ppsdnPersistentStoreDict.initialize(totalFilters)
        self.assertTrue(self.ppsdnPersistentStoreDict.isInitialized())

    def test_initialize_fail_badType(self):
        totalFilters = u"this will fail"
        self.assertIsNot(type(totalFilters), int)
        with self.assertRaises(TypeError) as contextManager:
            self.ppsdnPersistentStoreDict.initialize(totalFilters)
        self.assertEqual(contextManager.exception.args[0], "totalFilters must be of type 'int'.")

    def test_initialize_fail_alreadyInitialized(self):
        self.test_initialize_success()
        totalFilters = self.TOTAL_FILTERS_VALUE
        self.assertIs(type(totalFilters), int)
        with self.assertRaises(Exception) as contextManager:
            self.ppsdnPersistentStoreDict.initialize(totalFilters)
        self.assertEqual(contextManager.exception.args[0], "Already initialized.")

    def test_useFilter_mixed(self):
        self.test_initialize_success()
        # Success Test
        for x in range(0, self.TOTAL_FILTERS_VALUE):
            self.assertTrue(self.ppsdnPersistentStoreDict.canUseFilter())
            self.ppsdnPersistentStoreDict.useFilter()
        # Fail Test
        self.assertFalse(self.ppsdnPersistentStoreDict.canUseFilter())
        with self.assertRaises(Exception) as contextManager:
            self.ppsdnPersistentStoreDict.useFilter()
        self.assertEqual(contextManager.exception.args[0], "No filters available for use.")

    def test_canUseFilter_fail_notInitialized(self):
        self.assertFalse(self.ppsdnPersistentStoreDict.isInitialized())
        with self.assertRaises(Exception) as contextManager:
            self.ppsdnPersistentStoreDict.canUseFilter()
        self.assertEqual(contextManager.exception.args[0], "Not initialized.")

    def test_useFilter_fail_notInitialized(self):
        self.assertFalse(self.ppsdnPersistentStoreDict.isInitialized())
        with self.assertRaises(Exception) as contextManager:
            self.ppsdnPersistentStoreDict.useFilter()
        self.assertEqual(contextManager.exception.args[0], "Not initialized.")

