# Note: No shebang since uframe python location is non-standard.

"""
@package mi.instrument.mclane.ras.rasfl.test.test_rasfl_persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/instrument/mclane/ras/rasfl/test/test_rasfl_persistent_store.py
@author Johnathon Rusk
@brief Unit tests for RasflPersistentStoreDict module
"""

# Note: Execute via, "nosetests -a UNIT -v mi/instrument/mclane/ras/rasfl/test/test_rasfl_persistent_store.py"

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

from nose.plugins.attrib import attr
from mi.core.unit_test import MiUnitTest

from mi.instrument.mclane.ras.rasfl.rasfl_persistent_store import RasflPersistentStoreDict

@attr('UNIT', group='mi')
class TestRasflPersistentStoreDict(MiUnitTest):
    def setUp(self):
        self.TOTAL_COLLECTION_BAGS_VALUE = 5
        self.rasflPersistentStoreDict = RasflPersistentStoreDict("GI01SUMO-00001")

    def tearDown(self):
        self.rasflPersistentStoreDict.clear() # NOTE: This technically assumes the delete functionality works.

    def test_initialize_success(self):
        self.assertFalse(self.rasflPersistentStoreDict.isInitialized())
        totalCollectionBags = self.TOTAL_COLLECTION_BAGS_VALUE
        self.assertIs(type(totalCollectionBags), int)
        self.rasflPersistentStoreDict.initialize(totalCollectionBags)
        self.assertTrue(self.rasflPersistentStoreDict.isInitialized())

    def test_initialize_fail_badType(self):
        totalCollectionBags = u"this will fail"
        self.assertIsNot(type(totalCollectionBags), int)
        with self.assertRaises(TypeError) as contextManager:
            self.rasflPersistentStoreDict.initialize(totalCollectionBags)
        self.assertEqual(contextManager.exception.args[0], "totalCollectionBags must be of type 'int'.")

    def test_initialize_fail_alreadyInitialized(self):
        self.test_initialize_success()
        totalCollectionBags = self.TOTAL_COLLECTION_BAGS_VALUE
        self.assertIs(type(totalCollectionBags), int)
        with self.assertRaises(Exception) as contextManager:
            self.rasflPersistentStoreDict.initialize(totalCollectionBags)
        self.assertEqual(contextManager.exception.args[0], "Already initialized.")

    def test_useCollectionBag_mixed(self):
        self.test_initialize_success()
        # Success Test
        for x in range(0, self.TOTAL_COLLECTION_BAGS_VALUE):
            self.assertTrue(self.rasflPersistentStoreDict.canUseCollectionBag())
            self.rasflPersistentStoreDict.useCollectionBag()
        # Fail Test
        self.assertFalse(self.rasflPersistentStoreDict.canUseCollectionBag())
        with self.assertRaises(Exception) as contextManager:
            self.rasflPersistentStoreDict.useCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "No bags available for use.")

    def test_canUseCollectionBag_fail_notInitialized(self):
        self.assertFalse(self.rasflPersistentStoreDict.isInitialized())
        with self.assertRaises(Exception) as contextManager:
            self.rasflPersistentStoreDict.canUseCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "Not initialized.")

    def test_useCollectionBag_fail_notInitialized(self):
        self.assertFalse(self.rasflPersistentStoreDict.isInitialized())
        with self.assertRaises(Exception) as contextManager:
            self.rasflPersistentStoreDict.useCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "Not initialized.")

