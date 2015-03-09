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

    def test_setCollectionBagInfo_success(self):
        with self.assertRaises(KeyError) as contextManager:
            self.rasflPersistentStoreDict.canUseCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "No item found with key: '{}'".format(self.rasflPersistentStoreDict.CURRENT_COLLECTION_BAG_KEY))
        totalCollectionBags = self.TOTAL_COLLECTION_BAGS_VALUE
        self.assertIs(type(totalCollectionBags), int)
        self.rasflPersistentStoreDict.setCollectionBagInfo(totalCollectionBags)
        self.assertTrue(self.rasflPersistentStoreDict.canUseCollectionBag())

    def test_setCollectionBagInfo_fail_badType(self):
        totalCollectionBags = u"this will fail"
        self.assertIsNot(type(totalCollectionBags), int)
        with self.assertRaises(TypeError) as contextManager:
            self.rasflPersistentStoreDict.setCollectionBagInfo(totalCollectionBags)
        self.assertEqual(contextManager.exception.args[0], "totalCollectionBags must be of type 'int'.")

    def test_useCollectionBag_mixed(self):
        self.test_setCollectionBagInfo_success()
        # Success Test
        for x in range(0, self.TOTAL_COLLECTION_BAGS_VALUE):
            self.assertTrue(self.rasflPersistentStoreDict.canUseCollectionBag())
            self.rasflPersistentStoreDict.useCollectionBag()
        # Fail Test
        self.assertFalse(self.rasflPersistentStoreDict.canUseCollectionBag())
        with self.assertRaises(Exception) as contextManager:
            self.rasflPersistentStoreDict.useCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "No bags available for use")

    def test_delCollectionBagInfo_success(self):
        self.test_setCollectionBagInfo_success()
        self.rasflPersistentStoreDict.delCollectionBagInfo()
        with self.assertRaises(KeyError) as contextManager:
            self.rasflPersistentStoreDict.canUseCollectionBag()
        self.assertEqual(contextManager.exception.args[0], "No item found with key: '{}'".format(self.rasflPersistentStoreDict.CURRENT_COLLECTION_BAG_KEY))

