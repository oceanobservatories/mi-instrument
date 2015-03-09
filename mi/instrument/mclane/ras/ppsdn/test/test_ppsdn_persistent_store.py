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

    def test_setFilterInfo_success(self):
        with self.assertRaises(KeyError) as contextManager:
            self.ppsdnPersistentStoreDict.canUseFilter()
        self.assertEqual(contextManager.exception.args[0], "No item found with key: '{}'".format(self.ppsdnPersistentStoreDict.CURRENT_FILTER_KEY))
        totalFilters = self.TOTAL_FILTERS_VALUE
        self.assertIs(type(totalFilters), int)
        self.ppsdnPersistentStoreDict.setFilterInfo(totalFilters)
        self.assertTrue(self.ppsdnPersistentStoreDict.canUseFilter())

    def test_setFilterInfo_fail_badType(self):
        totalFilters = u"this will fail"
        self.assertIsNot(type(totalFilters), int)
        with self.assertRaises(TypeError) as contextManager:
            self.ppsdnPersistentStoreDict.setFilterInfo(totalFilters)
        self.assertEqual(contextManager.exception.args[0], "totalFilters must be of type 'int'.")

    def test_useFilter_mixed(self):
        self.test_setFilterInfo_success()
        # Success Test
        for x in range(0, self.TOTAL_FILTERS_VALUE):
            self.assertTrue(self.ppsdnPersistentStoreDict.canUseFilter())
            self.ppsdnPersistentStoreDict.useFilter()
        # Fail Test
        self.assertFalse(self.ppsdnPersistentStoreDict.canUseFilter())
        with self.assertRaises(Exception) as contextManager:
            self.ppsdnPersistentStoreDict.useFilter()
        self.assertEqual(contextManager.exception.args[0], "No filters available for use")

    def test_delFilterInfo_success(self):
        self.test_setFilterInfo_success()
        self.ppsdnPersistentStoreDict.delFilterInfo()
        with self.assertRaises(KeyError) as contextManager:
            self.ppsdnPersistentStoreDict.canUseFilter()
        self.assertEqual(contextManager.exception.args[0], "No item found with key: '{}'".format(self.ppsdnPersistentStoreDict.CURRENT_FILTER_KEY))

