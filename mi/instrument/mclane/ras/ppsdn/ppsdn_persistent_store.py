# Note: No shebang since uframe python location is non-standard.

"""
@package mi.instrument.mclane.ras.ppsdn.ppsdn_persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/instrument/mclane/ras/ppsdn/ppsdn_persistent_store.py
@author Johnathon Rusk
@brief Subclass of PersistentStoreDict that adds PPSDN specific functionality
Release notes: initial version
"""

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

from mi.core.persistent_store import PersistentStoreDict

class PpsdnPersistentStoreDict(PersistentStoreDict):
    def __init__(self, reference_designator, contact_points = ["127.0.0.1"], port = 9042):
        PersistentStoreDict.__init__(self, "ppsdn", reference_designator, contact_points, port)
        self.CURRENT_FILTER_KEY = u"CURRENT_FILTER"
        self.TOTAL_FILTERS_KEY = u"TOTAL_FILTERS"

    def isInitialized(self):
        with self.rLock:
            return ((self.CURRENT_FILTER_KEY in self) and (self.TOTAL_FILTERS_KEY in self))

    def initialize(self, totalFilters):
        with self.rLock:
            if type(totalFilters) is not int:
                raise TypeError("totalFilters must be of type 'int'.")
            if self.isInitialized():
                raise Exception("Already initialized.")
            self[self.CURRENT_FILTER_KEY] = 0
            self[self.TOTAL_FILTERS_KEY] = totalFilters

    def canUseFilter(self):
        with self.rLock:
            if not self.isInitialized():
                raise Exception("Not initialized.")
            return (self[self.CURRENT_FILTER_KEY] < self[self.TOTAL_FILTERS_KEY])

    def useFilter(self):
        with self.rLock:
            if not self.canUseFilter():
                raise Exception("No filters available for use.")
            self[self.CURRENT_FILTER_KEY] = self[self.CURRENT_FILTER_KEY] + 1

