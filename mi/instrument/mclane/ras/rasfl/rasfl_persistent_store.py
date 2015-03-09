# Note: No shebang since uframe python location is non-standard.

"""
@package mi.instrument.mclane.ras.rasfl.rasfl_persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/instrument/mclane/ras/rasfl/rasfl_persistent_store.py
@author Johnathon Rusk
@brief Subclass of PersistentStoreDict that adds RASFL specific functionality
Release notes: initial version
"""

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

from mi.core.persistent_store import PersistentStoreDict

class RasflPersistentStoreDict(PersistentStoreDict):
    def __init__(self, reference_designator, contact_points = ["127.0.0.1"], port = 9042):
        PersistentStoreDict.__init__(self, "rasfl", reference_designator, contact_points, port)
        self.CURRENT_COLLECTION_BAG_KEY = u"CURRENT_COLLECTION_BAG"
        self.TOTAL_COLLECTION_BAGS_KEY = u"TOTAL_COLLECTION_BAGS"

    def isInitialized(self):
        with self.rLock:
            return ((self.CURRENT_COLLECTION_BAG_KEY in self) and (self.TOTAL_COLLECTION_BAGS_KEY in self))

    def initialize(self, totalCollectionBags):
        with self.rLock:
            if type(totalCollectionBags) is not int:
                raise TypeError("totalCollectionBags must be of type 'int'.")
            if self.isInitialized():
                raise Exception("Already initialized.")
            self[self.CURRENT_COLLECTION_BAG_KEY] = 0
            self[self.TOTAL_COLLECTION_BAGS_KEY] = totalCollectionBags

    def canUseCollectionBag(self):
        with self.rLock:
            if not self.isInitialized():
                raise Exception("Not initialized.")
            return (self[self.CURRENT_COLLECTION_BAG_KEY] < self[self.TOTAL_COLLECTION_BAGS_KEY])

    def useCollectionBag(self):
        with self.rLock:
            if not self.canUseCollectionBag():
                raise Exception("No bags available for use.")
            self[self.CURRENT_COLLECTION_BAG_KEY] = self[self.CURRENT_COLLECTION_BAG_KEY] + 1

