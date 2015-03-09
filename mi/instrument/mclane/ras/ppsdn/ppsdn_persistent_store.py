"""
@package mi.instrument.mclane.ras.ppsdn.ppsdn_persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/instrument/mclane/ras/ppsdn/ppsdn_persistent_store.py
@author Johnathon Rusk
@brief Subclass of PersistentStoreDict that adds PPSDN specific functionality
Release notes:

initial version
"""

from mi.core.persistent_store import PersistentStoreDict

class PpsdnPersistentStoreDict(PersistentStoreDict):
	def __init__(self, reference_designator):
		PersistentStoreDict.__init__(self, "ppsdn", reference_designator)
		self.CURRENT_FILTER_KEY = "CURRENT_FILTER"
		self.TOTAL_FILTERS_KEY = "TOTAL_FILTERS"
	def setFilterInfo(self, totalFilters):
		with self.rLock:
			if type(totalFilters) is not int:
				raise TypeError("totalFilters must be of type 'int'.")
			self[self.CURRENT_FILTER_KEY] = 0
			self[self.TOTAL_FILTERS_KEY] = totalFilters
	def canUseFilter(self):
		with self.rLock:
			return (self[self.CURRENT_FILTER_KEY] < self[self.TOTAL_FILTERS_KEY])
	def useFilter(self):
		with self.rLock:
			if not self.canUseFilter():
				raise Exception("No filters available for use")
			self[self.CURRENT_FILTER_KEY] = self[self.CURRENT_FILTER_KEY] + 1
	def delFilterInfo(self):
		with self.rLock:
			del self[self.CURRENT_FILTER_KEY]
			del self[self.TOTAL_FILTERS_KEY]

