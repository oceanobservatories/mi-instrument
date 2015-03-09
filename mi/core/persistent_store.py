"""
@package mi.core.persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/core/persistent_store.py
@author Johnathon Rusk
@brief Wrapper around Cassandra database that allows instrument drivers to persist data
Release notes:

initial version
"""

from cassandra.cluster import Cluster
from multiprocessing import RLock
from UserDict import DictMixin

# Needed Cassandra Configurations:
# -------------------------------
# CREATE KEYSPACE instrument_driver WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
# DROP TABLE instrument_driver.persistent_store;
# CREATE TABLE instrument_driver.persistent_store(driver_name text, reference_designator text, key text, value text, type text, PRIMARY KEY (driver_name, reference_designator, key));

class PersistentStoreDict(DictMixin):
	def __init__(self, driver_name, reference_designator):
		self.stringConverter = { "str" : str, "int" : int, "float" : float, "bool" : lambda x: x == "True" }
		self.rLock = RLock()
		self.session = Cluster().connect('instrument_driver')
		self.pStmt_getitem = self.session.prepare("SELECT * FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}' AND key = ?".format(driver_name, reference_designator))
		self.pStmt_setitem = self.session.prepare("INSERT INTO persistent_store (driver_name, reference_designator, key, value, type) VALUES ('{}', '{}', ?, ?, ?)".format(driver_name, reference_designator))
		self.pStmt_delitem = self.session.prepare("DELETE FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}' AND key = ?".format(driver_name, reference_designator))
		self.pStmt_keys = "SELECT * FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}'".format(driver_name, reference_designator)
	def __getitem__(self, key):
		with self.rLock:
			if type(key) is not str:
				raise TypeError("Key must be of type 'str'.")
			result = self.session.execute(self.pStmt_getitem.bind([key]))
			if not result:
				raise KeyError("No item found with key: '{}'".format(key))
			return self.stringConverter[result[0].type](result[0].value)
	def __setitem__(self, key, item):
		with self.rLock:
			if type(key) is not str:
				raise TypeError("Key must be of type 'str'.")
			if type(item).__name__ not in ("str", "int", "float", "bool"):
				raise TypeError("Item must be of type: 'str', 'int', 'float', or 'bool'")
			self.session.execute(self.pStmt_setitem.bind([key, str(item), type(item).__name__]))
	def __delitem__(self, key):
		with self.rLock:
			self[key] # Throw exception if key is bad
			self.session.execute(self.pStmt_delitem.bind([key]))
	def keys(self):
		with self.rLock:
			result = self.session.execute(self.pStmt_keys)
			returnValue = []
			for row in result:
				returnValue.append(self.stringConverter[row.type](row.value))
			return returnValue

