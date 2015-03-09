# Note: No shebang since uframe python location is non-standard.

"""
@package mi.core.persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/core/persistent_store.py
@author Johnathon Rusk
@brief Wrapper around Cassandra database that allows instrument drivers to persist data
Release notes: initial version
"""

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

from cassandra.cluster import Cluster
from multiprocessing import RLock
from UserDict import DictMixin
import json

# Needed Cassandra Configurations:
# -------------------------------
# CREATE KEYSPACE IF NOT EXISTS instrument_driver WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
# CREATE TABLE IF NOT EXISTS instrument_driver.persistent_store(driver_name text, reference_designator text, key text, json_value text, PRIMARY KEY (driver_name, reference_designator, key));

class PersistentStoreDict(DictMixin):
    def __init__(self, driver_name, reference_designator, contact_points = ["127.0.0.1"], port = 9042):
        self.rLock = RLock()
        self.session = Cluster(contact_points, port).connect('instrument_driver')
        self.pStmt_getitem = self.session.prepare("SELECT * FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}' AND key = ?".format(driver_name, reference_designator))
        self.pStmt_setitem = self.session.prepare("INSERT INTO persistent_store (driver_name, reference_designator, key, json_value) VALUES ('{}', '{}', ?, ?)".format(driver_name, reference_designator))
        self.pStmt_delitem = self.session.prepare("DELETE FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}' AND key = ?".format(driver_name, reference_designator))
        self.pStmt_keys = "SELECT * FROM persistent_store WHERE driver_name = '{}' AND reference_designator = '{}'".format(driver_name, reference_designator)
    def __getitem__(self, key):
        with self.rLock:
            if type(key) not in [str, unicode]:
                raise TypeError("Key must be of type 'str' or 'unicode'.")
            result = self.session.execute(self.pStmt_getitem.bind([key]))
            if not result:
                raise KeyError("No item found with key: '{}'".format(key))
            return json.loads(result[0].json_value)
    def __setitem__(self, key, item):
        with self.rLock:
            if type(key) not in [str, unicode]:
                raise TypeError("Key must be of type 'str' or 'unicode'.")
            self.__checkItemType(item)
            self.session.execute(self.pStmt_setitem.bind([key, json.dumps(item)]))
    def __delitem__(self, key):
        with self.rLock:
            self[key] # Perform checks on key
            self.session.execute(self.pStmt_delitem.bind([key]))
    def keys(self):
        with self.rLock:
            result = self.session.execute(self.pStmt_keys)
            returnValue = []
            for row in result:
                returnValue.append(row.key)
            return returnValue
    def __checkItemType(self, item):
        if type(item) not in [unicode, int, long, float, bool, dict, list]:
            raise TypeError("Item must be of type: 'unicode', 'int', 'long', 'float', 'bool', 'dict', or 'list'")
        if type(item) is dict:
            for key, value in item.iteritems():
                self.__checkItemType(key)
                self.__checkItemType(value)
        if type(item) is list:
            for x in item:
                self.__checkItemType(x)

