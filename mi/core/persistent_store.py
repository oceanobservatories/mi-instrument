# Note: No shebang since uframe python location is non-standard.

"""
@package mi.core.persistent_store
@file <git-workspace>/ooi/edex/com.raytheon.uf.ooi.plugin.instrumentagent/utility/edex_static/base/ooi/instruments/mi-instrument/mi/core/persistent_store.py
@author Johnathon Rusk
@brief Wrapper around PostgreSQL database that allows instrument drivers to persist data
Release notes: initial version
"""

__author__ = 'Johnathon Rusk'
__license__ = 'Apache 2.0'

import psycopg2
from threading import RLock
from UserDict import DictMixin
import json

# Needed PostgreSQL Configurations:
# --------------------------------
# CREATE SCHEMA instrument_driver AUTHORIZATION awips;
# CREATE TABLE instrument_driver.persistent_store(driver_name text NOT NULL, reference_designator text NOT NULL, key text NOT NULL, json_value text NOT NULL, PRIMARY KEY (driver_name, reference_designator, key));

class DatabaseSession(object):
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.reentrantDepth = 0

    def __enter__(self):
        if self.reentrantDepth == 0:
            # Connect to an existing database
            self.conn = psycopg2.connect(database = self.database, user = self.user, password = self.password, host = self.host, port = self.port)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
        self.reentrantDepth = self.reentrantDepth + 1
        return self.cur

    def __exit__(self, exception_type, exception_value, traceback):
        self.reentrantDepth = self.reentrantDepth - 1
        if self.reentrantDepth == 0:
            # Make the changes to the database persistent
            self.conn.commit()
            # Close communication with the database
            self.cur.close()
            self.conn.close()


class PersistentStoreDict(DictMixin):
    def __init__(self, driver_name, reference_designator, host = "127.0.0.1", port = "5432"):
        self.rLock = RLock()
        self.databaseSession = DatabaseSession("metadata", "awips", "awips", host, port)
        self.driver_name = driver_name
        self.reference_designator = reference_designator
        self.stmt_getitem = "SELECT json_value FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s AND key = %s"
        self.stmt_setitem_insert = "INSERT INTO instrument_driver.persistent_store (driver_name, reference_designator, key, json_value) VALUES (%s, %s, %s, %s)"
        self.stmt_setitem_update = "UPDATE instrument_driver.persistent_store SET json_value = %s WHERE driver_name = %s AND reference_designator = %s AND key = %s;"
        self.stmt_delitem = "DELETE FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s AND key = %s"
        self.stmt_keys = "SELECT key FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s"

    def __getitem__(self, key):
        if type(key) not in [str, unicode]:
            raise TypeError("Key must be of type 'str' or 'unicode'.")
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_getitem, [self.driver_name, self.reference_designator, key])
                result = cur.fetchone()
                if not result:
                    raise KeyError("No item found with key: '{0}'".format(key))
                return json.loads(result[0])

    def __setitem__(self, key, item):
        if type(key) not in [str, unicode]:
            raise TypeError("Key must be of type 'str' or 'unicode'.")
        self.__checkItemType(item)
        with self.rLock:
            with self.databaseSession as cur:
                try:
                    self[key] # Will throw a KeyError if the key is not found
                    cur.execute(self.stmt_setitem_update, [json.dumps(item), self.driver_name, self.reference_designator, key])
                except KeyError:
                    cur.execute(self.stmt_setitem_insert, [self.driver_name, self.reference_designator, key, json.dumps(item)])

    def __delitem__(self, key):
        if type(key) not in [str, unicode]:
            raise TypeError("Key must be of type 'str' or 'unicode'.")
        with self.rLock:
            with self.databaseSession as cur:
                self[key] # Will throw a KeyError if the key is not found
                cur.execute(self.stmt_delitem, [self.driver_name, self.reference_designator, key])

    def keys(self):
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_keys, [self.driver_name, self.reference_designator])
                result = cur.fetchall()
                return [row[0] for row in result]

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

