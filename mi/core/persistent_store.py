#!/usr/bin/env python

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
import json
from threading import RLock
from collections import MutableMapping

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


class PersistentStoreDict(MutableMapping):
    def __init__(self, driver_name, reference_designator, host = "127.0.0.1", port = "5432"):
        self.rLock = RLock()
        self.databaseSession = DatabaseSession("metadata", "awips", "awips", host, port)
        self.driver_name = driver_name
        self.reference_designator = reference_designator
        self.stmt_getitem = "SELECT json_value FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s AND key = %s"
        self.stmt_setitem_insert = "INSERT INTO instrument_driver.persistent_store (driver_name, reference_designator, key, json_value) VALUES (%s, %s, %s, %s)"
        self.stmt_setitem_update = "UPDATE instrument_driver.persistent_store SET json_value = %s WHERE driver_name = %s AND reference_designator = %s AND key = %s"
        self.stmt_delitem = "DELETE FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s AND key = %s"
        self.stmt_iter = "SELECT key FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s"
        self.stmt_len = "SELECT COUNT(key) FROM instrument_driver.persistent_store WHERE driver_name = %s AND reference_designator = %s"
        # Note: "CREATE SCHEMA IF NOT EXISTS" not supported in PostgreSQL 9.2
        self.stmt_setupDatabase_checkIfSchemaExists = "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'instrument_driver')"
        self.stmt_setupDatabase_createSchema = "CREATE SCHEMA instrument_driver AUTHORIZATION awips"
        self.stmt_setupDatabase_createTableIfNotExists = ("CREATE TABLE IF NOT EXISTS instrument_driver.persistent_store("
                                                          "driver_name text NOT NULL,"
                                                          "reference_designator text NOT NULL,"
                                                          "key text NOT NULL,"
                                                          "json_value text NOT NULL,"
                                                          "PRIMARY KEY (driver_name, reference_designator, key))")
        self.__setupDatabase()

    def __getitem__(self, key):
        self.__checkKeyType(key)
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_getitem, [self.driver_name, self.reference_designator, key])
                result = cur.fetchone()
                if not result:
                    raise KeyError("No item found with key: '{0}'".format(key))
                return json.loads(result[0])

    def __setitem__(self, key, value):
        self.__checkKeyType(key)
        self.__checkValueType(value)
        with self.rLock:
            with self.databaseSession as cur:
                if key in self:
                    cur.execute(self.stmt_setitem_update, [json.dumps(value), self.driver_name, self.reference_designator, key])
                else:
                    cur.execute(self.stmt_setitem_insert, [self.driver_name, self.reference_designator, key, json.dumps(value)])

    def __delitem__(self, key):
        self.__checkKeyType(key)
        with self.rLock:
            with self.databaseSession as cur:
                if key not in self:
                    raise KeyError("No item found with key: '{0}'".format(key))
                cur.execute(self.stmt_delitem, [self.driver_name, self.reference_designator, key])

    def __iter__(self):
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_iter, [self.driver_name, self.reference_designator])
                result = cur.fetchall()
                for row in result:
                    yield row[0]

    def __len__(self):
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_len, [self.driver_name, self.reference_designator])
                result = cur.fetchone()
                if not result:
                    raise Exception("Program error: Database query for __len__ method failed to return a value.")
                return result[0]

    def __checkKeyType(self, key):
        if type(key) not in [str, unicode]:
            raise TypeError("Key must be of type 'str' or 'unicode'.")

    def __checkValueType(self, value):
        if type(value) not in [unicode, int, long, float, bool, dict, list]:
            raise TypeError("Value must be of type: 'unicode', 'int', 'long', 'float', 'bool', 'dict', or 'list'")
        if type(value) is dict:
            for key, value in value.iteritems():
                self.__checkValueType(key)
                self.__checkValueType(value)
        if type(value) is list:
            for x in value:
                self.__checkValueType(x)

    def __setupDatabase(self):
        with self.rLock:
            with self.databaseSession as cur:
                cur.execute(self.stmt_setupDatabase_checkIfSchemaExists)
                result = cur.fetchone()
                if not result:
                    raise Exception("Program error: Database query for __setupDatabase method failed to return a value.")
                if not result[0]:
                    cur.execute(self.stmt_setupDatabase_createSchema)
                cur.execute(self.stmt_setupDatabase_createTableIfNotExists)

