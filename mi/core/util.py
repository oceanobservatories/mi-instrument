#!/usr/bin/env python

"""
@package ion.services.mi.util Utility functions for MI
@file ion/services/mi/util.py
@author Bill French
@brief Common MI utility functions
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from mi.core.log import get_logger

log = get_logger()


def dict_equal(ldict, rdict, ignore_keys=[]):
    """
    Compare two dictionary.  assumes both dictionaries are flat
    @param ldict: left side dict
    @param rdict: right side dict
    @param ignore_keys: list of keys that we don't compare
    @return: true if equal false if not
    """
    if not isinstance(ignore_keys, list):
        ignore_keys = [ignore_keys]

    for key in set(ldict.keys() + rdict.keys()):
        if key in ldict.keys() and key in rdict.keys():
            if key not in ignore_keys and ldict[key] != rdict[key]:
                log.debug("Key '%s' %s != %s", key, ldict[key], rdict[key])
                return False
        else:
            log.debug("Key '%s' not in both lists", key)
            return False
        pass

    return True


def hex2value(hex_value, divisor=None):
    """
    Convert a hex string to value.
    @param hex_value: string to convert
    @param divisor: if present, used as a divisor for the value
    @return: equivalent value of the converted hex string
    """
    if not isinstance(hex_value, basestring):
        raise ValueError("hex value not a string")

    if divisor == 0:
        raise ValueError("divisor can not be 0")

    value = int(hex_value, 16)
    if divisor is not None:
        return float(value) / divisor
    return value

