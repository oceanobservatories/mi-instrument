#!/usr/bin/env python
# coding=utf-8

"""
@package mi.instrument.wetlabs.fluorometer.flord_d.driver
@file marine-integrations/mi/instrument/wetlabs/fluorometer/flord_d/driver.py
@author Art Teranishi
@brief Driver for the flord_d
Release notes:

Initial development
"""

__author__ = 'Richard Han'
__license__ = 'Apache 2.0'

from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.instrument.wetlabs.fluorometer.flort_d.driver import Parameter
from mi.instrument.wetlabs.fluorometer.flort_d.driver import Prompt
from mi.instrument.wetlabs.fluorometer.flort_d.driver import NEWLINE
from mi.instrument.wetlabs.fluorometer.flort_d.driver import Protocol

# Flord Instrument class
FLORD_CLASS = 'flord'


###############################################################################
# Driver
###############################################################################
class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses InstrumentDriver with connection state
    machine.
    """

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################
    @staticmethod
    def get_resource_params():
        """
        Return list of device parameters available.
        """
        return Parameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################
    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = FlordProtocol(Prompt, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################
class FlordProtocol(Protocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """

    __instrument_class__ = FLORD_CLASS
