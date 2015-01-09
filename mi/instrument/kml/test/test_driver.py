"""
@package mi.instrument.KML.test.test_driver
@file marine-integrations/mi/instrument/KML/test/test_driver.py
@author Sung Ahn
@brief Driver for the KML family
Release notes:
"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import time
import unittest
from mi.core.log import get_logger

log = get_logger()

from nose.plugins.attrib import attr
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import InstrumentDriverPublicationTestCase
from mi.core.exceptions import NotImplementedException
from mi.instrument.kml.particles import DataParticleType

from mi.instrument.kml.driver import KMLProtocolState, ParameterIndex
from mi.instrument.kml.driver import KMLProtocolEvent
from mi.instrument.kml.driver import KMLParameter

DEFAULT_CLOCK_DIFF = 5


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
# 1. Pick a single method within the class.                                   #
# 2. Create an instance of the class                                          #
# 3. If the method to be tested tries to call out, over-ride the offending    #
#    method with a mock                                                       #
# 4. Using above, try to cover all paths through the functions                #
# 5. Negative testing if at all possible.                                     #
###############################################################################
@attr('UNIT', group='mi')
class KMLUnitTest(InstrumentDriverUnitTestCase):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class KMLIntegrationTest(InstrumentDriverIntegrationTestCase):

    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class KMLQualificationTest(InstrumentDriverQualificationTestCase):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)


###############################################################################
#                             PUBLICATION  TESTS                              #
# Device specific publication tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('PUB', group='mi')
class KMLPublicationTest(InstrumentDriverPublicationTestCase):
    def setUp(self):
        InstrumentDriverPublicationTestCase.setUp(self)
