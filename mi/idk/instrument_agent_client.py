"""
@file mi/idk/instrument_agent_client.py
@author Bill French
@brief Helper class start the instrument_agent_client
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import signal
import subprocess
import time
import gevent
from gevent import spawn
from gevent.event import AsyncResult

from mi.core.log import get_logger ; log = get_logger()

# Set testing to false because the capability container tries to clear out
# couchdb if we are testing. Since we don't care about couchdb for the most
# part we can ignore this. See initialize_ion_int_tests() for implementation.
# If you DO care about couch content make sure you do a force_clean when needed.

from copy import deepcopy

from mi.core.common import BaseEnum
from mi.idk.config import Config

from mi.idk.exceptions import TestNoDeployFile
from mi.idk.exceptions import NoContainer
from mi.idk.exceptions import MissingConfig
from mi.idk.exceptions import MissingExecutable
from mi.idk.exceptions import FailedToLaunch
from mi.idk.exceptions import SampleTimeout
from mi.idk.exceptions import IDKException
from mi.idk.exceptions import ParameterException

from mi.core.unit_test import MiIntTestCase
from mi.core.instrument.data_particle import CommonDataParticleType


DEFAULT_DEPLOY = 'res/deploy/r2deploy.yml'
DEFAULT_STREAM_NAME = CommonDataParticleType.RAW


class InstrumentAgentClient(object):
    """
    Launch a capability container and instrument agent client
    """
    pass

class InstrumentAgentDataSubscribers(object):
    """
    Setup Instrument Agent Publishers
    """
    pass

class InstrumentAgentEventSubscribers(object):
    """
    Create subscribers for agent and driver events.
    """
    pass
