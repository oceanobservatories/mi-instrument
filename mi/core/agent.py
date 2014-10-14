#!/usr/bin/env python
"""
@package pyon.agent.common_agent
@file pyon/agent/common_agent.py
@author Edward Hunter
@brief Common base class for ION resource agents.
"""

__author__ = 'Edward Hunter'



from pickle import dumps, loads
import json


# Interface imports.
from interface.services.iresource_agent import BaseResourceAgent
from interface.services.iresource_agent import ResourceAgentProcessClient
from interface.objects import CapabilityType
from interface.objects import ResourceAgentExecutionStatus
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient

from mi.core.common import BaseEnum


class UserAgent():
    pass


class ResourceAgentStreamStatus(BaseEnum):
      pass


class ResourceAgent(BaseResourceAgent):
      pass


class ResourceAgentClient(ResourceAgentProcessClient):
      pass
