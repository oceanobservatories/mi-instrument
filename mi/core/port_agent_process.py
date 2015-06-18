#!/usr/bin/env python

"""
@package ion.agents.instrument.port_agent_process
@file ion/agents.instrument/driver_launcher.py
@author Bill French
@brief Port agent process class that provides a factory for different launch mechanisms

USAGE:

config = {
    device_host : 'localhost',
    device_port : '4001'

    type : PortAgentType.ETHERNET,
    process_type : PortAgentProcessType.PYTHON,
}

# These lines can also be run as one command, launch_process
process = PortAgentType.get_process(config)
process.launch()

# alternative launch
process = launch_process(config)

pid = process.get_pid()
cmd_port = process.get_command_port()
data_port = process.get_data_port()
if(process.poll()):
    process.stop()

"""

__author__ = 'Bill French'

import os
import time
import signal
import gevent
import tempfile
import subprocess

from mi.core.log import log
from mi.core.common import BaseEnum

from mi.core.logger_process import EthernetDeviceLogger
from mi.core.exceptions import PortAgentLaunchException
from mi.core.exceptions import NotImplementedException
from mi.core.exceptions import PortAgentTimeout
from mi.core.exceptions import PortAgentMissingConfig

from gevent import Timeout

PYTHON_PATH = 'bin/python'
UNIX_PROCESS = 'port_agent'
DEFAULT_TIMEOUT = 60
PROCESS_BASE_DIR = '/tmp'
PID_FILE = "%s/port_agent_%d.pid"
LOCALHOST = 'localhost'
DEFAULT_HEARTBEAT = 0


class PortAgentProcessType(BaseEnum):
    """
    Defines the process types for the port agent.  i.e. C++ or Python
    """
    PYTHON = 'PYTHON'
    UNIX = 'UNIX'


class PortAgentType(BaseEnum):
    """
    What type of port agent are we running?  ethernet, serial, digi etc...
    """
    ETHERNET = 'tcp'
    BOTPT = "botpt"
    RSN = 'rsn'


class ObservatoryType(BaseEnum):
    """
    What type of port agent are we running?  ethernet, serial, digi etc...
    """
    STANDARD = 'standard'
    MULTI = 'multi'


class PortAgentProcess(object):
    """
    Base class for port agent process launcher
    """
    _command_port = None
    _data_port = None
    _pid = None

    def __init__(self, config, timeout=DEFAULT_TIMEOUT, test_mode=False):
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

    @classmethod
    def get_process(cls, config, timeout=DEFAULT_TIMEOUT, test_mode=False):
        """
        factory class to return the correct PortAgentProcess type based on the config.
        config must contain process_type and type.  Currently both of these default
        to python and ethernet respectively because we only have one type of port agent

        could use variable length parameter lists (**kwargs) here, but I am following the
        same pattern the initial port agent used for passing in configurations.

        @param config dictionary containing configuration information for the port agent.
        @param timeout timeout for port agent launch.  If exceeded an exception is raised
        @param test_mode enable test mode for the port agent
        """

        # Default to unix port agent
        process_type = config.get("process_type", PortAgentProcessType.UNIX)

        if process_type == PortAgentProcessType.PYTHON:
            return PythonPortAgentProcess(config, timeout, test_mode)

        if process_type == PortAgentProcessType.UNIX:
            return UnixPortAgentProcess(config, timeout, test_mode)

        else:
            raise PortAgentLaunchException("unknown port agent process type: %s" % process_type)

    @classmethod
    def launch_process(cls, config, timeout=DEFAULT_TIMEOUT, test_mode=False):
        """
        Just like the get_process factory method except we call launch with the new object.

        @param config dictionary containing configuration information for the port agent.
        @param timeout timeout for port agent launch.  If exceeded an exception is raised
        @param test_mode enable test mode for the port agent
        """
        process = cls.get_process(config, timeout, test_mode)
        process.launch()
        return process

    def launch(self):
        """
        Launch the port agent process. Must be overloaded.
        @raises NotImplementedException
        """
        raise NotImplementedException('launch()')

    def poll(self):
        """
        Check to see if the port agent process is alive.
        @return true if process is running, false otherwise
        """

        if not self._pid:
            return False

        try:
            os.kill(self._pid, 0)
        except OSError, e:
            log.warn("Could not send a signal to the driver, pid: %s" % self._pid)
            return False

        return True

    def stop(self):
        """
        Stop the driver process.  We just send a signal to a process.  We may be able to overload this to do something
        more graceful.
        """
        pid = self.get_pid()
        if pid:
            os.kill(pid, signal.SIGTERM)

    def get_pid(self):
        """
        Get the pid of the current running process and ensure that it is running.
        @returns the pid of the driver process if it is running, otherwise None
        """
        if self.poll():
            return self._pid
        else:
            return None

    def get_command_port(self):
        """
        Get the command port for the port agent process
        @returns port number
        """
        return self._command_port

    def get_data_port(self):
        """
        Get the data port for the port agent process
        @returns port number
        """
        return self._data_port


class PythonPortAgentProcess(PortAgentProcess):
    """
    Object to facilitate launching port agent processes using a python class and module path.

    Port Agent config requirements:
    dvr_mod :: the python module that defines the driver class
    dvr_cls :: the driver class defined in the module

    Example:

    port_agent_config = {
        device_addr: mi.instrument.seabird.sbe37smb.ooicore.driver
        device_port: SBE37Driver

        working_dir = "/tmp/"
        delimiter = ['<<','>>']

        type: PortAgentType.ETHERNET
    }
    @param config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    """

    _port_agent = None

    def __init__(self, config, timeout=DEFAULT_TIMEOUT, test_mode=False):
        """
        Initialize the Python port agent object using the passed in config.  This
        defaults to ethernet as the type because that is currently the only port
        agent we have.
        @raises PortAgentMissingConfig
        """
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

        # Verify our configuration is correct

        self._device_addr = config.get("device_addr")
        self._device_port = config.get("device_port")
        self._working_dir = config.get("working_dir", '/tmp/')
        self._delimiter = config.get("delimiter", ['<<', '>>'])
        self._type = config.get("type", PortAgentType.ETHERNET)

        if not self._device_addr:
            raise PortAgentMissingConfig("missing config: device_addr")

        if not self._device_port:
            raise PortAgentMissingConfig("missing config: device_port")

        if not self._type == PortAgentType.ETHERNET:
            raise PortAgentLaunchException("unknown port agent type: %s" % self._type)

    def launch(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @retval return the pid to the logger process
        """
        log.info("Startup Port Agent")
        # Create port agent object.
        this_pid = os.getpid() if self._test_mode else None

        log.debug(" -- our pid: %s" % this_pid)
        log.debug(" -- address: %s, port: %s" % (self._device_addr, self._device_port))

        # Working dir and delim are hard coded here because this launch process
        # will change with the new port agent.
        self.port_agent = EthernetDeviceLogger.launch_process(
            self._device_addr,
            self._device_port,
            self._working_dir,
            self._delimiter,
            this_pid)

        log.debug(" Port agent object created")

        start_time = time.time()
        expire_time = start_time + int(self._timeout)
        pid = self.port_agent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self.port_agent.get_pid()
            if time.time() > expire_time:
                log.error("!!!! Failed to start Port Agent !!!!")
                raise PortAgentTimeout('port agent could not be started')
        self._pid = pid

        port = self.port_agent.get_port()

        start_time = time.time()
        expire_time = start_time + int(self._timeout)
        while not port:
            gevent.sleep(.1)
            port = self.port_agent.get_port()
            if time.time() > expire_time:
                log.error("!!!! Port Agent could not bind to port !!!!")
                self.stop()
                raise PortAgentTimeout('port agent could not bind to port')
        self._data_port = port

        log.info('Started port agent pid %s listening at port %s' % (pid, port))
        return port

    def stop(self):
        if self.port_agent:
            pid = self.port_agent.get_pid()

        if pid:
            log.info('Stopping pagent pid %i' % pid)
            self.port_agent.stop()
        else:
            log.info('No port agent running.')


class UnixPortAgentProcess(PortAgentProcess):
    """
    Object to facilitate launching port agent processes using a c++ compiled port agent

    Port Agent config requirements:
    binary_path :: Path the the port agent executable
    command_port :: port number of the observatory command port to the port agent
    log_level :: how many -v options to add to the launch
    port_agent_address :: If port agent address isn't localhost the process won't be launched

    Example:

    port_agent_config = {
        device_addr: sbe37-simulator.oceanobservatories.org,
        device_port: 4001,

        binary_path: /bin/port_agent,
        port_agent_addr: localhost
        command_port: 4000,
        data_port: 4002,
        log_level: 5,

        type: PortAgentType.ETHERNET
    }
    @param config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    """

    _port_agent = None

    def __init__(self, config, timeout=DEFAULT_TIMEOUT, test_mode=False):
        """
        Initialize the Python port agent object using the passed in config.  This
        defaults to ethernet as the type because that is currently the only port
        agent we have.
        @raises PortAgentMissingConfig
        """
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

        # Verify our configuration is correct
        self._observatory_type = config.get("observatory_type", ObservatoryType.STANDARD)
        self._device_addr = config.get("device_addr")
        self._device_port = config.get("device_port")
        self._device_tx_port = config.get("device_tx_port")
        self._device_rx_port = config.get("device_rx_port")
        self._binary_path = config.get("binary_path", "port_agent")
        self._command_port = config.get("command_port")
        self._pa_addr = config.get("port_agent_addr")
        self._heartbeat_interval = config.get("heartbeat_interval")
        self._sniffer_port = config.get('telnet_sniffer_port')

        self._data_port = config.get("data_port")
        self._log_level = config.get("log_level")
        self._type = config.get("instrument_type", PortAgentType.ETHERNET)

        if self._type == PortAgentType.ETHERNET:
            self._device_addr = config.get("device_addr")
            self._device_port = config.get("device_port")
        elif self._type == PortAgentType.RSN:
            self._device_addr = config.get("device_addr")
            self._device_port = config.get("device_port")
            self._device_cmd_port = config.get("instrument_command_port")

        if not self._pa_addr:
            self._pa_addr = LOCALHOST

        if not self._heartbeat_interval:
            self._heartbeat_interval = DEFAULT_HEARTBEAT

        if PortAgentType.BOTPT == self._type:
            if not self._device_tx_port:
                raise PortAgentMissingConfig("missing config: device_tx_port (BOTPT)")
            if not self._device_rx_port:
                raise PortAgentMissingConfig("missing config: device_rx_port (BOTPT)")
        elif PortAgentType.ETHERNET == self._type:
            if not self._device_addr:
                raise PortAgentMissingConfig("missing config: device_addr")
            if not self._device_port:
                raise PortAgentMissingConfig("missing config: device_port (ETHERNET)")
        elif PortAgentType.RSN == self._type:
            if not self._device_addr:
                raise PortAgentMissingConfig("missing config: device_addr")
            if not self._device_port:
                raise PortAgentMissingConfig("missing config: device_port (RSN)")
            if not self._device_cmd_port:
                raise PortAgentMissingConfig("missing config: device_cmd_port (RSN)")
        else:
            raise PortAgentLaunchException("unknown port agent type: %s" % self._type)

        if not self._command_port:
            raise PortAgentMissingConfig("missing config: command_port")

        if ObservatoryType.MULTI == self._observatory_type:
            if not self._data_port:
                raise PortAgentMissingConfig("missing config: data_port")
        else:
            if not self._data_port:
                raise PortAgentMissingConfig("missing config: data_port")

        if not self._binary_path:
            raise PortAgentMissingConfig("missing config: binary_path")

        self._tmp_config = self.get_config()

    def get_config(self):
        """
        @brief Write a configuration file for the port agent to read.
        @ret NamedTemporaryFile object to the config file.
        """

        temp = tempfile.NamedTemporaryFile()
        temp.write("\n")
        temp.write("log_dir %s\n" % PROCESS_BASE_DIR)
        temp.write("pid_dir %s\n" % PROCESS_BASE_DIR)
        temp.write("data_dir %s\n" % PROCESS_BASE_DIR)
        if PortAgentType.BOTPT == self._type:
            temp.write("instrument_type botpt\n")
            temp.write("instrument_data_tx_port %d\n" % self._device_tx_port)
            temp.write("instrument_data_rx_port %d\n" % self._device_rx_port)
        else:
            temp.write("instrument_type tcp\n")
            temp.write("instrument_data_port %d\n" % self._device_port)
        temp.write("instrument_addr %s\n" % self._device_addr)
        temp.write("data_port %d\n" % self._data_port)
        temp.write("heartbeat_interval %d\n" % self._heartbeat_interval)
        temp.flush()

        return temp

    def launch(self):
        '''
        @brief Launch a port agent process if it is supposed to run on the local host  Otherwise
               do nothing.
        @return the command port the port agent is listening on.
        '''
        if self._pa_addr == LOCALHOST:
            self._launch()
        else:
            self._pid = None
            log.info("Port Agent Address: %s" % self._pa_addr)
            log.info("Not starting port agent")

        return self._command_port

    def _launch(self):
        """
        @brief Launch the port agent process. If the address isn't localhost
        then we don't start anything
        @retval return the command port the process is listening on.
        """
        log.info("Startup Unix Port Agent")
        # Create port agent object.
        this_pid = os.getpid() if self._test_mode else None

        log.debug(" -- our pid: %s" % this_pid)
        log.debug(" -- command port: %s" % self._command_port)
        log.debug(" -- address: %s, port: %s" % (self._device_addr, self._device_port))

        command_line = [self._binary_path, self._type, self._data_port, self._command_port, self._device_addr]
        if self._type == PortAgentType.ETHERNET:
            command_line.extend([self._device_port])
        elif self._type == PortAgentType.RSN:
            command_line.extend([self._device_port, self._device_cmd_port])
        elif self._type == PortAgentType.BOTPT:
            command_line.extend([self._device_rx_port, self._device_tx_port])

        if self._sniffer_port:
            command_line.append('--sniff=%d' % self._sniffer_port)

        command_line = [str(arg) for arg in command_line]

        self._pid = self.run_command(command_line)

        return self._command_port

    def run_command(self, command_line):
        log.debug("run command: " + str(command_line))
        process = subprocess.Popen(command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        gevent.sleep(1)

        process.poll()

        # We have failed!
        if process.returncode and process.pid:
            output, error_message = process.communicate()
            log.error("Failed to run command: STDERR: %s", error_message)
            raise PortAgentLaunchException("failed to launch port agent")

        log.debug("command successful. pid: %d", process.pid)

        return process.pid

    def _read_config(self):
        self._tmp_config.seek(0)
        return "".join(self._tmp_config.readlines())

    def stop(self):
        log.info('Stop port agent')
        os.kill(self._pid, 2)
