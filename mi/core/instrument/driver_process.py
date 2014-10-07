#!/usr/bin/env python

"""
@package ion.services.mi.driver_process
@file ion/services/mi/driver_process.py
@author Edward Hunter
@brief Messaging enabled driver processes.
"""
import Queue
import importlib
import subprocess
import signal
import os
import time
import sys

from ooi.logging import log


__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# max seconds between interrupts before killing driver
INTERRUPT_REPEAT_INTERVAL = 3


class DriverProcess(object):
    """
    Base class for messaging enabled OS-level driver processes. Provides
    run loop, dynamic driver import and construction and interface
    for messaging implementation subclasses.
    """

    @staticmethod
    def launch_process(cmd_str):
        """
        Base class static constructor. Launch the calling class as a
        separate OS level process. This method combines the derived class
        command string with the common python interpreter command.
        @param cmd_str The python command sequence to import, create and
        run a derived class object.
        @retval a Popen object representing the driver process.
        """

        # Launch a separate python interpreter, executing the calling
        # class command string.
        spawnargs = ['python', '-c', cmd_str]
        return subprocess.Popen(spawnargs, close_fds=True)

    def __init__(self, driver_module, driver_class, ppid):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.ppid = ppid
        self.driver = None
        self.events = Queue.Queue()
        self.messaging_started = False
        self.int_time = 0

    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        try:
            module = importlib.import_module(self.driver_module)
            driver_class = getattr(module, self.driver_class)
            self.driver = driver_class(self.send_event)
            log.info('Imported and created driver from module: %r class: %r driver: %r',
                     module, driver_class, self.driver)
            return True
        except Exception as e:
            log.error('Could not import/construct driver module %s, class %s.',
                      self.driver_module, self.driver_class)
            log.error('%s' % str(e))
            return False

    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. Overridden in subclasses for
        specific messaging technologies. 
        """
        pass

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Overridden in subclasses
        for specific messaging technologies.
        """
        pass

    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        log.info('Driver process shutting down.')
        self.driver_module = None
        self.driver_class = None
        self.driver = None

    def check_parent(self):
        """
        Test for existence of original parent process, if ppid specified.
        """
        if self.ppid:
            try:
                os.kill(self.ppid, 0)

            except OSError:
                log.info('Driver process COULD NOT DETECT PARENT.')
                return False

        return True

    def send_event(self, evt):
        """
        Append an event to the list to be sent by the event threaed.
        """
        self.events.put(evt)

    def run(self):
        """
        Process entry point. Construct driver and start messaging loops.
        Periodically check messaging is going and parent exists if
        specified.
        """

        from mi.core.log import LoggerManager

        LoggerManager()

        log.info('Driver process started.')

        # noinspection PyUnusedLocal
        def shand(signum, frame):
            now = time.time()
            if now - self.int_time < INTERRUPT_REPEAT_INTERVAL:
                self.stop_messaging()
            else:
                self.int_time = now
                log.info('mi/core/instrument/driver_process.py DRIVER GOT SIGINT and is ignoring it...')

        signal.signal(signal.SIGINT, shand)

        if self.construct_driver():
            self.start_messaging()
            while self.messaging_started:
                if self.check_parent():
                    time.sleep(2)
                else:
                    self.stop_messaging()
                    break

        self.shutdown()
        time.sleep(1)
        os._exit(0)
