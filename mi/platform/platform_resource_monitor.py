#!/usr/bin/env python

"""
@package ion.agents.platform.platform_resource_monitor
@file    ion/agents/platform/platform_resource_monitor.py
@author  Carlos Rueda
@brief   Platform resource monitoring handling for all the associated attributes
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.resource_monitor import ResourceMonitor
from ion.agents.platform.resource_monitor import _STREAM_NAME
from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent

from gevent import Greenlet, sleep
from gevent.coros import RLock

import pprint


class PlatformResourceMonitor(object):
    """
    Resource monitoring for a given platform.
    """

    def __init__(self, platform_id, attr_info, stream_info,get_attribute_values, notify_driver_event):
        """
        @param platform_id Platform ID
        @param attr_info Attribute information
        @param stream_info Stream information for this Platform
        @param get_attribute_values Function to retrieve attribute
                 values for the specific platform, called like this:
                 get_attribute_values([attr_id], from_time)
                 for each attr_id in the platform.
        @param notify_driver_event Callback to notify whenever a value is
                retrieved.
        """

        self._platform_id = platform_id
        self._attr_info = attr_info
        self._stream_info = stream_info
        self._get_attribute_values = get_attribute_values
        self._notify_driver_event = notify_driver_event

        log.debug("%r: PlatformResourceMonitor instance created", self._platform_id)

        # _monitors: dict { rate_secs: ResourceMonitor }
        self._monitors = {}

        # buffers used by the monitoring greenlets to put retrieved data in
        # and by the publisher greenlet to process that data to construct
        # aggregated AttributeValueDriverEvent objects that the platform
        # agent finally process to create and publish granules.
        self._buffers = {}

        # to synchronize access to the buffers
        self._lock = RLock()

        # publishing rate in seconds, set by _set_publisher_rate
        self._pub_rate = None
        self._publisher_active = False

        # for debugging purposes
        self._pp = pprint.PrettyPrinter()

    def _group_by_monitoring_rate(self, group_size_secs=1):
        """
        Groups the list of attr defs according to similar monitoring rate.

        @param group_size_secs
                    each group will contain the attributes having a
                    monitoring rate within this interval in seconds.
                    By default, 1).

        @return { rate_secs : [attr_defn, ...], ... },
                    where rate_secs is an int indicating the monitoring
                    rate to be used for the corresponding list of attr defs.
        """

        # first, collect attrDefs by individual rate:
        by_rate = {}  # { rate: [attrDef, ...], ... }
        for attr_defn in self._attr_info.itervalues():
            if 'monitor_cycle_seconds' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'monitor_cycle_seconds', attr_defn)
                continue

            rate = float(attr_defn['monitor_cycle_seconds'])
            if not rate in by_rate:
                by_rate[rate] = []
            by_rate[rate].append(attr_defn)

        groups = {}
        if not by_rate:
            # no attributes to monitor, just return the empty grouping:
            return groups

        # merge elements in groups from by_rate having a similar rate
        prev_rates = []
        prev_defns = []
        for rate in sorted(by_rate.iterkeys()):
            attr_defns = by_rate[rate]

            if not prev_rates:
                # first pass in the iteration.
                prev_rates.append(rate)
                prev_defns += attr_defns

            elif abs(rate - min(prev_rates)) < group_size_secs:
                # merge this similar element:
                prev_rates.append(rate)
                prev_defns += attr_defns

            else:
                # group completed: it is indexed by the maximum
                # of the collected previous rates:
                groups[max(prev_rates)] = prev_defns
                # re-init stuff for next group:
                prev_defns = attr_defns
                prev_rates = [rate]

        if prev_rates:
            # last group completed:
            groups[max(prev_rates)] = prev_defns

        if log.isEnabledFor(logging.DEBUG):  # pragma: not cover
            from pprint import PrettyPrinter
            log.debug("%r: _group_by_monitoring_rate = %s",
                      self._platform_id, PrettyPrinter().pformat(groups))
        return groups

    def start_resource_monitoring(self):
        """
        Starts greenlets to periodically retrieve values of the attributes
        associated with my platform, and to generate aggregated events that
        will be used by the platform agent to create and publish
        corresponding granules.
        """

        log.debug("%r: starting resource monitoring: attr_info=%s",
                  self._platform_id, self._attr_info)

        self._init_buffers()

        # attributes are grouped by similar monitoring rate so a single
        # greenlet is used for each group:
        groups = self._group_by_monitoring_rate()
        for rate_secs, attr_defns in groups.iteritems():
            self._start_monitor_greenlet(rate_secs, attr_defns)

        if self._monitors:
            self._start_publisher_greenlet()

    def _start_monitor_greenlet(self, rate_secs, attr_defns):
        """
        Creates and starts a ResourceMonitor
        """
        log.debug("%r: _start_monitor_greenlet rate_secs=%s attr_defns=%s",
                  self._platform_id, rate_secs, attr_defns)

        resmon = ResourceMonitor(self._platform_id,
                                 rate_secs, attr_defns,
                                 self._get_attribute_values,
                                 self._receive_from_monitor)
        self._monitors[rate_secs] = resmon
        resmon.start()

    def stop_resource_monitoring(self):
        """
        Stops the publisher greenlet and all the monitoring greenlets.
        """
        log.debug("%r: stopping resource monitoring", self._platform_id)

        self._stop_publisher_greenlet()

        for resmon in self._monitors.itervalues():
            resmon.stop()
        self._monitors.clear()

        with self._lock:
            self._buffers.clear()

    def destroy(self):
        """
        Simply calls self.stop_resource_monitoring()
        """
        self.stop_resource_monitoring()

    def _init_buffers(self):
        """
        Initializes self._buffers (empty arrays for each attribute)
        """
        self._buffers = {}
        for attr_key, attr_defn in self._attr_info.iteritems():
            if 'monitor_cycle_seconds' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'monitor_cycle_seconds', attr_defn)
                continue
            if 'ion_parameter_name' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'ion_parameter_name', attr_defn)
                continue

          

            self._buffers[attr_defn['ion_parameter_name']] = []
            
            log.debug('*********%r: Created Buffer for =%s',
                      self._platform_id, attr_defn['ion_parameter_name'])

    def _receive_from_monitor(self, driver_event):
        """
        Callback to receive data from the monitoring greenlets and update the
        internal buffer for further processing by the publisher greenlet.

        @param driver_event An AttributeValueDriverEvent
        """
        with self._lock:
            if len(self._buffers) == 0:
                # we are not currently monitoring.
                return

            log.debug('%r: received driver_event from monitor=%s',
                      self._platform_id, driver_event)

            for param_name, param_value in driver_event.vals_dict.iteritems():
                if param_name not in self._buffers:
                    log.warn("unexpected: param_name %s does not in list of available buffers %s ",
                         param_name,
                         self._buffers)
                    continue
                self._buffers[param_name] += param_value

    def _set_publisher_rate(self):
        """
        Gets the rate for the publisher greenlet.
        This is equal to the minimum of the monitoring rates.
        """
        self._pub_rate = min(self._monitors.keys())

    def _start_publisher_greenlet(self):
        if self._publisher_active == True:
            return
        
        self._set_publisher_rate()

        self._publisher_active = True
        runnable = Greenlet(self._run_publisher)
        runnable.start()
        log.debug("%r: publisher greenlet started, dispatch rate=%s",
                  self._platform_id, self._pub_rate)

    def _run_publisher(self):
        """
        The target run function for the publisher greenlet.
        """
        while self._publisher_active:

            # loop to incrementally sleep up to self._pub_rate while promptly
            # reacting to request for termination
            slept = 0
            while self._publisher_active and slept < self._pub_rate:
                # sleep in increments of 0.5 secs
                incr = min(0.5, self._pub_rate - slept)
                sleep(incr)
                slept += incr

            # dispatch publication (if still active):
            with self._lock:
                if self._publisher_active:
                    self._dispatch_publication()

        log.debug("%r: publisher greenlet stopped. _pub_rate=%s",
                  self._platform_id, self._pub_rate)

    def _dispatch_publication(self):
        """
        Inspects the collected data in the buffers to create and notify an
        aggregated AttributeValueDriverEvent.

        Keeps all samples for each attribute, reporting all associated timestamps
        and filling with None values for missing values at particular timestamps,
        but an attribute is included *only* if it has at least an actual value.

        @note The platform agent will translate any None entries to
              corresponding fill_values.
        """
        log.debug("%r: _dispatch_publication: %s", self._platform_id,self._buffers)
        
                 
                
        
        
        
        # step 1:
        # - collect all actual values in a dict indexed by timestamp
        # - keep track of the attributes having actual values
        by_ts = {}  # { ts0 : { attr_n : val_n, ... }, ... }
        attrs_with_actual_values = set()
        for attr_id, attr_vals in self._buffers.iteritems():

            for v, ts in attr_vals:
                if not ts in by_ts:
                    by_ts[ts] = {}

                by_ts[ts][attr_id] = v

                attrs_with_actual_values.add(attr_id)

            # re-init buffer for this attribute:
            self._buffers[attr_id] = []

        if not attrs_with_actual_values:
            # No new data collected at all; nothing to publish, just return:
            log.debug("%r: _dispatch_publication: no new data collected.", self._platform_id)
            return

        """
        # step 2:
        # - put None's for any missing attribute value per timestamp:
        for ts in by_ts:
            # only do this for attrs_with_actual_values:
            # (note: these attributes do have actual values, but not necessarily
            # at every reported timestamp in this cycle):
            for attr_id in attrs_with_actual_values:
                if not attr_id in by_ts[ts]:
                    by_ts[ts][attr_id] = None
        """

        # step 2:
        # - put None's for any missing attribute value per timestamp:
        # EH. Here I used all attributes instead of only the measured ones
        # so the agent can properly populate rdts and construct granules.
        for ts in by_ts:
            # only do this for attrs_with_actual_values:
            # (note: these attributes do have actual values, but not necessarily
            # at every reported timestamp in this cycle):
            for attr_id in self._buffers.keys():
                if not attr_id in by_ts[ts]:
                    by_ts[ts][attr_id] = None

        """        
        # step 3:
        # - construct vals_dict for the event:
        vals_dict = {}
        for attr_id in attrs_with_actual_values:
            vals_dict[attr_id] = []
            for ts in sorted(by_ts.iterkeys()):
                val = by_ts[ts][attr_id]
                vals_dict[attr_id].append((val, ts))
        """
        
        # step 3:
        # - construct vals_dict for the event:
        # EH. Here I used all attributes instead of only the measured ones
        # so the agent can properly populate rdts and construct granules.
        vals_dict = {}
        for attr_id in self._buffers.keys():
            vals_dict[attr_id] = []
            for ts in sorted(by_ts.iterkeys()):
                val = by_ts[ts][attr_id]
                vals_dict[attr_id].append((val, ts))
                
        """
        new Step 4: MikeH - The buffers have data from all possible streams form this
        node - So go through and put them into their own set of buffers before before publishing each stream
        """
         
        for stream_name, stream_config in self._stream_info.iteritems():
            if 'stream_def_dict' not in stream_config:
                msg = "_dispatch_publication: validate_configuration: 'stream_def_dict' key not in configuration for stream %r" % stream_name
                log.error(msg)
                return
            else :
#                log.trace("%r: _dispatch_publication: stream name %s stream dict %s", self._platform_id,stream_name,stream_config['stream_def_dict'])
                log.trace("%r: _dispatch_publication: stream name %s stream ", self._platform_id,stream_name)
            
            stream_vals_dict = {}
            
            for attr_name in stream_config['stream_def_dict']['parameter_dictionary'].iterkeys():
                log.trace("%r: _dispatch_publication: stream name %s attr %s", self._platform_id,stream_name,attr_name)
                
                for attr_id in vals_dict:
                    if attr_id == attr_name :
                        stream_vals_dict[attr_id] = vals_dict[attr_id]
                        log.trace("%r: _dispatch_publication: stream name %s attr %s copied to stream_vals_dict", self._platform_id,stream_name,attr_name)
  

        # finally, create and notify event:
            
            #remove any time-slices that do not have any attribute values for this stream
            #before publishing
            compact_stream_vals_dict = self._remove_empty_ts(stream_vals_dict)
            
            
            if len(compact_stream_vals_dict) > 0 :
            
                
                
                
                log.trace("%r: _dispatch_publication: stream name %s attr %s copied to stream_vals_dict", self._platform_id,stream_name,attr_name)
            
                driver_event = AttributeValueDriverEvent(self._platform_id,
                                                 stream_name,
                                                 compact_stream_vals_dict)

                log.debug("%r: _dispatch_publication: notifying event: %s",
                                  self._platform_id, driver_event)

                if log.isEnabledFor(logging.TRACE):  # pragma: no cover
                   log.trace("%r: vals_dict:\n%s",
                                self._platform_id, self._pp.pformat(driver_event.vals_dict))

                self._notify_driver_event(driver_event)

    def _remove_empty_ts(self,stream_vals_dict):
        # Go through and remove any timestamps that have all attributes set to None            
        compact_stream_vals_dict = {}
        by_ts = {}

        # Count the number of attributes not equal to None for each timestamp
        for attr_id in stream_vals_dict:
            vals_list = stream_vals_dict[attr_id]
            for attr_pair in vals_list :
                if attr_pair[1] not in by_ts:
                    by_ts[attr_pair[1]]=0
                if attr_pair[0]!=None:
                    by_ts[attr_pair[1]]+=1
           
        # make a new list with just timestamps that had at least one attribute != None
        # and then add it to the compact_stream_vals 
        # compact_stream_vals could be empty on return   
        for attr_id in stream_vals_dict:
            compact_list = []
            vals_list = stream_vals_dict[attr_id]
            for attr_pair in vals_list :
                if by_ts[attr_pair[1]]>0:
                    compact_list.append(attr_pair)
            
            if len(compact_list)>0 :
                compact_stream_vals_dict[attr_id]=compact_list 
            
        return(compact_stream_vals_dict)


    def _stop_publisher_greenlet(self):
        if self._publisher_active:
            log.debug("%r: stopping publisher greenlet", self._platform_id)
            self._publisher_active = False
