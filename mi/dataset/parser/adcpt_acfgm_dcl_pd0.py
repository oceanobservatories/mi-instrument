#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_acfgm_dcl_pd0
@file marine-integrations/mi/dataset/parser/adcpt_acfgm_dcl_pd0.py
@author Jeff Roy
@brief Particle and Parser classes for the adcpt_acfgm_dcl_pd0 drivers
The particles are parsed by the common PD0 Parser and
Abstract particle class in file adcp_pd0.py
"""
import binascii
import re

import ntplib

import mi.dataset.parser.adcp_pd0 as adcp_pd0
from mi.core.common import BaseEnum
from mi.core.exceptions import RecoverableSampleException
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.core.log import get_logger
from mi.dataset.dataset_parser import Parser
from mi.dataset.parser import utilities
from mi.dataset.parser.pd0_parser import AdcpPd0Record, PD0ParsingException, InsufficientDataException

__author__ = 'Jeff Roy'
__license__ = 'Apache 2.0'

log = get_logger()
DATA_RE = re.compile(
    r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\[adcpt:DLOGP4\]:)?([0-9A-F]+?)((?=\d{4}/)|(?=\r)|(?=\n))')


class DclKey(BaseEnum):
    # Enumerations for the additional DCL parameters in the adcpt_acfgm_pd0_dcl streams
    # The remainder of the streams are identical to the adcps_jln streams and
    # are handled by the base AdcpPd0DataParticle class
    # this enumeration is also used for the dcl_data_dict
    # of the particle class constructor
    # so includes the additional enumeration 'PD0_DATA'
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    DCL_CONTROLLER_STARTING_TIMESTAMP = 'dcl_controller_starting_timestamp'
    PD0_DATA = 'pd0_data'


PD0_START_STRING = '\x7f\x7f'


class AdcptAcfgmDclPd0Parser(Parser):
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback,  # shouldn't be optional anymore
                 state_callback=None,  # No longer used
                 publish_callback=None):  # No longer used

        self._file_parsed = False
        self._record_buffer = []
        self._last_values = {}

        super(AdcptAcfgmDclPd0Parser, self).__init__(config,
                                                     stream_handle,
                                                     None,  # State no longer used
                                                     None,  # Sieve function no longer used
                                                     state_callback,
                                                     publish_callback,
                                                     exception_callback)

    def _changed(self, particle):
        particle_dict = particle.generate_dict()
        stream = particle_dict.get('stream_name')
        values = particle_dict.get('values')
        last_values = self._last_values.get(stream)
        if values == last_values:
            return False

        self._last_values[stream] = values
        return True

    def _parse_file(self):
        pd0_buffer = ''
        ts = None
        count = 0

        # Go through each line in the file
        for line in self._stream_handle:
            log.trace('line: %r', line)

            records = DATA_RE.findall(line)
            if records:
                if ts is None:
                    ts = records[0][0]
                data = ''.join([r[2] for r in records])

                pd0_buffer += binascii.unhexlify(data)

                # Look for start of a PD0 ensemble.  If we have a particle queued up, ship it
                # then reset our state.
                if pd0_buffer.startswith(PD0_START_STRING):
                    try:
                        pd0 = AdcpPd0Record(pd0_buffer)
                        count += 1
                        self._create_particles(pd0, ts)
                        ts = None
                        pd0_buffer = ''
                    except InsufficientDataException:
                        continue
                    except PD0ParsingException as e:
                        self._exception_callback(RecoverableSampleException('Unable to parse PD0: %s' % e))

        # provide an indication that the file was parsed
        self._file_parsed = True
        log.debug('PARSE_FILE create %s particles', len(self._record_buffer))

    def _create_particles(self, pd0, ts):
        utc_time = utilities.dcl_controller_timestamp_to_utc_time(ts)
        utc_time = ntplib.system_to_ntp_time(utc_time)
        velocity = adcp_pd0.VelocityEarth(pd0, port_timestamp=utc_time,
                                          preferred_timestamp=DataParticleKey.PORT_TIMESTAMP)
        self._record_buffer.append(velocity)

        config = adcp_pd0.AdcpsConfig(pd0, port_timestamp=utc_time,
                                      preferred_timestamp=DataParticleKey.PORT_TIMESTAMP)
        engineering = adcp_pd0.AdcpsEngineering(pd0, port_timestamp=utc_time,
                                                preferred_timestamp=DataParticleKey.PORT_TIMESTAMP)

        for particle in [config, engineering]:
            if self._changed(particle):
                self._record_buffer.append(particle)

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested > 0:

            # If the file was not read, let's parse it
            if self._file_parsed is False:
                self._parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return
