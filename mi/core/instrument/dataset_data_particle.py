#!/usr/bin/env python

"""
@package mi.core.instrument.data_particle_generator Base data particle generator
@file mi/core/instrument/data_particle_generator.py
@author Steve Foley
@brief Contains logic to generate data particles to be exchanged between
the driver and agent. This involves a JSON interchange format
"""

import time
import ntplib
import base64
import json

from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException, ReadOnlyException, NotImplementedException, InstrumentParameterException
from mi.core.log import get_logger

log = get_logger()

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'


class CommonDataParticleType(BaseEnum):
    """
    This enum defines all the common particle types defined in the modules.  Currently there is only one, but by
    using an enum here we have the opportunity to define more common data particles.
    """
    RAW = "raw"


class DataParticleKey(BaseEnum):
    PKT_FORMAT_ID = "pkt_format_id"
    PKT_VERSION = "pkt_version"
    STREAM_NAME = "stream_name"
    INTERNAL_TIMESTAMP = "internal_timestamp"
    PORT_TIMESTAMP = "port_timestamp"
    DRIVER_TIMESTAMP = "driver_timestamp"
    PREFERRED_TIMESTAMP = "preferred_timestamp"
    QUALITY_FLAG = "quality_flag"
    VALUES = "values"
    VALUE_ID = "value_id"
    VALUE = "value"
    BINARY = "binary"
    NEW_SEQUENCE = "new_sequence"


class DataParticleValue(BaseEnum):
    JSON_DATA = "JSON_Data"
    ENG = "eng"
    OK = "ok"
    CHECKSUM_FAILED = "checksum_failed"
    OUT_OF_RANGE = "out_of_range"
    INVALID = "invalid"
    QUESTIONABLE = "questionable"


class DataParticle(object):
    """
    This class is responsible for storing and ultimately generating data
    particles in the designated format from the associated inputs. It
    fills in fields as necessary, and is a valid Data Particle
    that can be sent up to the InstrumentAgent.

    It is the intent that this class is subclassed as needed if an instrument must
    modify fields in the outgoing packet. The hope is to have most of the superclass
    code be called by the child class with just values overridden as needed.
    """

    # data particle type is intended to be defined in each derived data particle class.  This value should be unique
    # for all data particles.  Best practice is to access this variable using the accessor method:
    # data_particle_type()
    _data_particle_type = None

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=None,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        """ Build a particle seeded with appropriate information

        @param raw_data The raw data used in the particle
        """
        if new_sequence is not None and not isinstance(new_sequence, bool):
            raise TypeError("new_sequence is not a bool")

        self.contents = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.INTERNAL_TIMESTAMP: internal_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: ntplib.system_to_ntp_time(time.time()),
            DataParticleKey.PREFERRED_TIMESTAMP: preferred_timestamp,
            DataParticleKey.QUALITY_FLAG: quality_flag,
        }
        self._encoding_errors = []
        if new_sequence is not None:
            self.contents[DataParticleKey.NEW_SEQUENCE] = new_sequence

        self.raw_data = raw_data
        self._values = None

    def __eq__(self, arg):
        """
        Quick equality check for testing purposes. If they have the same raw
        data, timestamp, they are the same enough for this particle
        """
        allowed_diff = .000001
        if self._data_particle_type != arg._data_particle_type:
            log.debug('Data particle type does not match: %s %s', self._data_particle_type, arg._data_particle_type)
            return False

        if self.raw_data != arg.raw_data:
            log.debug('Raw data does not match')
            return False

        t1 = self.contents[DataParticleKey.INTERNAL_TIMESTAMP]
        t2 = arg.contents[DataParticleKey.INTERNAL_TIMESTAMP]

        if (t1 is None) or (t2 is None):
            tdiff = allowed_diff
        else:
            tdiff = abs(t1 - t2)

        if tdiff > allowed_diff:
            log.debug('Timestamp %s does not match %s', t1, t2)
            return False

        generated1 = json.loads(self.generate())
        generated2 = json.loads(arg.generate())
        missing, differing = self._compare(generated1, generated2, ignore_keys=[DataParticleKey.DRIVER_TIMESTAMP,
                                                                                DataParticleKey.PREFERRED_TIMESTAMP])
        if missing:
            log.error('Key mismatch between particle dictionaries: %r', missing)
            return False

        if differing:
            log.error('Value mismatch between particle dictionaries: %r', differing)

        return True

    @staticmethod
    def _compare(d1, d2, ignore_keys=None):
        ignore_keys = ignore_keys if ignore_keys else []
        missing = set(d1).symmetric_difference(d2)
        differing = {}
        for k in d1:
            if k in ignore_keys or k in missing:
                continue
            if d1[k] != d2[k]:
                differing[k] = (d1[k], d2[k])

        return missing, differing

    def set_internal_timestamp(self, timestamp=None, unix_time=None):
        """
        Set the internal timestamp
        @param timestamp: NTP timestamp to set
        @param unit_time: Unix time as returned from time.time()
        @raise InstrumentParameterException if timestamp or unix_time not supplied
        """
        if timestamp is None and unix_time is None:
            raise InstrumentParameterException("timestamp or unix_time required")

        if unix_time is not None:
            timestamp = ntplib.system_to_ntp_time(unix_time)

        # Do we want this to happen here or in down stream processes?
        # if(not self._check_timestamp(timestamp)):
        #    raise InstrumentParameterException("invalid timestamp")

        self.contents[DataParticleKey.INTERNAL_TIMESTAMP] = float(timestamp)

    def set_port_timestamp(self, timestamp=None, unix_time=None):
        """
        Set the port timestamp
        @param timestamp: NTP timestamp to set
        @param unix_time: Unix time as returned from time.time()
        @raise InstrumentParameterException if timestamp or unix_time not supplied
        """
        if timestamp is None and unix_time is None:
            raise InstrumentParameterException("timestamp or unix_time required")

        if unix_time is not None:
            timestamp = ntplib.system_to_ntp_time(unix_time)

        # Do we want this to happen here or in down stream processes?
        if not self._check_timestamp(timestamp):
            raise InstrumentParameterException("invalid timestamp")

        self.contents[DataParticleKey.PORT_TIMESTAMP] = float(timestamp)

    def set_value(self, id, value):
        """
        Set a content value, restricted as necessary

        @param id The ID of the value to set, should be from DataParticleKey
        @param value The value to set
        @raises ReadOnlyException If the parameter cannot be set
        """
        if (id == DataParticleKey.INTERNAL_TIMESTAMP) and (self._check_timestamp(value)):
            self.contents[DataParticleKey.INTERNAL_TIMESTAMP] = value
        else:
            raise ReadOnlyException("Parameter %s not able to be set to %s after object creation!" %
                                    (id, value))

    def get_value(self, id):
        """ Return a stored value from contents

        @param id The ID (from DataParticleKey) for the parameter to return
        @raises NotImplementedException If there is an invalid id
        """
        if DataParticleKey.has(id):
            return self.contents[id]
        else:
            raise NotImplementedException("Value %s not available in particle!", id)

    def get_value_from_values(self, value_id):
        """ Return a stored value from values list

        @param value_id The ID of the parameter to return
        """
        if not self._values:
            return None
        values = [i for i in self._values if i[DataParticleKey.VALUE_ID] == value_id]
        if not values:
            return None
        return values[0][DataParticleKey.VALUE]


    def data_particle_type(self):
        """
        Return the data particle type (aka stream name)
        @raise: NotImplementedException if _data_particle_type is not set
        """
        if self._data_particle_type is None:
            raise NotImplementedException("_data_particle_type not initialized")

        return self._data_particle_type

    def generate_dict(self):
        """
        Generate a simple dictionary of sensor data and timestamps, without
        going to JSON. This is useful for the times when JSON is not needed to
        go across an interface. There are times when particles are used
        internally to a component/process/module/etc.
        @retval A python dictionary with the proper timestamps and data values
        @throws InstrumentDriverException if there is a problem wtih the inputs
        """
        # verify preferred timestamp exists in the structure...
        if not self._check_preferred_timestamps():
            raise SampleException("Preferred timestamp not in particle!")

        # build response structure
        self._encoding_errors = []
        if self._values is None:
            self._values = self._build_parsed_values()
        result = self._build_base_structure()
        result[DataParticleKey.STREAM_NAME] = self.data_particle_type()
        result[DataParticleKey.VALUES] = self._values

        return result

    def generate(self, sorted=False):
        """
        Generates a JSON_parsed packet from a sample dictionary of sensor data and
        associates a timestamp with it

        @param sorted Returned sorted json dict, useful for testing, but slow,
           so dont do it unless it is important
        @return A JSON_raw string, properly structured with port agent time stamp
           and driver timestamp
        @throws InstrumentDriverException If there is a problem with the inputs
        """
        json_result = json.dumps(self.generate_dict(), sort_keys=sorted)
        return json_result

    def _build_parsed_values(self):
        """
        Build values of a parsed structure. Just the values are built so
        so that a child class can override this class, but call it with
        super() to get the base structure before modification

        @return the values tag for this data structure ready to JSONify
        @raises SampleException when parsed values can not be properly returned
        """
        raise SampleException("Parsed values block not overridden")

    def _build_base_structure(self):
        """
        Build the base/header information for an output structure.
        Follow on methods can then modify it by adding or editing values.

        @return A fresh copy of a core structure to be exported
        """
        result = dict(self.contents)
        # clean out optional fields that were missing
        if not self.contents[DataParticleKey.PORT_TIMESTAMP]:
            del result[DataParticleKey.PORT_TIMESTAMP]
        if not self.contents[DataParticleKey.INTERNAL_TIMESTAMP]:
            del result[DataParticleKey.INTERNAL_TIMESTAMP]
        return result

    def _check_timestamp(self, timestamp):
        """
        Check to make sure the timestamp is reasonable

        @param timestamp An NTP4 formatted timestamp (64bit)
        @return True if timestamp is okay or None, False otherwise
        """
        if timestamp is None:
            return True
        if not isinstance(timestamp, float):
            return False

        # is it sufficiently in the future to be unreasonable?
        if timestamp > ntplib.system_to_ntp_time(time.time() + (86400 * 365)):
            return False
        else:
            return True

    def _check_preferred_timestamps(self):
        """
        Check to make sure the preferred timestamp indicated in the
        particle is actually listed, possibly adjusting to 2nd best
        if not there.

        @throws SampleException When there is a problem with the preferred
            timestamp in the sample.
        """
        if self.contents[DataParticleKey.PREFERRED_TIMESTAMP] is None:
            raise SampleException("Missing preferred timestamp, %s, in particle" %
                                  self.contents[DataParticleKey.PREFERRED_TIMESTAMP])

        # This should be handled downstream.  Don't want to not publish data because
        # the port agent stopped putting out timestamps
        # if self.contents[self.contents[DataParticleKey.PREFERRED_TIMESTAMP]] == None:
        #    raise SampleException("Preferred timestamp, %s, is not defined" %
        #                          self.contents[DataParticleKey.PREFERRED_TIMESTAMP])

        return True

    def _encode_value(self, name, value, encoding_function, value_range=None):
        """
        Encode a value using the encoding function, if it fails store the error in a queue

        :param value_range  tuple containing min/max numerical values or min/max lengths
        """
        encoded_val = None

        # noinspection PyBroadException
        # - custom encoding_function exceptions are not known a priori
        try:
            encoded_val = encoding_function(value)

        except ValueError as e:
            log.error('Unable to convert %s to %s.', encoded_val, encoding_function)
            self._encoding_errors.append({name: value})
        except Exception as e:
            log.error('Data particle error encoding. Name: %s Value: %s, Encoding: %s', name, value, encoding_function)
            self._encoding_errors.append({name: value})

        # optional range checking
        if value_range:
            try:
                vmin, vmax = value_range

            except ValueError as e:  # this only occurs as a programming error and should cause the parser to exit
                log.exception('_encode_value must have exactly two values for tuple argument value_range')
                raise ValueError(e)

            if encoding_function in [int, float]:
                if vmin and encoded_val < vmin:
                    log.error('Particle value (%s) below minimum threshold (%s < %s)', name, value, vmin)
                    self._encoding_errors.append({name: value})
                elif vmax and encoded_val > vmax:
                    log.error('Particle value (%s) exceeds maximum threshold (%s > %s)', name, value, vmax)
                    self._encoding_errors.append({name: value})
            elif hasattr(encoded_val, '__len__'):
                try:
                    if vmin and len(encoded_val) < vmin:
                        log.error('Particle value (%s) length below minimum threshold (%s < %s)',
                                  name, value, vmin)
                        self._encoding_errors.append({name: value})
                    elif vmax and len(encoded_val) > vmax:
                        log.error('Particle value (%s) length exceeds maximum threshold (%s > %s)',
                                  name, value, vmax)
                        self._encoding_errors.append({name: value})
                # in the unlikely event that a range was specified and the encoding object created a bogus len()
                # we'll just ignore the range check
                except TypeError:
                    log.warning('_encode_value received an encoding function (%s) that claimed to implement len() but '
                                'does not. Unable to apply range test to %s', encoding_function, name)

        return {DataParticleKey.VALUE_ID: name,
                DataParticleKey.VALUE: encoded_val}

    def get_encoding_errors(self):
        """
        Return the encoding errors list
        """
        return self._encoding_errors


class RawDataParticleKey(BaseEnum):
    PAYLOAD = "raw"
    LENGTH = "length"
    TYPE = "type"
    CHECKSUM = "checksum"


class RawDataParticle(DataParticle):
    """
    This class a common data particle for generating data particles of raw
    data.

    It essentially is a translation of the port agent packet
    """
    _data_particle_type = CommonDataParticleType.RAW

    def _build_parsed_values(self):
        """
        Build a particle out of a port agent packet.
        @returns A list that is ready to be added to the "values" tag before
           the structure is JSONified
        """

        port_agent_packet = self.raw_data
        if not isinstance(port_agent_packet, dict):
            raise SampleException("raw data not a dictionary")

        for param in ["raw", "length", "type", "checksum"]:
            if param not in port_agent_packet:
                raise SampleException("raw data not a complete port agent packet. missing %s" % param)

        payload = None
        length = None
        type = None
        checksum = None

        # Attempt to convert values
        try:
            payload = base64.b64encode(port_agent_packet.get("raw"))
        except TypeError:
            pass

        try:
            length = int(port_agent_packet.get("length"))
        except TypeError:
            pass

        try:
            type = int(port_agent_packet.get("type"))
        except TypeError:
            pass

        try:
            checksum = int(port_agent_packet.get("checksum"))
        except TypeError:
            pass

        result = [{
            DataParticleKey.VALUE_ID: RawDataParticleKey.PAYLOAD,
            DataParticleKey.VALUE: payload,
            DataParticleKey.BINARY: True},
            {
                DataParticleKey.VALUE_ID: RawDataParticleKey.LENGTH,
                DataParticleKey.VALUE: length},
            {
                DataParticleKey.VALUE_ID: RawDataParticleKey.TYPE,
                DataParticleKey.VALUE: type},
            {
                DataParticleKey.VALUE_ID: RawDataParticleKey.CHECKSUM,
                DataParticleKey.VALUE: checksum},
        ]

        return result
