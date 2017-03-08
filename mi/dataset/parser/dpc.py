#!/usr/bin/env python

"""
@package mi.dataset.parser.dpc
@file marine-integrations/mi/dataset/parser/dpc.py
@author Pete Cable
"""
import msgpack
import ntplib
import struct
from mi.core.exceptions import SampleException, RecoverableSampleException
from mi.core.instrument.dataset_data_particle import DataParticle, DataParticleKey
from mi.core.log import get_logger, get_logging_metaclass
from mi.dataset.dataset_parser import SimpleParser
from mi.core.common import BaseEnum

log = get_logger()
METACLASS = get_logging_metaclass('trace')

__author__ = 'Peter Cable'
__license__ = 'Apache 2.0'


ACS_STRUCT = struct.Struct('>BB4s7HIBB340H')


class DataParticleType(BaseEnum):
    # Data particle types for the Deep Profiler
    ACM = 'dpc_acm_instrument_recovered'
    ACS = 'dpc_acs_instrument_recovered'
    CTD = 'dpc_ctd_instrument_recovered'
    FLCD = 'dpc_flcdrtd_instrument_recovered'
    FLNTU = 'dpc_flnturtd_instrument_recovered'
    MMP = 'dpc_mmp_instrument_recovered'
    OPTODE = 'dpc_optode_instrument_recovered'


class DeepProfileParticleKey(BaseEnum):
    RAW_SECS = 'raw_time_seconds'
    RAW_MSECS = 'raw_time_microseconds'


class ACSDataParticleKey(BaseEnum):
    PACKET_TYPE = 'packet_type'
    METER_TYPE = 'meter_type'
    SERIAL_NUMBER = 'serial_number'
    A_REFERENCE_DARK_COUNTS = 'a_reference_dark_counts'
    PRESSURE_COUNTS = 'pressure_counts'
    A_SIGNAL_DARK_COUNTS = 'a_signal_dark_counts'
    EXTERNAL_TEMP_RAW = 'external_temp_raw'
    INTERNAL_TEMP_RAW = 'internal_temp_raw'
    C_REFERENCE_DARK_COUNTS = 'c_reference_dark_counts'
    C_SIGNAL_DARK_COUNTS = 'c_signal_dark_counts'
    ELAPSED_RUN_TIME = 'elapsed_run_time'
    NUM_WAVELENGTHS = 'num_wavelengths'
    C_REFERENCE_COUNTS = 'c_reference_counts'
    A_REFERENCE_COUNTS = 'a_reference_counts'
    C_SIGNAL_COUNTS = 'c_signal_counts'
    A_SIGNAL_COUNTS = 'a_signal_counts'


class DeepProfilerParser(SimpleParser):
    def __init__(self, config, stream_handle, exception_callback):
        super(DeepProfilerParser, self).__init__(config, stream_handle, exception_callback)
        self._particle_type = None
        self._gen = None

    def parse_file(self):
        for record in msgpack.Unpacker(self._stream_handle):
            try:
                particle = DeepProfilerParticle(record, preferred_timestamp=DataParticleKey.INTERNAL_TIMESTAMP)
                self._record_buffer.append(particle)
            except (SampleException, RecoverableSampleException) as e:
                self._exception_callback(e)


class DeepProfilerParticle(DataParticle):
    # map of instrument parameter names to particle parameter names
    parameter_map = {
        'preswat': 'pressure',
        'tempwat': 'temp',
        'condwat': 'conductivity_millisiemens',
        'doconcs': 'calibrated_phase',
        't': 'optode_temperature',
        'va': 'vel3d_a_va',
        'vb': 'vel3d_a_vb',
        'vc': 'vel3d_a_vc',
        'vd': 'vel3d_a_vd',
        'hx': 'vel3d_a_hx',
        'hy': 'vel3d_a_hy',
        'hz': 'vel3d_a_hz',
        'tx': 'vel3d_a_tx',
        'ty': 'vel3d_a_ty',
        'current': 'wfp_prof_current',
        'mode': 'operating_mode',
        'pnum': 'wfp_profile_number',
        'pressure': 'wfp_prof_pressure',
        'vbatt': 'wfp_prof_voltage'
    }

    # map of instrument stream names to particle names
    particle_map = {
        'hx': DataParticleType.ACM,
        'condwat': DataParticleType.CTD,
        'cdomflo': DataParticleType.FLCD,
        'chlaflo': DataParticleType.FLNTU,
        'current': DataParticleType.MMP,
        'doconcs': DataParticleType.OPTODE
    }

    __metaclass__ = METACLASS

    def __init__(self, *args, **kwargs):
        super(DeepProfilerParticle, self).__init__(*args, **kwargs)
        self._data_particle_type = self._find_particle_type()

    def _find_particle_type(self):
        if len(self.raw_data) != 3:
            raise SampleException('Invalid sample, does not contain the correct record size')

        data = self.raw_data[2]
        if type(data) == str:
            return DataParticleType.ACS
        elif type(data) == dict:
            return self.particle_map.get(sorted(data.keys())[0])

        if self._data_particle_type is None:
            raise SampleException('Invalid sample, unable to determine particle type')

    def _build_parsed_values(self):
        try:
            seconds, microseconds, data = self.raw_data
            self.set_internal_timestamp(ntplib.system_to_ntp_time(seconds) + microseconds/1e6)
            self.timelist = [
                {DataParticleKey.VALUE_ID: DeepProfileParticleKey.RAW_SECS, DataParticleKey.VALUE: seconds},
                {DataParticleKey.VALUE_ID: DeepProfileParticleKey.RAW_MSECS, DataParticleKey.VALUE: microseconds}
            ]
        except:
            raise SampleException('Invalid sample, unable to parse timestamp')

        if self._data_particle_type == DataParticleType.ACS:
            return self.build_acs_parsed_values(data)

        if not isinstance(data, dict):
            raise SampleException('Invalid sample, does not contain data dictionary')

        return self.timelist + [{DataParticleKey.VALUE_ID: DeepProfilerParticle.parameter_map.get(k, k),
                                 DataParticleKey.VALUE: v} for k, v in data.iteritems()]

    def build_acs_parsed_values(self, data):
        if not len(data) == ACS_STRUCT.size:
            raise SampleException('Received invalid ACS data (incorrect length %d, expected %d' %
                                  (len(data), ACS_STRUCT.size))

        record = ACS_STRUCT.unpack(data)
        packet_type, _, type_and_serial, aref_dark, pressure, asig_dark, raw_ext_temp,\
            raw_int_temp, cref_dark, csig_dark, msecs, _, count = record[:13]

        meter_type = ord(type_and_serial[0])
        serial_number = struct.unpack('>I', '\x00' + type_and_serial[1:])[0]

        data_records = record[13:]
        raw_cref = list(data_records[0::4])
        raw_aref = list(data_records[1::4])
        raw_csig = list(data_records[2::4])
        raw_asig = list(data_records[3::4])

        return self.timelist + [
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.PACKET_TYPE, DataParticleKey.VALUE: packet_type},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.METER_TYPE, DataParticleKey.VALUE: meter_type},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.SERIAL_NUMBER, DataParticleKey.VALUE: str(serial_number)},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.A_REFERENCE_DARK_COUNTS, DataParticleKey.VALUE: aref_dark},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.PRESSURE_COUNTS, DataParticleKey.VALUE: pressure},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.A_SIGNAL_DARK_COUNTS, DataParticleKey.VALUE: asig_dark},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.EXTERNAL_TEMP_RAW, DataParticleKey.VALUE: raw_ext_temp},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.INTERNAL_TEMP_RAW, DataParticleKey.VALUE: raw_int_temp},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.C_REFERENCE_DARK_COUNTS, DataParticleKey.VALUE: cref_dark},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.C_SIGNAL_DARK_COUNTS, DataParticleKey.VALUE: csig_dark},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.ELAPSED_RUN_TIME, DataParticleKey.VALUE: msecs},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.NUM_WAVELENGTHS, DataParticleKey.VALUE: count},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.C_REFERENCE_COUNTS, DataParticleKey.VALUE: raw_cref},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.A_REFERENCE_COUNTS, DataParticleKey.VALUE: raw_aref},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.C_SIGNAL_COUNTS, DataParticleKey.VALUE: raw_csig},
            {DataParticleKey.VALUE_ID: ACSDataParticleKey.A_SIGNAL_COUNTS, DataParticleKey.VALUE: raw_asig},
        ]
