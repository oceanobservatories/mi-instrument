#!/usr/bin/env python

"""
@package mi.dataset.driver.vel3d_l.wfp
@file marine-integrations/mi/dataset/driver/vel3d_l/wfp/vel3d_l_wfp_recovered_driver.py
@author Tapana Gupta
@brief Driver for the vel3d_l_wfp instrument

Release notes:

Initial Release
"""

from mi.core.log import get_logger
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.vel3d_l_wfp import Vel3dLWfpParser, Vel3dLWfpDataParticleType
from mi.core.versioning import version

log = get_logger()


@version("0.2.0")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'r') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = Vel3dlWfpRecoveredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class Vel3dlWfpRecoveredDriver(SimpleDatasetDriver):
    """
    Derived vel3d_l_wfp_recovered driver class
    All this needs to do is create a concrete _build_parser method
    """
    def __init__(self, unused, stream_handle, particle_data_handler):
        super(Vel3dlWfpRecoveredDriver, self).__init__(unused, stream_handle, particle_data_handler)
        self._data_particle_record_buffer = []
        self._unexpected_interval_found = False

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dLWfpInstrumentRecoveredParticle',
                                                     'Vel3dLWfpMetadataRecoveredParticle']
        }

        # The parser inherits from simple parser - other callbacks not needed here
        parser = Vel3dLWfpParser(parser_config,
                                 stream_handle,
                                 self._exception_callback)

        return parser

    def processFileStream(self):
        """
        Method to extract records from a parser's get_records method and
        add them to the particle_data_handler passed in by the caller
        """
        try:
            while True:

                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    # Only adjust the times in the data particles, not metadata particles
                    if record.data_particle_type() == Vel3dLWfpDataParticleType.WFP_INSTRUMENT_PARTICLE:
                        self._data_particle_record_buffer.append(record)
                    else:
                        self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

            # Adjust the timestamps of the records in the _data_particle_record_buffer
            self.adjust_sample_times()

            for record in self._data_particle_record_buffer:
                self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

        except Exception as e:
            log.error(e)
            self._particle_data_handler.setParticleDataCaptureFailure()
            self._particle_data_handler._samples = {}

    def adjust_sample_times(self):
        """
        The instrument generates internal timestamps with a precision of seconds. However, since several measurements
        are taken each second, the internal timestamps on consecutive records are identical. Stream engine filters out
        particles with duplicate timestamps so all the data can not be retrieved. To get around this, increment the
        internal timestamps so they are unique.
        """
        st_idx = 0
        interval = 0
        for i in range(1, len(self._data_particle_record_buffer)):
            # Keep iterating through the region (window) of identical timestamps until the timestamp changes
            time_diff = self._data_particle_record_buffer[i].get_value(DataParticleKey.INTERNAL_TIMESTAMP) - \
                        self._data_particle_record_buffer[st_idx].get_value(DataParticleKey.INTERNAL_TIMESTAMP)
            if time_diff == 0:
                continue

            # Timestamps should be 1 second apart, print a single warning message if otherwise
            if time_diff > 1:
                if not self._unexpected_interval_found:
                    log.warning('A %f second interval was found between particles, expected 1.0 seconds' % time_diff)
                self._unexpected_interval_found = True
                time_diff = 1

            # Determine the interval between particles in the window to be the time span of the window
            # divided by the number of particles in the window
            interval = float(time_diff)/(i - st_idx)

            self.adjust_times_in_window(self._data_particle_record_buffer[st_idx:i], interval)

            # set start index to beginning of next window
            st_idx = i

        # Adjust the records in the final window. Use the interval from the previous window
        if st_idx < len(self._data_particle_record_buffer)-1:
            self.adjust_times_in_window(self._data_particle_record_buffer[st_idx:], interval)

    @staticmethod
    def adjust_times_in_window(window, interval):
        if not window:
            return

        # All the internal timestamps in the window are exactly the same so sort by driver timestamp.
        sorted_window = sorted(window, cmp=Vel3dlWfpRecoveredDriver.compare_particle_by_driver_timestamp)

        # increment the internal timestamps by the determined interval
        window_base_ts = sorted_window[0].get_value(DataParticleKey.INTERNAL_TIMESTAMP)
        for window_idx in range(len(sorted_window)):
            new_timestamp = window_base_ts + (interval * window_idx)
            sorted_window[window_idx].set_value(DataParticleKey.INTERNAL_TIMESTAMP, new_timestamp)

    @staticmethod
    def compare_particle_by_driver_timestamp(p1, p2):
        ts1 = p1.get_value_from_values(DataParticleKey.DRIVER_TIMESTAMP)
        ts2 = p2.get_value_from_values(DataParticleKey.DRIVER_TIMESTAMP)

        if ts1 < ts2:
            return -1
        if ts2 < ts1:
            return 1
        return 0


