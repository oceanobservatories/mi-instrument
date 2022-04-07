
__author__ = "msteiner"

from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_driver import ProcessingInfoKey
from mi.core.exceptions import NotImplementedException

from ion_functions.data.ctd_functions import ctd_sbe52mp_preswat

import scipy.interpolate as interpolate

from mi.core.time_tools import ntp_to_string
from mi.core.log import get_logger

log = get_logger()


class WfpCFileDriver(SimpleDatasetDriver):
    """
    A non-sio c-file contains only 1 profile and there is exactly one corresponding e-file, also having only 1 profile.
    Use the time-pressure tuples from this e-file to correct the c-sample times by finding matching pressures
    in the c and e profiles.
    """
    def __init__(self, unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples):
        super(WfpCFileDriver, self).__init__(unused, stream_handle, particle_data_handler)

        self._e_file_time_pressure_tuples = e_file_time_pressure_tuples
        self._data_particle_record_buffer = []

        self.pressure_tolerance = 0.02
        self.pressure_conversion_function = ctd_sbe52mp_preswat

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
                    if record.data_particle_type() == self.pressure_containing_data_particle_stream():
                        self._data_particle_record_buffer.append(record)
                    else:
                        self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

            # Adjust the timestamps of the records in the _data_particle_record_buffer
            self.adjust_c_file_sample_times()

            for record in self._data_particle_record_buffer:
                self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

        except Exception as e:
            log.error(e)
            self._particle_data_handler.setParticleDataCaptureFailure()
            self._particle_data_handler._samples = {}

    def adjust_c_file_sample_times(self):
        """
        Set the time in the "c" samples (CTD, DOSTA) generated from the "C" data file
        from the time in the "e" samples (FLORT, PARAD) generated from the "E" data file
        when the pressures in both samples match to a tolerance of 0.02 dbar.
        @throws Exception if there are not any e_samples or if we could not find
        matching pressures in the c and e samples
        """
        if not self._e_file_time_pressure_tuples:
            warning_msg = "e_file time-pressure tuples list is empty, can not adjust timestamps"
            log.error(warning_msg)
            self._particle_data_handler.setProcessingInfo(ProcessingInfoKey.WARNING_MESSAGE, warning_msg)
            return

        # Trim the e profile by 10 percent so we can compare only pressures while the profiler is moving
        trim_number = int(len(self._e_file_time_pressure_tuples) * 0.10)
        if trim_number:
            e_profile = self._e_file_time_pressure_tuples[trim_number:-trim_number]
        else:
            e_profile = self._e_file_time_pressure_tuples

        pressure_increasing = e_profile[0][1] < e_profile[-1][1]

        e_samples_size = len(e_profile)
        curr_e_sample_index = -1
        curr_e_sample_time = None
        curr_e_sample_pressure = None
        prev_e_sample_time = None
        curr_e_sample_matched = False

        # These will get set below as we iterate the c and e samples
        c_sample_time_interval = None
        prev_c_sample_time = None

        # Create a buffer to temporarily hold the ctd samples until we find one
        # having the same pressure as the current flort sample
        c_samples_before_curr_e_sample = []

        # Counter for the number of c_samples since the last e_sample pressure match
        # This will equal len(c_samples_before_curr_e_sample) after the second pressure match
        # when c_sample_time_interval will have been determined and the buffer can be cleared.
        num_c_samples_between_e_samples = 0

        for c_sample in self._data_particle_record_buffer:

            # Place the c_sample in the temp buffer. We will Adjust the times of the samples in buffer later when
            # we find a pressure match between the c_sample and the e_sample
            c_samples_before_curr_e_sample.append(c_sample)
            num_c_samples_between_e_samples += 1

            # Get the pressure from the list of values (name-value pairs) in the sample
            c_sample_pressure = c_sample.get_value_from_values(
                self.pressure_containing_data_particle_field())

            # e_sample pressures are in dbar so convert the c_sample pressure from counts to dbar
            c_sample_pressure_dbar = self.pressure_conversion_function(c_sample_pressure)

            # For a descending profile (pressure increasing), get the next e sample
            # having pressure greater than or within tolerance of the c pressure.
            # For an ascending profile (pressure decreasing), get the next e sample
            # having pressure less than or within tolerance of the c pressure.
            while curr_e_sample_index < e_samples_size - 1 and\
                    (curr_e_sample_index == -1 or curr_e_sample_matched or
                     ((pressure_increasing
                       and (curr_e_sample_pressure < c_sample_pressure_dbar - self.pressure_tolerance))
                      or
                      (not pressure_increasing
                       and (curr_e_sample_pressure > c_sample_pressure_dbar + self.pressure_tolerance)))):
                # Get the next e_sample
                curr_e_sample_index += 1
                curr_e_sample_time = e_profile[curr_e_sample_index][0]
                curr_e_sample_pressure = e_profile[curr_e_sample_index][1]
                curr_e_sample_matched = False

            # If the pressures are the same in the curr_e_sample and curr_c_sample, calculate and back fill
            # the timestamps of the c_samples in the temp buffer
            if abs(curr_e_sample_pressure - c_sample_pressure_dbar) < self.pressure_tolerance:
                log.debug("Pressures of e_sample(%s) and [a|c]_sample(%s) match at %s" %
                         (str(curr_e_sample_pressure), str(c_sample_pressure_dbar), str(curr_e_sample_time)))
                # Record the match to ensure that we roll to the next e-sample in case the profiler got stuck
                # and pressure did not change. This ensures that a single c-sample will match a single e-sample.
                curr_e_sample_matched = True

                # If we have 2 e_sample times, we can calculate the time interval between c_samples
                # over that time range
                if prev_e_sample_time:
                    # Calculate the time interval between c_samples in the temporary c_sample buffer
                    c_sample_time_interval = (curr_e_sample_time - prev_e_sample_time) / num_c_samples_between_e_samples

                    # Initialize the prev_c_sample_time if it has not yet been found up to this point.
                    # This will be the first corrected c_sample time.
                    if not prev_c_sample_time:
                        prev_c_sample_time = curr_e_sample_time - (
                                c_sample_time_interval * len(c_samples_before_curr_e_sample))

                    # Back fill the times of the c_samples in the temp buffer and then clear the buffer
                    temp_counter = 0
                    for c_data_particle in c_samples_before_curr_e_sample:
                        temp_counter += 1
                        prev_c_sample_time += c_sample_time_interval
                        # Explicitly set the time of the last c_sample in the buffer to prevent rounding errors
                        # from the increment above from carrying forward beyond this e_sample time
                        if temp_counter == len(c_samples_before_curr_e_sample):
                            prev_c_sample_time = curr_e_sample_time
                        c_data_particle.set_value(DataParticleKey.INTERNAL_TIMESTAMP, prev_c_sample_time)
                    # Clear the temp c_sample buffer
                    c_samples_before_curr_e_sample[:] = []

                prev_e_sample_time = curr_e_sample_time
                num_c_samples_between_e_samples = 0

        # If we did not find a pressure match between a c and e sample, the last e_sample time
        # will not have been set, indicating that the c_sample times have not been adjusted.
        if not prev_e_sample_time:
            error_message = "Could not find a pressure match between an e_sample and [a|c]_sample to adjust timestamps"
            log.error(error_message)
            self._particle_data_handler.setProcessingInfo(ProcessingInfoKey.WARNING_MESSAGE, error_message)
            return

        # If the number of c_samples that have not yet been adjusted equals the total number of c_samples,
        # it is implied that only 1 match was found and we could not calculate the c_sample_time_interval
        # based on the time difference between 2 e sample matches. So calculate the interval from the first 2 c samples.
        # Use the time of the only match (prev_e_sample_time) to back-calculate the time before the first record
        # in the buffer to get ready for the roll in the next block below.
        if len(c_samples_before_curr_e_sample) == len(self._data_particle_record_buffer):
            if len(c_samples_before_curr_e_sample) > 1:
                c_sample_time_interval = \
                    c_samples_before_curr_e_sample[1].get_value(DataParticleKey.INTERNAL_TIMESTAMP) -\
                    c_samples_before_curr_e_sample[0].get_value(DataParticleKey.INTERNAL_TIMESTAMP)
            else:
                # There is only 1 c sample in the profile (should never happen but we handle it just in case)
                c_sample_time_interval = 0

            prev_c_sample_time = prev_e_sample_time - (
                    c_sample_time_interval * (len(c_samples_before_curr_e_sample) - num_c_samples_between_e_samples))

        # Roll the last c_sample time by the c_sample_time_interval and set the time on the remaining c_samples
        for c_sample in c_samples_before_curr_e_sample:
            prev_c_sample_time += c_sample_time_interval
            c_sample.set_value(DataParticleKey.INTERNAL_TIMESTAMP, prev_c_sample_time)

    def pressure_containing_data_particle_stream(self):
        """
        This method must be overridden. This method should return the name of the stream from which to
        get the time-pressure tuples.
        """
        raise NotImplementedException("pressure_containing_data_particle_stream() not overridden!")

    def pressure_containing_data_particle_field(self):
        """
        This method must be overridden. This method should return the name of the field in the stream from which to
        get the pressure for the time-pressure tuples.
        """
        raise NotImplementedException("pressure_containing_data_particle_field() not overridden!")


class WfpAFileDriver(WfpCFileDriver):
    """
    A non-sio a-file contains only 1 profile and there is exactly one corresponding e-file, also having only 1 profile.
    Use the time-pressure tuples from this e-file to correct the a-sample times by finding matching pressures
    in the a and e profiles. Inherit from WfpCFileDriver since the logic to correct the a samples and c samples is the
    same. The only differences are the pressure_conversion_function and pressure_tolerance which are overridden below.
    """
    def __init__(self, unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples):
        super(WfpAFileDriver, self).__init__(unused, stream_handle, particle_data_handler, e_file_time_pressure_tuples)

        self.pressure_tolerance = 0.02
        self.pressure_conversion_function = self.convert_to_dbar

    @staticmethod
    def convert_to_dbar(pressure):
        """
        Convert from pressure in 0.001dbar to dbar
        :param pressure: pressure in 0.001dbar
        :return: pressure in dbar
        """
        return float(pressure)/1000


class WfpSioCFileDriver(SimpleDatasetDriver):
    """
    A sio c-file can contain multiple profiles and there can be more than one e-file
    also having multiple profiles for the corresponding time ranges. Unlike the non-sio driver above,
    the pressures in the corresponding e and c profiles may not match and therefor an interpolation
    model is needed to determine the correct time from pressure.

    The instantiator of this driver should call the following functions in order:
        processFileStream()
        while e_profiles_are_missing():
            add_possible_e_profiles(e_time_pressure_list)
        adjust_c_file_sample_times()
        populate_particle_data_handler()
    """
    def __init__(self, unused, stream_handle, particle_data_handler):
        self._data_particle_record_buffer = []
        self._c_file_profiles = []
        self._e_file_profiles = []
        self._missing_e_profile_indexes = []

        super(WfpSioCFileDriver, self).__init__(unused, stream_handle, particle_data_handler)

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
                    if record.data_particle_type() == self.pressure_containing_data_particle_stream():
                        self._data_particle_record_buffer.append(record)
                    else:
                        self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

            self._c_file_profiles = self.get_c_file_profiles(self._data_particle_record_buffer)
            self._missing_e_profile_indexes = range(len(self._c_file_profiles))

        except Exception as e:
            log.error(e)
            self._particle_data_handler.setParticleDataCaptureFailure()
            self._particle_data_handler._samples = {}

    def populate_particle_data_handler(self):
        for record in self._data_particle_record_buffer:
            self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())

    def adjust_c_file_sample_times(self):
        """
        Set the time in the "c" samples (CTD, DOSTA) generated from the "C" data file
        from the time in the "e" samples (FLORT, PARAD) generated from the "E" data file
        when the pressures in both samples match
        @throws Exception if there are not any e_samples or if we could not find
        matching pressures in the c and e samples
        """
        for i in range(len(self._c_file_profiles)):
            c_profile = self._c_file_profiles[i]

            c_profile_mid_point_index = int(len(c_profile) / 2)
            c_profile_mid_point_time = c_profile[c_profile_mid_point_index].get_value(
                DataParticleKey.INTERNAL_TIMESTAMP)

            e_profile = self.get_e_profile_containing_time(c_profile_mid_point_time, self._e_file_profiles)

            if not e_profile:
                log.error("Could not find e_profile containing time %s" % ntp_to_string(c_profile_mid_point_time))
                continue

            self.adjust_times_in_c_profile(c_profile, e_profile)

    def adjust_times_in_c_profile(self, c_profile, e_profile):
        """
        Use the e-samples to generate a interpolation model of time vs pressure.
        Set the time in the "c" samples by interpolation c-time from c-pressure using this model.
        """
        # trim each end of e profile by 10 percent to ensure that values before and after the actual time
        # of WFP movement are discarded so they don't corrupt the pressure-time linear interpolation model.
        trim_number = int(len(e_profile) * 0.10)

        # make sure samples are at least 1 min apart to ensure that pressure is unique in the model profile
        num_samples = (e_profile[-trim_number][0] - e_profile[trim_number][0])/60
        step = int(max(1, (len(e_profile)-(2*trim_number))/num_samples))

        e_profile_trimmed = e_profile[trim_number:-trim_number:step]

        # log.info('Trimmed E file time-pressure tuples:')
        # for e_file_tuple in e_profile_trimmed:
        #     log.info('Time: %s, Pressure: %s' % (ntp_to_string(e_file_tuple[0]), str(e_file_tuple[1])))

        e_times = [x[0] for x in e_profile_trimmed]
        e_press = [x[1] for x in e_profile_trimmed]

        # Note: we do not use arg fill_value="extrapolate" so the function will throw a
        # value error if pressure is not within the pressure range of e_press
        interpolate_time_from_pressure = interpolate.interp1d(e_press, e_times, kind='linear', axis=0, copy=False)

        c_index_within_model_begin = None
        c_index_within_model_end = None

        for i in range(len(c_profile)):

            c_sample = c_profile[i]

            # Get the pressure from the list of values (name-value pairs) in the sample
            c_sample_pressure = c_sample.get_value_from_values(
                self.pressure_containing_data_particle_field())

            # e_sample pressures are in dbar so convert the c_sample pressure from counts to dbar
            c_sample_pressure_dbar = ctd_sbe52mp_preswat(c_sample_pressure)

            # Wire following profilers can either descend (increasing pressure) or ascend (decreasing pressure)
            # so we need to test for both cases to determine if the current sample is before, after or within
            # the pressure range of the interpolation model.
            if (c_sample_pressure_dbar < e_press[0] < e_press[1]) \
                    or (c_sample_pressure_dbar > e_press[0] > e_press[1]):
                # The current sample is before the pressure range of the interpolation model so pass here.
                # We will backfill these samples later once we have determined the avg_c_sample_time_interval
                continue
            elif (c_sample_pressure_dbar > e_press[-1] > e_press[-2]) \
                    or (c_sample_pressure_dbar < e_press[-1] < e_press[-2]):
                # The current sample is after the pressure range of the interpolation model
                # We will backfill these samples later once we have determined the avg_c_sample_time_interval
                break
            else:
                # The current sample is within the pressure range of the interpolation model
                curr_c_sample_time = interpolate_time_from_pressure(c_sample_pressure_dbar).item(0)
                c_sample.set_value(DataParticleKey.INTERNAL_TIMESTAMP, curr_c_sample_time)

                if c_index_within_model_begin is None:
                    c_index_within_model_begin = i
                c_index_within_model_end = i

        if c_index_within_model_begin is None:
            error_message = "Could not find a match between the e_samples and c_samples to adjust c_sample times"
            log.error(error_message)
            return

        if c_index_within_model_end - c_index_within_model_begin == 0 and len(c_profile) > 1:
            error_message = "Cannot determine the time interval between c samples"
            log.error(error_message)
            return

        # Calculate the average time interval between c samples within the interpolation model
        # so we can calculate the c times that are either before or after the interpolation model
        c_time_within_model_begin = c_profile[c_index_within_model_begin].get_value(DataParticleKey.INTERNAL_TIMESTAMP)
        c_time_within_model_end = c_profile[c_index_within_model_end].get_value(DataParticleKey.INTERNAL_TIMESTAMP)
        avg_c_sample_time_interval = (c_time_within_model_end - c_time_within_model_begin) / \
                                     (c_index_within_model_end - c_index_within_model_begin)

        log.info('Interpolation model ranges: '
                 'e_indexes(%d, %d), e_times(%s, %s), e_press(%s, %s), e_samples: %d of %d, '
                 'c_indexes(%d, %d), c_times(%s, %s), c_samples: %d of %d, avg_c_sample_time_interval(%s)' %
                 (trim_number, len(e_profile)-trim_number-1,
                  str(e_times[0]), str(e_times[-1]),
                  str(e_press[0]), str(e_press[-1]),
                  len(e_times), len(e_profile),
                  c_index_within_model_begin, c_index_within_model_end,
                  str(c_time_within_model_begin), str(c_time_within_model_end),
                  c_index_within_model_end-c_index_within_model_begin+1, len(c_profile),
                  str(avg_c_sample_time_interval)))

        # Adjust c sample times that came before the time range of the interpolation model
        backfill_c_sample_time = c_time_within_model_begin
        for i in reversed(range(c_index_within_model_begin)):
            backfill_c_sample_time -= avg_c_sample_time_interval
            c_profile[i].set_value(DataParticleKey.INTERNAL_TIMESTAMP, backfill_c_sample_time)

        # Adjust c sample times that came after the time range of the interpolation model
        backfill_c_sample_time = c_time_within_model_end
        for i in range(c_index_within_model_end+1, len(c_profile), 1):
            backfill_c_sample_time += avg_c_sample_time_interval
            c_profile[i].set_value(DataParticleKey.INTERNAL_TIMESTAMP, backfill_c_sample_time)

    def pressure_containing_data_particle_stream(self):
        """
        This method must be overridden. This method should return the name of the stream from which to
        get the time-pressure tuples.
        """
        raise NotImplementedException("pressure_containing_data_particle_stream() not overridden!")

    def pressure_containing_data_particle_field(self):
        """
        This method must be overridden. This method should return the name of the field in the stream from which to
        get the pressure for the time-pressure tuples.
        """
        raise NotImplementedException("pressure_containing_data_particle_field() not overridden!")

    @staticmethod
    def get_e_file_profiles(time_pressure_list):
        return WfpSioCFileDriver.get_profiles(time_pressure_list,
                                              lambda x: x[0])

    @staticmethod
    def get_c_file_profiles(time_pressure_list):
        return WfpSioCFileDriver.get_profiles(time_pressure_list,
                                              lambda x: x.get_value(DataParticleKey.INTERNAL_TIMESTAMP))

    @staticmethod
    def get_profiles(time_pressure_list, time_getter_fn):
        """
        The file can contain multiple profiles.
        The time interval between samples in single profile is very consistent (difference < 0.0001 sec).
        The time interval between the last sample in a profile and the first sample in the next profile
        is usually very large (> 20 hours), so if the interval is greater than a chosen threshold,
        assume that the interval is between particles in different profiles.
        :return: a list of lists of particles in each profile
        """
        # Consider particles to be from different profiles if the time between them exceeds this many seconds
        time_between_profiles_threshold = 3600

        profiles = []

        for x in time_pressure_list:
            if len(profiles) == 0 or \
                    time_getter_fn(x) - time_getter_fn(profiles[-1][-1]) > time_between_profiles_threshold:
                profiles.append([])
            profiles[-1].append(x)

        return profiles

    @staticmethod
    def get_e_profile_containing_time(c_profile_mid_point_time, e_profiles):
        """
        Find the e_profile containing the mid point time of the c_profile
        :param c_profile_mid_point_time:
        :param e_profiles:
        :return: e_profile containing the same time range as the c_profile
        """
        for i in range(len(e_profiles)):
            e_profile = e_profiles[i]
            if e_profile[0][0] <= c_profile_mid_point_time <= e_profile[-1][0]:
                return e_profile

    def get_mid_time_of_c_profile(self, c_profile):
        c_profile_mid_point_index = int(len(c_profile) / 2)
        return c_profile[c_profile_mid_point_index].get_value(DataParticleKey.INTERNAL_TIMESTAMP)

    def add_possible_e_profiles(self, time_pressure_list):
        ret_val = False
        found_e_profile_indexes = []
        e_profiles = WfpSioCFileDriver.get_e_file_profiles(time_pressure_list)
        for missing_e_profile_index in self._missing_e_profile_indexes:
            c_profile_mid_point_time = self.get_mid_time_of_c_profile(self._c_file_profiles[missing_e_profile_index])
            e_profile = WfpSioCFileDriver.get_e_profile_containing_time(c_profile_mid_point_time, e_profiles)
            if e_profile:
                self._e_file_profiles.append(e_profile)
                found_e_profile_indexes.append(missing_e_profile_index)
                ret_val = True

        for found_e_profile_index in found_e_profile_indexes:
            self._missing_e_profile_indexes.remove(found_e_profile_index)

        return ret_val

    def e_profiles_are_missing(self):
        return len(self._missing_e_profile_indexes) > 0

    def get_missing_e_profile_time_ranges(self):
        missing_time_ranges = []
        for i in self._missing_e_profile_indexes:
            c_profile = self._c_file_profiles[i]
            missing_time_ranges.append((
                c_profile[0].get_value(DataParticleKey.INTERNAL_TIMESTAMP),
                c_profile[-1].get_value(DataParticleKey.INTERNAL_TIMESTAMP)))
        return missing_time_ranges
