import calendar
import datetime
import ntplib

from mi.instrument.satlantic.suna_deep.ooicore.driver import SUNASampleDataParticle, SUNASampleDataParticleKey
from mi.dataset.dataset_parser import Parser
from mi.core.common import BaseEnum
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey

GROUP_FRAME_TYPE = 1
GROUP_SERIAL_NUM = 2
GROUP_SAMPLE_DATE = 3
GROUP_SAMPLE_TIME = 4
GROUP_NITRATE_CONCEN = 5
GROUP_NITROGEN = 6
GROUP_ABSORB_254 = 7
GROUP_ABSORB_350 = 8
GROUP_BROMIDE_TRACE = 9
GROUP_SPECTRUM_AVE = 10
GROUP_FIT_DARK_VALUE = 11
GROUP_TIME_FACTOR = 12
GROUP_SPECTRAL_CHANNELS = 13
GROUP_TEMP_SPECTROMETER = 14
GROUP_TEMP_INTERIOR = 15
GROUP_TEMP_LAMP = 16
GROUP_LAMP_TIME = 17
GROUP_HUMIDITY = 18
GROUP_VOLTAGE_MAIN = 19
GROUP_VOLTAGE_LAMP = 20
GROUP_VOLTAGE_INT = 21
GROUP_CURRENT_MAIN = 22
GROUP_FIT_1 = 23
GROUP_FIT_2 = 24
GROUP_FIT_BASE_1 = 25
GROUP_FIT_BASE_2 = 26
GROUP_FIT_RMSE = 27
GROUP_CHECKSUM = 28


class SUNASampleDataParticleKey(BaseEnum):
    FRAME_TYPE = "frame_type"
    SERIAL_NUM = "serial_number"
    SAMPLE_DATE = "date_of_sample"
    SAMPLE_TIME = "time_of_sample"
    NITRATE_CONCEN = "nitrate_concentration"
    NITROGEN = "nutnr_nitrogen_in_nitrate"
    ABSORB_254 = "nutnr_absorbance_at_254_nm"
    ABSORB_350 = "nutnr_absorbance_at_350_nm"
    BROMIDE_TRACE = "nutnr_bromide_trace"
    SPECTRUM_AVE = "nutnr_spectrum_average"
    FIT_DARK_VALUE = "nutnr_dark_value_used_for_fit"
    TIME_FACTOR = "nutnr_integration_time_factor"
    SPECTRAL_CHANNELS = "spectral_channels"
    TEMP_SPECTROMETER = "temp_spectrometer"
    TEMP_INTERIOR = "temp_interior"
    TEMP_LAMP = "temp_lamp"
    LAMP_TIME = "lamp_time"
    HUMIDITY = "humidity"
    VOLTAGE_MAIN = "voltage_main"
    VOLTAGE_LAMP = "voltage_lamp"
    VOLTAGE_INT = "nutnr_voltage_int"
    CURRENT_MAIN = "nutnr_current_main"
    FIT_1 = "aux_fitting_1"
    FIT_2 = "aux_fitting_2"
    FIT_BASE_1 = "nutnr_fit_base_1"
    FIT_BASE_2 = "nutnr_fit_base_2"
    FIT_RMSE = "nutnr_fit_rmse"
    CHECKSUM = "checksum"


class SunaCommon(DataParticle):

    def __init__(self, raw_data, *args, **kwargs):
        super(SunaCommon, self).__init__(raw_data, *args, **kwargs)


class SunaDclRecoveredParticle(DataParticle):

    def __init__(self, raw_data, *args, **kwargs):
        super(SunaDclRecoveredParticle, self).__init__(raw_data, *args, **kwargs)

        self.raw_data = raw_data

    def _build_parsed_values(self):
        suna_sample = SUNASampleDataParticle(self.raw_data)
        return suna_sample._build_parsed_values()


class SunaDclRecoveredDataParticle(SunaDclRecoveredParticle):
    _data_particle_type = 'suna_dcl_recovered'


class SunaInstrumentRecoveredParticle(DataParticle):

    def __init__(self, raw_data, *args, **kwargs):
        super(SunaInstrumentRecoveredParticle, self).__init__(raw_data, *args, **kwargs)

        self.raw_data = raw_data


class SUnaInstrumentRecoveredDataParticle(SunaInstrumentRecoveredParticle):

    _data_particle_type = 'suna_instrument_recovered'


class SunaParser(Parser):

    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):

        super(SunaParser, self).__init__(config,
                                         stream_handle,
                                         None,  # State not used
                                         None,  # Sieve function not used
                                         None,  # state callback not used
                                         None,  # publish callback not used
                                         exception_callback)

        self._file_parsed = False
        self._record_buffer = []

    @staticmethod
    def _date_time_sample_values_to_ntp_timestamp(date_sample_str, time_sample_str):

        year = int(date_sample_str[0:4])
        days = int(date_sample_str[4:7])

        hours_float = float(time_sample_str)

        date_time_val = datetime.datetime(year, 1, 1) + datetime.timedelta(days=days-1, hours=hours_float)
        ntp_timestamp = ntplib.system_to_ntp_time(calendar.timegm(date_time_val.timetuple()))

        return ntp_timestamp

    def parse_file(self):
        particle_class = SunaDclRecoveredDataParticle

        for line in self._stream_handle:
            # DCL/Telemetered
            if not line.startswith('SATSLF') and 'SATSLF' in line:
                # Get date and time at the beginning of the line
                date_time = line.split(',')[0].split()[:2]

                # Get rid of the date and time at the beginning of the line. raw_data will start with SATSLF...
                raw_data = line.split(' ', 2)[2]

                timestamp = self._date_time_sample_values_to_ntp_timestamp(
                    raw_data.split(',')[1],
                    raw_data.split(',')[2],
                )

                particle = self._extract_sample(particle_class, None, raw_data,
                                                internal_timestamp=timestamp)

                self._record_buffer.append(particle)

            elif line.startswith('SATSLF'):
                particle_class = SUnaInstrumentRecoveredDataParticle
                raw_data = line

                particle = self._extract_sample(particle_class, None, raw_data)
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
                self.parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) < num_records_requested and len(self._record_buffer) > 0:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return