import calendar
import datetime
import ntplib

from mi.core.log import get_logger
from utilities import dcl_time_to_ntp
from mi.instrument.satlantic.suna_deep.ooicore.driver import SUNASampleDataParticleKey
from mi.dataset.dataset_parser import Parser
from mi.core.instrument.dataset_data_particle import \
    DataParticle, \
    DataParticleKey

log = get_logger()


class SunaCommon(DataParticle):

    def __init__(self, raw_data, *args, **kwargs):
        super(SunaCommon, self).__init__(raw_data, *args, **kwargs)

        self.raw_data = raw_data

    def _encode_spectral_channels_values(self, name, value_list, encoding_function):
        """
        Encode a value using the encoding function, if it fails store the error in a queue
        This function is to encode the list of spectral channels
        """
        encoded_val = None

        try:
            encoded_val = [encoding_function(s) for s in value_list]
        except Exception as e:
            log.error("Data particle error encoding. Name:%s Value:%s", name, value_list)
            self._encoding_errors.append({name: value_list})
        return {DataParticleKey.VALUE_ID: name,
                DataParticleKey.VALUE: encoded_val}

    def _build_parsed_values(self):
        data_list = []
        spectral_channels_list = []

        instrument_map = [
            (SUNASampleDataParticleKey.FRAME_TYPE,          str),
            (SUNASampleDataParticleKey.SERIAL_NUM,          str),
            (SUNASampleDataParticleKey.SAMPLE_DATE,         int),
            (SUNASampleDataParticleKey.SAMPLE_TIME,         float),
            (SUNASampleDataParticleKey.NITRATE_CONCEN,      float),
            (SUNASampleDataParticleKey.NITROGEN,            float),
            (SUNASampleDataParticleKey.ABSORB_254,          float),
            (SUNASampleDataParticleKey.ABSORB_350,          float),
            (SUNASampleDataParticleKey.BROMIDE_TRACE,       float),
            (SUNASampleDataParticleKey.SPECTRUM_AVE,        int),
            (SUNASampleDataParticleKey.FIT_DARK_VALUE,      int),
            (SUNASampleDataParticleKey.TIME_FACTOR,         int),
            (SUNASampleDataParticleKey.SPECTRAL_CHANNELS,   int),  # x256
            (SUNASampleDataParticleKey.TEMP_SPECTROMETER,   float),
            (SUNASampleDataParticleKey.TEMP_INTERIOR,       float),
            (SUNASampleDataParticleKey.TEMP_LAMP,           float),
            (SUNASampleDataParticleKey.LAMP_TIME,           int),
            (SUNASampleDataParticleKey.HUMIDITY,            float),
            (SUNASampleDataParticleKey.VOLTAGE_MAIN,        float),
            (SUNASampleDataParticleKey.VOLTAGE_LAMP,        float),
            (SUNASampleDataParticleKey.VOLTAGE_INT,         float),
            (SUNASampleDataParticleKey.CURRENT_MAIN,        float),
            (SUNASampleDataParticleKey.FIT_1,               float),
            (SUNASampleDataParticleKey.FIT_2,               float),
            (SUNASampleDataParticleKey.FIT_BASE_1,          float),
            (SUNASampleDataParticleKey.FIT_BASE_2,          float),
            (SUNASampleDataParticleKey.FIT_RMSE,            float),
            (SUNASampleDataParticleKey.CHECKSUM,            int)
        ]

        # spectral channels start at index 12 and end at index 256
        spectral_channel_index = 12
        # index 268
        temp_spec_index = spectral_channel_index + 256

        # will keep track of index, but position will keep track of where it will be mapped (according to the order of
        # the data layout
        position = 0
        for i, item in enumerate(self.raw_data):

            # Once at the end of the spectral channel, append it to the data_list
            if i == temp_spec_index:
                data_list.append(self._encode_spectral_channels_values(instrument_map[spectral_channel_index][0],
                                                                       spectral_channels_list,
                                                                       instrument_map[spectral_channel_index][1]))
                position += 1

            # Handle spectral channels by adding them to a list
            if spectral_channel_index <= i < temp_spec_index:
                spectral_channels_list.append(item)

            # Handle all other pieces of data that are not the spectral channels
            # CTDs are empty
            elif item:
                data_list.append(self._encode_value(instrument_map[position][0], item, instrument_map[position][1]))
                position += 1

        return data_list


class SunaDclRecoveredParticle(SunaCommon):
    def __init__(self, raw_data, *args, **kwargs):
        super(SunaDclRecoveredParticle, self).__init__(raw_data, *args, **kwargs)
        self.raw_data = raw_data


class SunaDclRecoveredDataParticle(SunaDclRecoveredParticle):
    _data_particle_type = 'suna_dcl_recovered'


class SunaInstrumentRecoveredParticle(SunaCommon):

    def __init__(self, raw_data, *args, **kwargs):
        super(SunaInstrumentRecoveredParticle, self).__init__(raw_data, *args, **kwargs)

        self.raw_data = raw_data


class SunaInstrumentRecoveredDataParticle(SunaInstrumentRecoveredParticle):
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
        self._raw_data_length = 286

    def parse_file(self):
        for line in self._stream_handle:

            # DCL/Telemetered
            if not line.startswith('SATSLF') and 'SATSLF' in line or 'SATNDF' in line:
                raw_data = line.split(',')
                if len(raw_data) != self._raw_data_length:
                    continue

                particle_class = SunaDclRecoveredDataParticle

                # Split first element. Format is "<date> <time> SAT(SLF|NDF)". Take timestamp, and
                # Date and time are joined and to be processed into port_timestamp
                # Date and time will be removed from raw_data. raw_data will start with SAT(SLF|NDF)
                timestamp = raw_data[0].split(' ', 2)
                raw_data[0] = timestamp[2]
                port_timestamp = dcl_time_to_ntp(' '.join(timestamp[0:2]))

                raw_data.insert(1, raw_data[0][3:6])
                raw_data[0] = raw_data[0][0:3]
                particle = self._extract_sample(particle_class, None, raw_data,
                                                port_timestamp=port_timestamp,
                                                preferred_ts=DataParticleKey.PORT_TIMESTAMP)

                self._record_buffer.append(particle)

            elif line.startswith('SATSLF') or line.startswith('SATSDF'):
                raw_data = line.split(',')
                if len(raw_data) != self._raw_data_length:
                    continue

                particle_class = SunaInstrumentRecoveredDataParticle
                raw_data.insert(1, raw_data[0][3:6])
                raw_data[0] = raw_data[0][0:3]

                particle = self._extract_sample(particle_class, None, raw_data)
                self._record_buffer.append(particle)

    def get_records(self, num_records_requested=1):
        """
        Returns a list of particles that is  equal to the num_records_requested when there are that many particles
        are available or a list of particles less than the num_records_requested when there are fewer than
        num_records_requested available.
        """
        particles_to_return = []

        if num_records_requested:

            # If the file was not read, let's parse it
            if not self._file_parsed:
                self.parse_file()

            # Iterate through the particles returned, and pop them off from the beginning of the record
            # buffer to the end
            while len(particles_to_return) is not num_records_requested and self._record_buffer:
                particles_to_return.append(self._record_buffer.pop(0))

        return particles_to_return
