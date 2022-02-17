
__author__ = "msteiner"

from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.core.exceptions import NotImplementedException

from mi.core.log import get_logger

log = get_logger()


class WfpEFileDriver(SimpleDatasetDriver):

    def get_time_pressure_tuples(self):
        """
        Get a list of (time, pressure) tuples. This is intended to be used to adjust the
        internal timestamps of the "c" file particles.
        :return: a list of (time, pressure) tuples
        """
        time_pressure_tuples = []
        accept_samples = False
        pressure_precision = 0.001
        while True:
            try:
                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    if record.data_particle_type() == self.pressure_containing_data_particle_stream():
                        time_pressure_tuple = (
                            record.get_value(DataParticleKey.INTERNAL_TIMESTAMP),
                            record.get_value_from_values(self.pressure_containing_data_particle_field()))

                        # Consider pressures as invalid until the first non-zero pressure is encountered
                        if not accept_samples and time_pressure_tuple[1] > pressure_precision:
                            accept_samples = True

                        if accept_samples:
                            time_pressure_tuples.append(time_pressure_tuple)
            except Exception as e:
                log.error(e)
                return None
        return time_pressure_tuples

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
