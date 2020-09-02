from mi.dataset.parser.flort_kn__stc_imodem import \
    DataParticleType, Flort_kn_stc_imodemParser, Flort_kn_stc_imodemParserDataParticle, Flort_kn__stc_imodemParserDataParticleKey
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

from mi.core.log import get_logger

log = get_logger()


class FlortKnStcImodemDriver(SimpleDatasetDriver):

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.flort_kn__stc_imodem",
            DataSetDriverConfigKeys.PARTICLE_CLASS: "Flort_kn_stc_imodemParserDataParticle"
        }

        parser = Flort_kn_stc_imodemParser(
            parser_config,
            None,
            stream_handle,
            lambda state, f: None,
            lambda state: None)

        return parser

    def get_time_pressure_tuples(self):
        """
        Get a list of (time, pressure) tuples. This is intended to be used to adjust the
        internal timestamps of te "c" file particles.
        :return: a list of (time, pressure) tuples
        """
        time_pressure_tuples = []
        while True:
            try:
                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    if record.data_particle_type() == DataParticleType.FLORT_KN_INSTRUMENT:
                        time_pressure_tuples.append((
                            record.get_value(DataParticleKey.INTERNAL_TIMESTAMP),
                            record.get_value_from_values(Flort_kn__stc_imodemParserDataParticleKey.PRESSURE_DEPTH)))
            except Exception as e:
                log.error(e)
                return None
        return time_pressure_tuples


@version("0.0.1")
def parse(unused, source_file_path, particle_data_handler):
    with open(source_file_path, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = FlortKnStcImodemDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
