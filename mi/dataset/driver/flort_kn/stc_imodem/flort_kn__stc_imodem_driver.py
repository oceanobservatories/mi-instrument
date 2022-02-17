from mi.dataset.parser.flort_kn__stc_imodem import \
    DataParticleType, Flort_kn_stc_imodemParser, Flort_kn_stc_imodemParserDataParticle, Flort_kn__stc_imodemParserDataParticleKey
from mi.dataset.driver.wfp_common.wfp_e_file_driver import WfpEFileDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version

from mi.core.log import get_logger

log = get_logger()


class FlortKnStcImodemDriver(WfpEFileDriver):

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

    def pressure_containing_data_particle_stream(self):
        return DataParticleType.FLORT_KN_INSTRUMENT

    def pressure_containing_data_particle_field(self):
        return Flort_kn__stc_imodemParserDataParticleKey.PRESSURE_DEPTH


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    with open(source_file_path, 'rb') as stream_handle:

        # create and instance of the concrete driver class defined below
        driver = FlortKnStcImodemDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
