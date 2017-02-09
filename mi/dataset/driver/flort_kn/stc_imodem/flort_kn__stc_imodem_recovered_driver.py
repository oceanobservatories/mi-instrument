from mi.dataset.parser.flort_kn__stc_imodem import Flort_kn_stc_imodemParser,Flort_kn_stc_imodemParserDataParticleRecovered
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.core.versioning import version


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    with open(source_file_path,"r") as fil :
        parser = Flort_kn_stc_imodemParser({
            DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.flort_kn__stc_imodem",
            DataSetDriverConfigKeys.PARTICLE_CLASS: "Flort_kn_stc_imodemParserDataParticleRecovered"},
            None,
            fil,
            lambda state, f: None,
            lambda state: None)
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()
    return particle_data_handler
