from mi.dataset.parser.suna import SunaParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

MODULE_NAME = 'mi.dataset.parser.suna'
RECOVERED_PARTICLE_CLASS = 'SunaDclRecoveredParticle'
INSTRUMENT_RECOVERED_PARTICLE_CLASS = 'SunaInstrumentRecoveredParticle'


def process(source_file_path, particle_data_handler, particle_class):

    with open(source_file_path, "r") as stream_handle:
        parser = SunaParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            stream_handle,
            lambda ex: particle_data_handler.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_handler)
        driver.processFileStream()
