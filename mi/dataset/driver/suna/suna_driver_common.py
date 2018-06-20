from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.suna import SunaParser
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.suna'
RECOVERED_PARTICLE_CLASS = 'SunaDclRecoveredParticle'
INSTRUMENT_RECOVERED_PARTICLE_CLASS = 'SunaInstrumentRecoveredParticle'


class SunaDriver(SimpleDatasetDriver):
    """
    Derived presf_abc_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """
    def __init__(self, class_type, unused, stream_handle, particle_data_handler):

        self._class_type = class_type

        super(SunaDriver, self).__init__(unused, stream_handle, particle_data_handler)

    def _build_parser(self, stream_handle):
        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: self._class_type}

        parser = SunaParser(parser_config,
                            stream_handle,
                            self._exception_callback)

        return parser
