from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.suna import SunaParser
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.driver.suna.suna_driver_common import SunaDriver, INSTRUMENT_RECOVERED_PARTICLE_CLASS
from mi.core.versioning import version


@version("0.0.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rU') as stream_handle:

        # Create an instance of the concrete driver class defined below.
        driver = SunaDriver(INSTRUMENT_RECOVERED_PARTICLE_CLASS, unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler
