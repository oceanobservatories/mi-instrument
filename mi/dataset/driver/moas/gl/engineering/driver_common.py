##
# OOIPLACEHOLDER
#
##

__author__= "ehahn"

from mi.core.log import get_logger
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.glider import GliderEngineeringParser


class GliderEngineeringDriver:

    def __init__(self, source_file_path, particle_data_handler, parser_config):
        """
        Initialize glider engineering driver
        @param source_file_path - source file from Java
        @param particle_data_handler - particle data handler object from Java
        @param parser_config - parser configuration dictionary
        """

        self._source_file_path = source_file_path
        self._particle_data_handler = particle_data_handler
        self._parser_config = parser_config

    def process(self):
        """
        Process a file by opening the file and instantiating a parser and driver
        """
        log = get_logger()

        with open(self._source_file_path, "rb") as file_handle:
            def exception_callback(exception):
                log.debug("Exception %s", exception)
                self._particle_data_handler.setParticleDataCaptureFailure()

            # essentially comment out the state and data callbacks by inserting
            # lambda with None functions, so it doesn't complain about not being
            # able to pass arguments
            parser = GliderEngineeringParser(self._parser_config,
                                             file_handle,
                                             exception_callback)

            # instantiate the driver
            driver = DataSetDriver(parser, self._particle_data_handler)
            # start the driver processing the file
            driver.processFileStream()

        return self._particle_data_handler

