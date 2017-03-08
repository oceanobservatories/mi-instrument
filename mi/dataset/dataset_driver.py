import os

from mi.logging import config
from mi.core.log import get_logger
from mi.core.exceptions import NotImplementedException


__author__ = 'wordenm'
log = get_logger()


class ParticleDataHandler(object):
    def __init__(self):
        self._samples = {}
        self._failure = False

    def addParticleSample(self, sample_type, sample):
        log.debug("Sample type: %s, Sample data: %s", sample_type, sample)
        self._samples.setdefault(sample_type, []).append(sample)

    def setParticleDataCaptureFailure(self):
        log.debug("Particle data capture failed")
        self._failure = True


class DataSetDriver(object):
    """
    Base Class for dataset drivers used within uFrame
    This class of objects processFileStream method
    will be used by the parse method
    which is called directly from uFrame
    """

    def __init__(self, parser, particle_data_handler):

        self._parser = parser
        self._particle_data_handler = particle_data_handler

    def processFileStream(self):
        """
        Method to extract records from a parser's get_records method
        and pass them to the Java particle_data_handler passed in from uFrame
        """
        while True:
            try:
                records = self._parser.get_records(1)

                if len(records) == 0:
                    log.debug("Done retrieving records.")
                    break

                for record in records:
                    self._particle_data_handler.addParticleSample(record.data_particle_type(), record.generate())
            except Exception as e:
                log.error(e)
                self._particle_data_handler.setParticleDataCaptureFailure()
                break


class SimpleDatasetDriver(DataSetDriver):
    """
    Abstract class to simplify driver writing.  Derived classes simply need to provide
    the _build_parser method
    """

    def __init__(self, unused, stream_handle, particle_data_handler):
        parser = self._build_parser(stream_handle)

        super(SimpleDatasetDriver, self).__init__(parser, particle_data_handler)

    def _build_parser(self, stream_handle):
        """
        abstract method that must be provided by derived classes to build a parser
        :param stream_handle: an open fid created from the source_file_path passed in from edex
        :return: A properly configured parser object
        """

        raise NotImplementedException("_build_parser must be implemented")

    def _exception_callback(self, exception):
        """
        A common exception callback method that can be used by _build_parser methods to
        map any exceptions coming from the parser back to the edex particle_data_handler
        :param exception: any exception from the parser
        :return: None
        """

        log.debug("ERROR: %r", exception)
        self._particle_data_handler.setParticleDataCaptureFailure()

