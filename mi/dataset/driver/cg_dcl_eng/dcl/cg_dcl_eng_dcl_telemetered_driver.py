#!/usr/bin/env python

"""
@package mi.dataset.driver.cg_dcl_eng.dcl
@file mi-dataset/mi/dataset/driver/cg_dcl_eng/dcl/cg_dcl_eng_dcl_telemetered_driver.py
@author Mark Worden
@brief Driver for the cg_dcl_eng_dcl instrument

Release notes:

Initial Release
"""

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.cg_dcl_eng_dcl import CgDclEngDclParser, ParticleClassTypes, \
    CgDclEngDclMsgCountsTelemeteredDataParticle, \
    CgDclEngDclCpuUptimeTelemeteredDataParticle, \
    CgDclEngDclErrorTelemeteredDataParticle, \
    CgDclEngDclGpsTelemeteredDataParticle, \
    CgDclEngDclPpsTelemeteredDataParticle, \
    CgDclEngDclSupervTelemeteredDataParticle, \
    CgDclEngDclDlogMgrTelemeteredDataParticle, \
    CgDclEngDclDlogStatusTelemeteredDataParticle, \
    CgDclEngDclStatusTelemeteredDataParticle, \
    CgDclEngDclDlogAarmTelemeteredDataParticle
from mi.core.versioning import version


@version("15.6.1")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    with open(source_file_path, 'rb') as stream_handle:

        driver = CgDclEngDclTelemeteredDriver(unused, stream_handle, particle_data_handler)
        driver.processFileStream()

    return particle_data_handler


class CgDclEngDclTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived flntu_x_mmp_cds driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.cg_dcl_eng_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: None,
            DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                ParticleClassTypes.MSG_COUNTS_PARTICLE_CLASS:
                    CgDclEngDclMsgCountsTelemeteredDataParticle,
                ParticleClassTypes.CPU_UPTIME_PARTICLE_CLASS:
                    CgDclEngDclCpuUptimeTelemeteredDataParticle,
                ParticleClassTypes.ERROR_PARTICLE_CLASS:
                    CgDclEngDclErrorTelemeteredDataParticle,
                ParticleClassTypes.GPS_PARTICLE_CLASS:
                    CgDclEngDclGpsTelemeteredDataParticle,
                ParticleClassTypes.PPS_PARTICLE_CLASS:
                    CgDclEngDclPpsTelemeteredDataParticle,
                ParticleClassTypes.SUPERV_PARTICLE_CLASS:
                    CgDclEngDclSupervTelemeteredDataParticle,
                ParticleClassTypes.DLOG_MGR_PARTICLE_CLASS:
                    CgDclEngDclDlogMgrTelemeteredDataParticle,
                ParticleClassTypes.DLOG_STATUS_PARTICLE_CLASS:
                    CgDclEngDclDlogStatusTelemeteredDataParticle,
                ParticleClassTypes.STATUS_PARTICLE_CLASS:
                    CgDclEngDclStatusTelemeteredDataParticle,
                ParticleClassTypes.DLOG_AARM_PARTICLE_CLASS:
                    CgDclEngDclDlogAarmTelemeteredDataParticle,
            }

        }

        parser = CgDclEngDclParser(parser_config,
                                   stream_handle,
                                   self._exception_callback)

        return parser
