##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.WFP_ENG.STC_IMODEM.driver_common import WfpEngStcImodemDriver
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemEngineeringTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStartTelemeteredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStatusTelemeteredDataParticle


__author__ = "mworden"


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.driver.WFP_ENG.STC_IMODEM',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'status_data_particle_class': WfpEngStcImodemStatusTelemeteredDataParticle,
            'start_data_particle_class': WfpEngStcImodemStartTelemeteredDataParticle,
            'engineering_data_particle_class': WfpEngStcImodemEngineeringTelemeteredDataParticle
        }
    }

    driver = WfpEngStcImodemDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
