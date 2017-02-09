##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "mworden"

from mi.core.versioning import version
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.driver.WFP_ENG.STC_IMODEM.driver_common import WfpEngStcImodemDriver
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemEngineeringRecoveredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStartRecoveredDataParticle
from mi.dataset.parser.wfp_eng__stc_imodem_particles import WfpEngStcImodemStatusRecoveredDataParticle


@version("0.0.2")
def parse(unused, source_file_path, particle_data_handler):
    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.driver.WFP_ENG.STC_IMODEM',
        DataSetDriverConfigKeys.PARTICLE_CLASS: None,
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            'status_data_particle_class': WfpEngStcImodemStatusRecoveredDataParticle,
            'start_data_particle_class': WfpEngStcImodemStartRecoveredDataParticle,
            'engineering_data_particle_class': WfpEngStcImodemEngineeringRecoveredDataParticle
        }
    }

    driver = WfpEngStcImodemDriver(source_file_path, particle_data_handler, parser_config)

    return driver.process()
