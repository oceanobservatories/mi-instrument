#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdpf_ckl.wfp_sio.ctdpf_ckl_wfp_sio_telemetered_driver.py
@file mi-dataset/mi/dataset/driver/ctdpf_ckl/wfp_sio/ctdpf_wfp_sio_telemetered_driver.py
@author Jeff Roy
@brief Driver for the ctdpf_ckl_wfp_sio instrument

Release notes:

Initial Release
"""
import os
import re

from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.dataset_driver import ProcessingInfoKey
from mi.dataset.driver.wfp_common.wfp_c_file_driver import WfpSioCFileDriver
from mi.dataset.parser.ctdpf_ckl_wfp_sio import CtdpfCklWfpSioParser
from mi.dataset.parser.ctdpf_ckl_wfp_sio import DataParticleType as CtdpfWfpSioDataParticleType
from mi.dataset.parser.ctdpf_ckl_wfp_sio import CtdpfCklWfpSioDataParticleKey
from mi.dataset.driver.flord_l_wfp.sio.flord_l_wfp_sio_telemetered_driver import FlordLWfpSioTelemeteredDriver

from mi.core.versioning import version
from mi.core.log import get_logger

log = get_logger()


@version("0.0.6")
def parse(unused, source_file_path, particle_data_handler):
    """
    This is the method called by Uframe
    :param unused
    :param source_file_path This is the full path and filename of the file to be parsed
    :param particle_data_handler Java Object to consume the output of the parser
    :return particle_data_handler
    """

    c_file_driver = None
    with open(source_file_path, 'rb') as stream_handle:
        # create and instance of the concrete driver class defined below
        c_file_driver = CtdpfWfpSioTelemeteredDriver(unused, stream_handle, particle_data_handler)
        c_file_driver.processFileStream()

    # get a list of e files that could possibly contain profiles
    # having the same time ranges as those in the c file
    e_file_paths = get_e_file_paths(source_file_path)

    for e_file_path in e_file_paths:
        if not os.path.exists(e_file_path):
            log.warning("e_file_path does not exist: %s" % e_file_path)
            continue

        #  Get a list of (time, pressure) tuples from the "E" file using the flord driver
        try:
            with open(e_file_path, 'rb') as flord_stream_handle:
                driver = FlordLWfpSioTelemeteredDriver(unused, flord_stream_handle, ParticleDataHandler())
                e_file_time_pressure_tuples = driver.get_time_pressure_tuples()
                if e_file_time_pressure_tuples:
                    if c_file_driver.add_possible_e_profiles(e_file_time_pressure_tuples):
                        log.info("e-profiles for %s were found in %s" % (source_file_path, e_file_path))
                else:
                    log.warning("e-profiles for %s were not generated from %s" % (source_file_path, e_file_path))
        except Exception as e:
            log.error(e)

        # stop looking for e profiles if we found all the ones we need
        if not c_file_driver.e_profiles_are_missing():
            break

    # Set a warning message in the particle_data_handler
    if c_file_driver.e_profiles_are_missing():
        warning_msg = "Could not find e file profiles for time ranges %s" % \
                      str(c_file_driver.get_missing_e_profile_time_ranges())
        log.warning(warning_msg + " for c file " + source_file_path)
        particle_data_handler.setProcessingInfo(ProcessingInfoKey.WARNING_MESSAGE, warning_msg)

    # adjust the times in the c profiles using the times (and pressures) in the e profiles
    c_file_driver.adjust_c_file_sample_times()

    # populate the ParticleDataHandler with the particles containing the adjusted data
    c_file_driver.populate_particle_data_handler()

    return particle_data_handler


def get_e_file_paths(c_file_path):
    # c file path example: "/omc_data/whoi/OMC/GI02HYPM/D00005/node23p1_10.wc_wfp_1399321.dat"

    e_file_paths = []

    # Get the e file name from the c file name
    head, tail = os.path.split(c_file_path)
    e_tail = tail.replace('wc_wfp', 'we_wfp')

    if e_tail == tail:
        log.error('Could not generate e file name')
        return e_file_paths

    # Parse the e file name to get the telemetry session sequence number
    telem_session_num_start_idx = e_tail.find("_")+1
    telem_session_num_end_idx = e_tail.find(".")

    pre_telem_session_num = e_tail[:telem_session_num_start_idx]
    telem_session_num = int(e_tail[telem_session_num_start_idx:telem_session_num_end_idx])
    post_telem_session_num = e_tail[telem_session_num_end_idx:]

    e_file_regex = re.compile(pre_telem_session_num + r'(?P<sess_num>\d+)' + post_telem_session_num)

    e_files_LT_telem_session_num = []
    e_files_GTE_telem_session_num = []

    e_files = [f for f in os.listdir(head) if e_file_regex.match(f)]
    e_files.sort(key=lambda f_name: int(e_file_regex.match(f_name).group('sess_num')))
    for f in e_files:
        sess_num = int(e_file_regex.match(f).group('sess_num'))
        if sess_num < telem_session_num:
            e_files_LT_telem_session_num.append(f)
        else:
            e_files_GTE_telem_session_num.append(f)

    e_files_LT_telem_session_num.reverse()

    # Alternate files before and after primary file so we search for the missing e profiles in both directions evenly
    # Limit to 250 just as a sanity check in case data dir is dirty
    curr_index = 0
    while curr_index < 250 and \
            (curr_index < len(e_files_LT_telem_session_num) or curr_index < len(e_files_GTE_telem_session_num)):
        if curr_index < len(e_files_GTE_telem_session_num):
            e_file_paths.append(os.path.join(head, e_files_GTE_telem_session_num[curr_index]))

        if curr_index < len(e_files_LT_telem_session_num):
            e_file_paths.append(os.path.join(head, e_files_LT_telem_session_num[curr_index]))

        curr_index += 1

    # Check if the primary file is in the list since it is the one most likely to contain the required profiles
    if not os.path.join(head, e_tail) in e_file_paths:
        log.warning("E file with same telemetry session number not found: %s" % os.path.join(head, e_tail))

    return e_file_paths


class CtdpfWfpSioTelemeteredDriver(WfpSioCFileDriver):
    """
    Derived ctdpf_ckl_wfp_sio driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        parser_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.ctdpf_ckl_wfp_sio',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['CtdpfCklWfpSioDataParticle',
                                                     'CtdpfCklWfpSioMetadataParticle']
        }

        parser = CtdpfCklWfpSioParser(parser_config, stream_handle,
                                      self._exception_callback)

        return parser

    def pressure_containing_data_particle_stream(self):
        return CtdpfWfpSioDataParticleType.DATA

    def pressure_containing_data_particle_field(self):
        return CtdpfCklWfpSioDataParticleKey.PRESSURE
