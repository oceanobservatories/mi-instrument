#!/usr/bin/env python
"""
If the option --process is used this application runs as a process that will
run once a day, looking at all of the ZPLSC-C instruments' latest raw data
files and generate the 24-hour echograms for each instrument.  It will store
the echogram in a local directory specified in the configuration file.

The application will run once and generate the echograms based on the arguments
passed in.

Usage:
    zplsc_echogram <subsites> [<deployments>] [<dates>] [--keep]
    zplsc_echogram (-p | --process) [--keep]
    zplsc_echogram (-a | --all) [--keep]
    zplsc_echogram (-f | --file) <zplsc_datafile>
    zplsc_echogram (-h | --help)

Arguments:
    subsites        Single or a list of subsites of ZPLSC instruments (delimit lists by "'s and space separated).
                    If omitted, all subsites from the config file are used and yesterday's echograms are generated.
    deployments     Single or a list of deployment numbers. (ex: 4 or "4 5")
    dates           Single or a list of dates of the desired 24 hour echograms.
                    (Ex: 2016-10-01 for one day or 2016-11 for the entire month or "2016-10-01 2016-11")
                    (Note: The date can also be in these formats: YYYY/MM/DD, YYYY/MM, YYYYMMDD or YYYYMM)
    zplsc_datafile  Raw ZPLSC data file

Options:
    -p --process    Starts the process that runs once/day generating echograms for all the ZPLSC subsites.
    -a --all        Generates echograms for all of the subsites in the configuration file.
    -f --file       Create a ZPLSC Echogram from the file given in the command.
    -h --help       Print this help message
    --keep          Keeps the temporary files after the echogram has been created."""

import docopt
import yaml
import types
import string
import shutil
import re
import calendar
import threading
import errno
import os
import os.path
from datetime import date, timedelta

from mi.logging import log
from mi.core.versioning import version
from mi.dataset.parser.zplsc_c import ZplscCParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import DATE2_YYYY_MM_DD_REGEX

__author__ = 'Rene Gelinas'

ZPLSC_CONFIG_FILE = 'zplsc_echogram_config.yml'

MODULE_NAME = 'mi.dataset.parser.zplsc_c'
CLASS_NAME = 'ZplscCRecoveredDataParticle'
CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
}

DCL_PATHS = [r'instrmt/dcl37', r'instrmts/dcl37', r'instruments/dcl37']
RECOVERED_DIR = r'%s_zplsc_%s_recovered_'
DATA_PATH = r'DATA'
RECOVERED_DATE_FMT = r'YYYY-MM-DD'

SECONDS_IN_DAY = 86400

RAW_FILE_EXT = r'.01A'
PNG_FILE_EXT = r'.png'
USER_HOME = r'~'
BASE_ECHOGRAM_DIRECTORY = r'~/ZPLSC_ECHOGRAMS/'
TEMP_DIR = r'TEMP_ZPLSC_DATA'

DATE_YYYY_MM_DD_REGEX_MATCHER = re.compile(DATE2_YYYY_MM_DD_REGEX + '$')

SERIAL_NUM_DIR_RE = r'ZPLSC_sn(\d*)\/?'
SERIAL_NUM_DIR_MATCHER = re.compile(SERIAL_NUM_DIR_RE)

DEPLOYMENT_DIR_RE = r'R(\d{5})\/?.*'
DEPLOYMENT_DIR_MATCHER = re.compile(DEPLOYMENT_DIR_RE)

DATE_DIR_RE = r'(\d{4})(\d{2}).*\/?'
DATE_DIR_RE_MATCHER = re.compile(DATE_DIR_RE)

HOUR_23_RAW_DATA_FILE_RE = r'(\d{4})(\d{2})23\.?.*'
HOUR_23_RAW_DATA_FILE_RE_MATCHER = re.compile(HOUR_23_RAW_DATA_FILE_RE)

"""
    Note: the format of the raw data path is as follows:
        '/omc_data/whoi/OMC/[SUBSITE]/[DEPLOYMENT]/[instrmt|instrmts|instruments]/dcl37/
        ZPLSC_sn[SERIAL_NUM]/ce01issm_zplsc_[SERIAL_NUM]_recovered_2016-05-26/DATA/[YYYYMM]/'
"""


@version("1.0.0")
class ZPLSCEchogramGenerator(object):
    """
    The ZPLSC Echogram Generator class will execute one time when command line
    parameters are passed in, generating all 24-hour echograms based on the
    parameters.  When no command line parameters are passed in, it will run as
    a process, executing once every day, generating echograms for every ZPLSC
    instruments from the previous day.
    """

    def __init__(self, _subsites, _deployments=None, _echogram_dates=None, _keep_temp_files=False,
                 _zplsc_datafile=None, _process_mode=False, _all_subsites=False):
        self.subsites = _subsites
        self.deployments = _deployments
        self.echogram_dates = self.parse_dates(_echogram_dates)
        self.keep_temp_files = _keep_temp_files
        self.process_mode = False
        self.zplsc_24_datafile_prefix = ''
        self.serial_num = ''
        self.temp_directory = ''
        self.base_echogram_directory = ''
        self.raw_data_dir = ''
        self.zplsc_datafile = _zplsc_datafile
        self.process_mode = _process_mode
        self.all_subsites = _all_subsites
        self.zplsc_subsites = []

        # Raise an exception if the any of the command line parameters are not valid.
        if not self.input_is_valid():
            raise ValueError

    #
    # Static Methods
    #
    @staticmethod
    def rec_exception_callback(exception):
        """
        Callback function to log exceptions and continue.

        @param exception - Exception that occurred
        """

        log.info("Exception occurred: %s", exception.message)

    @staticmethod
    def get_dir_contents(path, reverse_order=False):
        """
        This method will return the contents of the remote directory, based
        on the path passed.

        The method will raise the OSError exception if there is an issue
        reading the path passed in.

        :param reverse_order: If true the contents are sorted in reverse order.
        :param path: The path of the directory for which the contents are returned.
        :return: dir_contents: The contents of the remote directory.
        """

        try:
            dir_contents = os.listdir(path)
            dir_contents.sort(reverse=reverse_order)

        except OSError as ex:
            log.warning('Path does not exist: %s', ex)
            raise

        return dir_contents

    @staticmethod
    def parse_dates(echogram_dates):
        """
        Parse the date(s) passed in for proper format and convert them to a
        datetime object. Also, determine and set whether the entire month
        of echograms will be generated.

        :param echogram_dates: List of dates in string format.
        :return: parsed_dates: List of dates in the datetime object format.
        """

        parsed_dates = {}

        if echogram_dates is not None:
            for echogram_date in echogram_dates:
                date_regex = DATE_YYYY_MM_DD_REGEX_MATCHER.match(echogram_date)
                if date_regex:
                    year = int(date_regex.group(1))
                    month = int(date_regex.group(2))

                    if date_regex.lastindex == 3:
                        day = int(date_regex.group(3))
                        converted_date = date(year, month, day)
                        # Indicate this date is not an entire month.
                        parsed_dates[converted_date] = False
                    else:
                        day = 1
                        converted_date = date(year, month, day)
                        # Indicate this date is an entire month.
                        parsed_dates[converted_date] = True
                else:
                    log.error('Incorrect date format: %s: Correct format is YYYY[-/]MM-DD or YYYY[-/]MM', date)
                    parsed_dates = False
                    break

        return parsed_dates

    #
    # Member Methods
    #
    def input_is_valid(self):
        """
        This method validates the command line parameters entered.

        :return valid_input:  Boolean indicating whether all the inputs validated.
        """

        valid_input = True

        # Get the configuration parameters.
        zplsc_config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), ZPLSC_CONFIG_FILE)
        zplsc_config = None
        try:
            with open(zplsc_config_file, 'r') as config_file:
                try:
                    zplsc_config = yaml.load(config_file)
                except yaml.YAMLError as ex:
                    log.error('Error loading the configuration file: %s: %s', zplsc_config_file, ex.message)
                    valid_input = False
        except IOError as ex:
            log.error('Error opening configuration file: %s: %s', zplsc_config_file, ex.message)
            valid_input = False

        if valid_input:
            if self.zplsc_datafile is not None and not os.path.isfile(self.zplsc_datafile):
                    log.error('Invalid data file: %s', self.zplsc_datafile)

        if valid_input:
            self.zplsc_subsites = zplsc_config['zplsc_subsites']
            self.raw_data_dir = zplsc_config['raw_data_dir']

            # If this is a process run or we are processing all the subsite
            if self.process_mode or self.all_subsites:
                self.subsites = self.zplsc_subsites

            # If we are not generating a 1-hour echogram, validate the subsites in the list.
            if self.zplsc_datafile is None:
                for subsite in self.subsites:
                    if subsite not in self.zplsc_subsites:
                        log.error('Subsite is not in the list of subsites with ZPLSC instrumentation: %s', subsite)
                        valid_input = False
                        break

        if valid_input and self.deployments:
            if not isinstance(self.deployments, types.ListType):
                self.deployments = [self.deployments]

            for index in range(len(self.deployments)):
                try:
                    self.deployments[index] = int(self.deployments[index])

                except ValueError as ex:
                    log.error('Invalid deployment number: %s: %s', self.deployments[index], ex.message)
                    valid_input = False
                    break

        if valid_input:
            if self.echogram_dates is False:
                log.error('Invalid echogram date(s)')
                valid_input = False

        if valid_input:
            self.base_echogram_directory = zplsc_config.get('zplsc_echogram_directory', BASE_ECHOGRAM_DIRECTORY)

        return valid_input

    def get_data_filenames(self, date_dirs_path, data_date):
        """
        This method will take the data directory path and data date and it will
        return the list of filenames generated on the data date.

        :param date_dirs_path: The path of the date directories from the raw data server.
        :param data_date: The specific date of the data files.
        :return: filenames_list: The list of filenames for the data date passed in.
        """

        year = str(data_date.year)
        year2 = year[-2:]
        month = "%02d" % data_date.month
        day = "%02d" % data_date.day
        raw_datafile_prefix = year2 + month + day
        date_dir = year + month

        # Get the path for the ZPLSC raw data of the given instrument.
        raw_data_path = os.path.join(date_dirs_path, date_dir)
        all_files_list = self.get_dir_contents(raw_data_path)

        # Generate the list of the 24 1-hour raw data files for the given date.
        filename_re = '(' + raw_datafile_prefix + '.*?' + RAW_FILE_EXT + ').*'
        pattern = re.compile(filename_re)
        filenames_list = [filename for filename in all_files_list if pattern.match(filename)]

        return filenames_list, raw_data_path, raw_datafile_prefix

    def aggregate_raw_data(self, date_dirs_path, data_date):
        """
        This method will aggregate the 24 1-hour files residing at the path
        passed in for the date passed in.  It will copy the 24 files to a
        temporary directory and then aggregate them and store the single file
        in the temporary directory.  It will return the file name of the
        aggregated file and the file name of the echogram file that will be
        generated.

        Exceptions raised This method will raise an exception if there is an
        issue creating the local ZPLSC Echogram directory.

        :param date_dirs_path: The path of the raw data server where the 24 1-hour files reside.
        :param data_date: The date of the raw data for the echogram to be generated.
        :return: zplsc_24_datafile: The 24-hour concatenated raw data file
                 zplsc_echogram_file_path: The file path for the echogram to be generated.
        """

        zplsc_echogram_file_path = None

        try:
            filenames_list, raw_data_path, raw_datafile_prefix = self.get_data_filenames(date_dirs_path, data_date)
        except OSError:
            return '', None

        if len(filenames_list) != 24:
            return '', None

        # Copy the 24 1-hour raw data files to a temporary local directory.
        for raw_data_file in filenames_list:
            remote_raw_data_file = os.path.join(raw_data_path, raw_data_file)
            local_raw_data = os.path.join(self.temp_directory, raw_data_file)
            try:
                shutil.copyfile(remote_raw_data_file, local_raw_data)
            except OSError as ex:
                log.error('Error copying data file to temporary directory: %s: %s', remote_raw_data_file, ex.message)
                continue

        # Concatenate the 24 1-hour raw data files to 1 24-hour raw data file and return the filename.
        zplsc_24_datafilename = self.zplsc_24_datafile_prefix + raw_datafile_prefix
        zplsc_24_datafile = os.path.join(self.temp_directory, zplsc_24_datafilename) + RAW_FILE_EXT
        raw_data_glob = os.path.join(raw_data_path, raw_datafile_prefix) + '*' + RAW_FILE_EXT
        os.system('cat ' + raw_data_glob + ' > ' + zplsc_24_datafile)

        # Generate the ZPLSC Echogram filename.
        echogram_path_idx = string.find(raw_data_path, self.raw_data_dir)
        if echogram_path_idx >= 0:
            base_directory = os.path.expanduser(self.base_echogram_directory)
            path_structure = raw_data_path[echogram_path_idx+len(self.raw_data_dir)+1:]
            zplsc_echogram_file_path = os.path.join(base_directory, path_structure)

            # Create the ZPLSC Echogram directory structure if it doesn't exist.
            try:
                os.makedirs(zplsc_echogram_file_path)
            except OSError as ex:
                if ex.errno == errno.EEXIST and os.path.isdir(zplsc_echogram_file_path):
                    pass
                else:
                    log.error('Error creating local ZPLSC Echogram storage directory: %s', ex.message)
                    raise

        return zplsc_24_datafile, zplsc_echogram_file_path

    def get_latest_echogram_date(self, date_dirs_path, date_dirs):
        """
        This method will return the list of raw data files, in the format - YYMMDDHHMM,
        of the latest, complete set of 24 1-hour raw data files.

        :param date_dirs_path: The path to the directory of the YYYYMM sub-directories.
        :param date_dirs: The list of YYYYMM sub-directories
        :return: latest_echogram_date: The date of the latest complete set of 24 1-hour raw data files.
        """

        latest_echogram_date = None
        latest_echogram_date_found = False

        for date_dir in date_dirs:
            raw_data_path = os.path.join(date_dirs_path, date_dir[0] + date_dir[1])

            # Get all the year/month/day/hour raw data files date for this year/month directory.
            try:
                data_files = self.get_dir_contents(raw_data_path, True)
            except OSError:
                continue

            # Create a list of the last hour of each day of the month and sort them in reverse order
            hour_23_files = [(data_file[:4], data_file[4:6]) for data_file in data_files if data_file[6:8] == '23']

            # For each day that has a file of the last hour of the day, check the number of files for that day.
            for hour_23_file in hour_23_files:
                hour_files = [data_file for data_file in data_files if data_file[:4] == hour_23_file[0] and
                              data_file[4:6] == hour_23_file[1]]

                # If this day has 24 1-hour files, save it for the latest echogram date.
                if len(hour_files) == 24:
                    year = int(date_dir[0])
                    month = int(date_dir[1])
                    day = int(hour_23_file[1])
                    latest_echogram_date = date(year, month, day)

                    latest_echogram_date_found = True
                    break

            if latest_echogram_date_found:
                break

        return latest_echogram_date

    def get_date_dirs(self, subsite, deployment):
        """
        This method will generate the path to the directory of date directories
        in the format of YYYYMM.

        Exceptions raised by this method:
            OSError
            ValueError

        :param subsite: The subsite of the ZPLSC instrument.
        :param deployment: The deployment number of the data of interest.
        :return: echogram_dates: The mapping of echogram dates to the entire month flag
                 date_dirs_path: The path to the date directories.
        """

        # Generate the portion of the path up to the DCL directory to get the all the instrument sub-directories.
        deployment_dir = os.path.join(self.raw_data_dir, subsite.upper(), 'R%05d' % deployment)
        dcl_path = ''
        instrument_dirs = ''
        for dcl_rel_path in DCL_PATHS:
            dcl_path = os.path.join(deployment_dir, dcl_rel_path)
            try:
                instrument_dirs = self.get_dir_contents(dcl_path, True)
                break
            except OSError:
                log.info('Could not find path: %s: checking alternate path', dcl_path)
                if dcl_path is DCL_PATHS[-1]:
                    raise

        # Generate the portion of the path up to the ZPLSC Instrument serial number.
        serial_num_found = None
        for instrument in instrument_dirs:
            serial_num_found = SERIAL_NUM_DIR_MATCHER.match(instrument)
            if serial_num_found:
                break

        if serial_num_found is None:
            log.warning('Could not find ZPLSC data for subsite: %s and recovered deployment: %s', subsite, deployment)
            raise OSError

        self.serial_num = serial_num_found.group(1)
        serial_num_dir = os.path.join(dcl_path, serial_num_found.group())
        sub_dirs = self.get_dir_contents(serial_num_dir)

        # Generate the portion of the path that contains the recovered data path.
        recovered_path = RECOVERED_DIR % (subsite.lower(), self.serial_num)
        recovered_dir = ''
        for sub_dir in sub_dirs:
            if sub_dir.startswith(recovered_path):
                recovered_dir = sub_dir
                break

        if recovered_dir:
            # Create the raw data path including the recovered path
            date_dirs_path = os.path.join(serial_num_dir, recovered_dir, DATA_PATH)
        else:
            log.warning('Could not find ZPLSC recovered data path starting with: %s', recovered_path)
            raise OSError

        # If no dates were entered on the command line, get the entire  list of date directories.
        echogram_dates = self.echogram_dates
        if not echogram_dates:
            echogram_dates = {}

            # Get all the year/month date subdirectories for this subsite/deployment the get contents of the directory.
            date_dirs = self.get_dir_contents(date_dirs_path, True)
            date_dirs = [(date_dir[:4], date_dir[4:]) for date_dir in date_dirs]

            # If in process mode, get the latest date that has 24 1-hour data files for echogram generation.
            if self.process_mode:
                echogram_dates[self.get_latest_echogram_date(date_dirs_path, date_dirs)] = False

            # Otherwise, get all the year/month date subdirectories for this subsite and deployment.
            else:
                for date_dir in date_dirs:
                    year = int(date_dir[0])
                    month = int(date_dir[1])

                    # Save the date and indicate that the entire month should be generated.
                    echogram_dates[date(year, month, 1)] = True

        return echogram_dates, date_dirs_path

    def get_deployment_dirs(self, subsite):
        """
        This method will determine the deployment directories for the subsite
        passed in.

        :param subsite: The subsite of the ZPLSC instrument.
        :return: deployments: The list of deployment directories.
        """

        # Generate a temporary deployment list to maintain the integrity of the
        # original list for subsequent subsite processing.
        deployments = [deployment for deployment in self.deployments]
        if not deployments:
            # Generate the subsite portion of the raw data path and get all the files in the subsite path.
            subsite_path = os.path.join(self.raw_data_dir, subsite.upper())
            deployment_dirs = self.get_dir_contents(subsite_path)

            # Generate the list of the 24 1-hour raw data files for the given date.
            deployment_list = [(DEPLOYMENT_DIR_MATCHER.match(ddir)).group(1)
                               for ddir in deployment_dirs if DEPLOYMENT_DIR_MATCHER.match(ddir) is not None]

            if self.process_mode:
                deployment_list = [deployment_list[-1]]

            for deployment in deployment_list:
                try:
                    deployments.append(int(deployment))

                except ValueError as ex:
                    log.error('Invalid deployment number: %s: %s', deployment, ex.message)
                    break

        return deployments

    def purge_temporary_files(self):
        """
        This method will purge the temporary directory of the temporary files.
        """

        # If the temporary directory exists, purge it.
        if os.path.exists(self.temp_directory):
            for temp_file in os.listdir(self.temp_directory):
                os.remove(os.path.join(self.temp_directory, temp_file))

    def generate_zplsc_echograms(self):
        """
        This method will get the subsites, deployments and dates from the
        command line or all of the subsites, deployments and dates for the
        daily process.  It will generate the echograms based on those inputs
        and upload the echograms to the raw data server.

        :return:
        """

        # If we are creating a 1-hour echogram, generate the echogram.
        if self.zplsc_datafile is not None:
            # Send the 1-hour raw data file to the zplsc C Series parser to generate the echogram.
            with open(self.zplsc_datafile) as file_handle:
                base_directory = os.path.expanduser(self.base_echogram_directory)
                path_structure, filename = os.path.split(self.zplsc_datafile)
                zplsc_echogram_file_path = None
                for subsite in self.zplsc_subsites:
                    subsite_index = path_structure.find(subsite)
                    if subsite_index >= 0:
                        zplsc_echogram_file_path = os.path.join(base_directory, path_structure[subsite_index:])
                        # Create the ZPLSC Echogram directory structure if it doesn't exist.
                        try:
                            os.makedirs(zplsc_echogram_file_path)
                        except OSError as ex:
                            if ex.errno == errno.EEXIST and os.path.isdir(zplsc_echogram_file_path):
                                pass
                            else:
                                log.error('Error creating local ZPLSC Echogram storage directory: %s', ex.message)
                                raise
                        break

                if zplsc_echogram_file_path is not None:
                    # Get the parser for this file and generate the echogram.
                    parser = ZplscCParser(CONFIG, file_handle, self.rec_exception_callback)
                    parser.create_echogram(zplsc_echogram_file_path)
                else:
                    log.warning('The subsite is not one of the subsites containing a ZPLSC-C instrument.')

        else:  # We are creating 24-hour echograms ...
            # Create the temporary data file directory.
            self.temp_directory = os.path.join(os.path.expanduser(USER_HOME), TEMP_DIR)
            if not os.path.exists(self.temp_directory):
                os.mkdir(self.temp_directory)

            # Create the echograms for the zplsc instruments of each subsite.
            for subsite in self.subsites:
                zplsc_24_subsite_prefix = subsite + '-'

                try:
                    deployments = self.get_deployment_dirs(subsite)
                except OSError:
                    continue

                for deployment in deployments:
                    zplsc_24_deployment_prefix = zplsc_24_subsite_prefix + 'R' + str(deployment) + '-'

                    try:
                        echogram_dates, date_dirs_path = self.get_date_dirs(subsite, deployment)
                    except OSError:
                        continue

                    for date_dir, entire_month in echogram_dates.items():
                        self.zplsc_24_datafile_prefix = zplsc_24_deployment_prefix + 'sn' + self.serial_num + '-'

                        if entire_month:
                            number_of_days_in_the_month = calendar.monthrange(date_dir.year, date_dir.month)[1]
                            for day in range(number_of_days_in_the_month):
                                echogram_date = date_dir + timedelta(days=day)

                                # Aggregate the 24 raw data files for the given instrument to 1 24-hour data file.
                                zplsc_24_datafile, zplsc_echogram_file_path = self.aggregate_raw_data(date_dirs_path,
                                                                                                      echogram_date)
                                if not zplsc_24_datafile:
                                    log.warning('Unable to aggregate raw data files for %s under %s',
                                                echogram_date, date_dirs_path)
                                    continue

                                # Send the 24-hour raw data file to the zplsc C Series parser to generate the echogram.
                                with open(zplsc_24_datafile) as file_handle:
                                    parser = ZplscCParser(CONFIG, file_handle, self.rec_exception_callback)
                                    parser.create_echogram(zplsc_echogram_file_path)

                                if not self.keep_temp_files:
                                    self.purge_temporary_files()

                        else:
                            # Aggregate the 24 raw data files for the given instrument to 1 24-hour data file.
                            zplsc_24_datafile, zplsc_echogram_file_path = self.aggregate_raw_data(date_dirs_path, date_dir)

                            if not zplsc_24_datafile:
                                log.warning('Unable to aggregate raw data files for %s under %s', date_dir, date_dirs_path)
                                continue

                            # Send the 24-hour raw data file to the zplsc C Series parser to generate the echogram.
                            with open(zplsc_24_datafile) as file_handle:
                                parser = ZplscCParser(CONFIG, file_handle, self.rec_exception_callback)
                                parser.create_echogram(zplsc_echogram_file_path)

                            if not self.keep_temp_files:
                                self.purge_temporary_files()

            # Remove the temporary data file directory and its content.
            if not self.keep_temp_files:
                shutil.rmtree(self.temp_directory)

            # If it's running as a daily process, wait 24 hours and re-run this method
            if self.process_mode:
                threading.Timer(SECONDS_IN_DAY, self.generate_zplsc_echograms).start()


def main():
    # Get the command line arguments
    options = docopt.docopt(__doc__)
    subsites = options.get('<subsites>')
    deployments = options.get('<deployments>')
    dates = options.get('<dates>')
    keep_temp_files = options.get('--keep')
    process_mode = options.get('--process')
    all_subsites = options.get('--all')
    zplsc_datafile = options.get('<zplsc_datafile>')

    if subsites is not None:
        subsites = subsites.split(" ")

    if deployments is not None:
        deployments = deployments.split(" ")
    else:
        deployments = []

    if dates is not None:
        dates = dates.split(" ")

    try:
        echogram_generator = ZPLSCEchogramGenerator(subsites, deployments, dates, keep_temp_files,
                                                    zplsc_datafile, process_mode, all_subsites)
        echogram_generator.generate_zplsc_echograms()
        log.info('Echogram processing completed successfully!')

    except ValueError:
        log.error('Invalid command line parameters: exiting Echogram Generator')


if __name__ == '__main__':
    main()
