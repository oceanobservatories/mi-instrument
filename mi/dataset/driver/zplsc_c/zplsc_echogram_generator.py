#!/usr/bin/env python
"""
If no arguments are passed in this application runs as a process that will
run once a day, looking at all of the ZPLSC-C instruments' latest raw data
and generate the 24-hour echograms for each instrument.  It will store the
echogram in a local directory specified in the configuration file.

If arguments are passed in, the application will run once and generate the
echograms based on the passed arguments.

Usage:
    zplcs_echogram_generator.py [<subsites>] [<deployments>] [<dates>] [--keep]
    zplcs_echogram_generator.py (-h | --help)

Arguments:
    subsites      Single or a list of subsites of ZPLSC instruments (delimit lists by "'s and space separated).
                  If omitted, all subsites from the config file are used and yesterday's echograms are generated.
    deployments   Single or a list of deployment numbers. (ex: 4 or "4 5")
    dates         Single or a list of dates of the desired 24 hour echograms.
                  (Ex: 2016-10-01 for one day or 2016-11 for the entire month or "2016-10-01 2016-11")

Options:
    -h --help     Print this help message
    --keep        Keeps the temporary files after the echogram has been created.
"""

import docopt
import yaml
import types
import string
import shutil
import re
import calendar
import threading
import urllib
import urllib2
import errno
from os import mkdir, system, makedirs
from os.path import exists, expanduser, join, isdir
from datetime import date, timedelta

from mi.logging import log
from mi.dataset.parser.zplsc_c import ZplscCParser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.common_regexes import DATE2_YYYY_MM_DD_REGEX, DATE2_YYYY_MM_REGEX

__author__ = 'Rene Gelinas'

ZPLSC_CONFIG_FILE = 'zplsc_echogram_config.yml'

MODULE_NAME = 'mi.dataset.parser.zplsc_c'
CLASS_NAME = 'ZplscCRecoveredDataParticle'
CONFIG = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
    DataSetDriverConfigKeys.PARTICLE_CLASS: CLASS_NAME
}

RAW_DATA_URL = r'https://rawdata.oceanobservatories.org/files'
DCL_PATH = r'instrmts/dcl37'
DCL_PATH2 = r'instruments/dcl37'
DCL_PATH3 = r'instrmt/dcl37'
RECOVERED_DIR = r'#SUBSITE_LC#_zplsc_#SERIAL_NUM#_recovered_'
DATA_PATH = r'DATA'
RECOVERED_DATE_FMT = r'YYYY-MM-DD'

SECONDS_IN_DAY = 86400

RAW_FILE_EXT = r'.01A'
PNG_FILE_EXT = r'.png'
USER_HOME = r'~'
BASE_ECHOGRAM_DIRECTORY = r'~/ZPLSC_ECHOGRAMS/'
TEMP_DIR = r'TEMP_ZPLSC_DATA'

DATE_YYYY_MM_DD_REGEX_MATCHER = re.compile(DATE2_YYYY_MM_DD_REGEX + '$')
DATE_YYYY_MM_REGEX_MATCHER = re.compile(DATE2_YYYY_MM_REGEX + '$')

SERIAL_NUM_DIR_RE = r'(ZPLSC_sn(\d*))\/?'
SERIAL_NUM_DIR_MATCHER = re.compile(SERIAL_NUM_DIR_RE)

DEPLOYMENT_DIR_RE = r'R(\d{5})\/?.*'
DEPLOYMENT_DIR_MATCHER = re.compile(DEPLOYMENT_DIR_RE)

DATE_DIR_RE = r'(\d{4})(\d{2}).*\/?'
DATE_DIR_RE_MATCHER = re.compile(DATE_DIR_RE)

HOUR_23_RAW_DATA_FILE_RE = r'(\d{4})(\d{2})23\.?.*'
HOUR_23_RAW_DATA_FILE_RE_MATCHER = re.compile(HOUR_23_RAW_DATA_FILE_RE)

"""
    Note: the format of the raw data URL is as follows:
        'https://rawdata.oceanobservatories.org/files/[SUBSITE]/[DEPLOYMENT]/[instrmt|instrmts|instruments]/dcl37/
        ZPLSC_sn[SERIAL_NUM]/ce01issm_zplsc_[SERIAL_NUM]_recovered_2016-05-26/DATA/[YYYYMM]/'
"""


class ZPLSCEchogramGenerator(object):
    """
    The ZPLSC Echogram Generator class will execute one time when command line
    parameters are passed in, generating all 24-hour echograms based on the
    parameters.  When no command line parameters are passed in, it will run as
    a process, executing once every day, generating echograms for every ZPLSC
    instruments from the previous day.
    """

    def __init__(self, _subsites, _deployments=None, _echogram_dates=None, _keep_temp_files=False):
        self.subsites = []
        self.deployments = []
        self.echogram_dates = {}
        self.keep_temp_files = _keep_temp_files
        self.process_mode = False
        self.zplsc_24_datafile_prefix = ''
        self.serial_num = ''
        self.temp_directory = ''
        self.base_echogram_directory = ''

        # Raise an exception if the any of the command line parameters are not valid.
        if not self.input_is_valid(_subsites, _deployments, _echogram_dates):
            raise ValueError

    #
    # Static Methods
    #
    @staticmethod
    def rec_exception_callback(exception):
        """
        Call back method to for exceptions

        @param exception - Exception that occurred
        """

        log.info("Exception occurred: %s", exception.message)

    @staticmethod
    def get_dir_contents(url):
        """
        From the URL path passed return the contents of the remote directory.

        :param url: The URL of the directory for which the contents are returned.
        :return: dir_contents: The contents of the remote URL directory.
        """

        try:
            url_io = urllib2.urlopen(url)
            dir_contents = url_io.read().decode('utf-8')

        except urllib2.HTTPError as ex:
            log.warning('URL does not exist: %s: %s', url, ex)
            raise

        return dir_contents

    @staticmethod
    def parse_dates(echogram_dates):
        """
        Parse the date(s) passed in for proper format and convert them to a
        datetime object. Also, determine and set whether the entire month
        of echograms will be generated.

        :param echogram_dates: List of d
        ates in string format.
        :return: parsed_dates: List of dates in the datetime object format.
        """

        parsed_dates = dict()

        if echogram_dates is not None:
            for echogram_date in echogram_dates:
                date_regex = DATE_YYYY_MM_DD_REGEX_MATCHER.match(echogram_date)
                if date_regex:
                    year = int(date_regex.group(1))
                    month = int(date_regex.group(2))
                    day = int(date_regex.group(3))

                    converted_date = date(year, month, day)

                    # Indicate this date is not an entire month.
                    parsed_dates[converted_date] = False

                else:
                    date_regex = DATE_YYYY_MM_REGEX_MATCHER.match(echogram_date)

                    if date_regex:
                        year = int(date_regex.group(1))
                        month = int(date_regex.group(2))
                        day = 1

                        converted_date = date(year, month, day)

                        # Indicate this date is an entire month.
                        parsed_dates[converted_date] = True

                    else:
                        log.error('Incorrect date format: %s: Correct format is YYYY-MM-DD or YYYY-MM', date)
                        parsed_dates = False
                        break

        return parsed_dates

    #
    # Member Methods
    #
    def input_is_valid(self, subsites, deployments, echogram_dates):
        """
        This method validates the command line parameters entered.

        :param subsites: The subsite(s) that the ZPLSC instrument(s) are attached.
        :param deployments: The command line deployment number of interest.
        :param echogram_dates: The command line dates of interest.
        :return valid_input:  Boolean indicating whether all the inputs validated.
        """

        valid_input = True

        # Get the configuration parameters.
        zplsc_config = yaml.load(open(ZPLSC_CONFIG_FILE))
        zplsc_subsites = zplsc_config['zplsc_subsites']

        # If no subsites were passed in, set the list of subsites to all in the config file.
        self.subsites = subsites
        if subsites is None:
            self.subsites = zplsc_subsites
            self.process_mode = True

        # Validate the subsites in the list.
        for subsite in self.subsites:
            if subsite not in zplsc_subsites:
                log.error('Subsite %s is not in the list of subsites with ZPLSC instrumentation.', subsite)
                valid_input = False
                break

        if valid_input and deployments is not None:
            self.deployments = []
            if not isinstance(deployments, types.ListType):
                deployments = [deployments]

            for deployment in deployments:
                try:
                    self.deployments.append(int(deployment))

                except ValueError as ex:
                    log.error('Invalid deployment number: %s: %s', deployment, ex.message)
                    valid_input = False
                    break

        if valid_input:
            self.echogram_dates = self.parse_dates(echogram_dates)
            if self.echogram_dates is False:
                log.error('Invalid start date: %s', echogram_dates)
                valid_input = False

        zplsc_echrogram_directory = zplsc_config['zplsc_echrogram_directory']
        if not zplsc_echrogram_directory:
            self.base_echogram_directory = BASE_ECHOGRAM_DIRECTORY
        else:
            self.base_echogram_directory = zplsc_echrogram_directory

        return valid_input

    def get_data_filenames(self, date_dirs_url, data_date):
        """
        This method will take the data directory URL and data date and it will
        return the list of filenames generated on the data date.

        :param date_dirs_url: The URL of the date directories on the raw data server
        :param data_date: The specific date of the data files.
        :return: filenames_list: The list of filenames for the data date passed in.
        """

        year = str(data_date.year)
        year2 = year[-2:]
        month = "%02d" % data_date.month
        day = "%02d" % data_date.day
        raw_datafile_prefix = year2 + month + day
        date_dir = year + month

        # Get the URL for the ZPLSC raw data of the given instrument.
        raw_data_url = join(date_dirs_url, date_dir)
        try:
            all_files_list = self.get_dir_contents(raw_data_url)
        except urllib2.HTTPError:
            raise

        # Generate the list of the 24 1-hour raw data files for the given date.
        filename_re = '(' + raw_datafile_prefix + '.*?' + RAW_FILE_EXT + ').*'
        pattern = re.compile(filename_re)
        filenames_list = pattern.findall(all_files_list)

        return filenames_list, raw_data_url, raw_datafile_prefix

    def aggregate_raw_data(self, date_dirs_url, data_date):
        """
        This method will retrieve the 24 1-hour files residing at the URL
        passed in for the date passed in.  It will store the files locally,
        concatenate them to one file.  It will return the file name of the
        concatenated file and the file name of the echogram that will be
        generated.

        :param date_dirs_url: The URL of the raw data server where the 24 1-hour files reside.
        :param data_date: The date of the raw data for the echogram to be generated.
        :return: zplsc_24_datafile: The 34-hour concatenated raw data file
                 zplsc_echogram_file_path: The file path for the echogram to be generated.
        """

        zplsc_24_datafile = ''
        zplsc_echogram_file_path = None

        filenames_list, raw_data_url, raw_datafile_prefix = self.get_data_filenames(date_dirs_url, data_date)
        if len(filenames_list) == 24:
            # Download the 24 1-hour raw data files to a temporary local directory.
            for raw_data_file in filenames_list:
                remote_raw_data_file = join(raw_data_url, raw_data_file)
                local_raw_data = join(self.temp_directory, raw_data_file)
                try:
                    urllib.urlretrieve(remote_raw_data_file, local_raw_data)
                except urllib.ContentTooShortError as ex:
                    log.error('Error retrieving: %s: %s', remote_raw_data_file, ex.message)
                    continue

            # Concatenate the 24 1-hour raw data files to 1 24-hour raw data file and return the filename.
            zplsc_24_datafilename = self.zplsc_24_datafile_prefix + raw_datafile_prefix
            zplsc_24_datafile = join(self.temp_directory, zplsc_24_datafilename) + RAW_FILE_EXT
            system('cat ' + join(self.temp_directory, raw_datafile_prefix) + '*' + RAW_FILE_EXT + ' > ' +
                   zplsc_24_datafile)

            # Generate the ZPLSC Echogram filename.
            echogram_path_idx = string.find(raw_data_url, RAW_DATA_URL)
            if echogram_path_idx >= 0:
                base_directory = expanduser(self.base_echogram_directory)
                path_structure = raw_data_url[echogram_path_idx+len(RAW_DATA_URL)+1:]
                zplsc_echogram_file_path = join(base_directory, path_structure)

                # Create the ZPLSC Echogram directory structure if it doesn't exist.
                try:
                    makedirs(zplsc_echogram_file_path)
                except OSError as ex:
                    if ex.errno == errno.EEXIST and isdir(zplsc_echogram_file_path):
                        pass
                    else:
                        raise

        return zplsc_24_datafile, zplsc_echogram_file_path

    def get_latest_echogram_date(self, date_dirs_url, date_dirs_list):
        """
        This method will return the list of raw data files, in the format - YYMMDDHHMM,
        of the latest, complete set of 24 1-hour raw data files.

        :param date_dirs_url: The URL to the directory of the YYYYMM sub-directories.
        :param date_dirs_list: The list of YYYYMM sub-directories
        :return: latest_echogram_date: The date of the latest complete set of 24 1-hour raw data files.
        """

        latest_echogram_date = None
        latest_echogram_date_found = False

        for date_dir in date_dirs_list:
            raw_data_files_url = join(date_dirs_url, date_dir[0]+date_dir[1])

            # Get all the year/month/day/hour raw data files date for this year/month directory.
            try:
                raw_data_files_response = self.get_dir_contents(raw_data_files_url)
            except urllib2.HTTPError:
                continue

            # Create a list of the last hour of each day of the month and sort them in reverse order
            hour_23_data_file_list = HOUR_23_RAW_DATA_FILE_RE_MATCHER.findall(raw_data_files_response)
            hour_23_data_file_list = sorted(hour_23_data_file_list, key=lambda x: x[1], reverse=True)

            # For each day that has a file of the last hour of the day, check the number of files for that day.
            for data_file in hour_23_data_file_list:
                latest_echogram_date_re = data_file[0] + data_file[1] + r'\d{2}\.?.*'
                latest_echogram_date_re_matcher = re.compile(latest_echogram_date_re)
                latest_echogram_date_list = latest_echogram_date_re_matcher.findall(raw_data_files_response)

                # If this day has 24 1-hour files, save it for the latest echogram date.
                if len(latest_echogram_date_list) == 24:
                    year = int(date_dir[0])
                    month = int(date_dir[1])
                    day = int(data_file[1])
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

        :param subsite: The subsite of the ZPLSC instrument.
        :param deployment: The deployment number of the data of interest.
        :return: echogram_dates: The mapping of echogram dates to the entire month flag
                 date_dirs_url: The path to the date directories.
        """

        echogram_dates = dict()
        date_dirs_url = ''

        # Generate the portion of the URL up to the DCL directory to get the all the instrument sub-directories.
        deployment_path = join(RAW_DATA_URL, subsite.upper(), 'R%05d' % deployment)
        dcl_path = join(deployment_path, DCL_PATH)
        try:
            instrument_dirs = self.get_dir_contents(dcl_path)
        except urllib2.HTTPError:
            log.info('Could not find "%s": searching for "%s"', DCL_PATH, DCL_PATH2)

            # Check the alternate directory structure.
            dcl_path = join(deployment_path, DCL_PATH2)
            try:
                instrument_dirs = self.get_dir_contents(dcl_path)
            except urllib2.HTTPError:
                log.info('Could not find "%s": searching for "%s"', DCL_PATH2, DCL_PATH3)

                # Check the alternate directory structure.
                dcl_path = join(deployment_path, DCL_PATH3)
                try:
                    instrument_dirs = self.get_dir_contents(dcl_path)
                except urllib2.HTTPError:
                    raise

        # Generate the portion of the URL up to the ZPLSC Instrument serial number.
        serial_num_found = SERIAL_NUM_DIR_MATCHER.search(instrument_dirs)
        if serial_num_found is None:
            log.warning('Could not find ZPLSC data for subsite: %s and deployment: %s', subsite, deployment)

        else:
            self.serial_num = serial_num_found.group(2)
            serial_num_url = join(dcl_path, SERIAL_NUM_DIR_MATCHER.search(instrument_dirs).group(1))
            try:
                sub_dirs = self.get_dir_contents(serial_num_url)
            except urllib2.HTTPError:
                raise

            # Generate the portion of the URL that contains the recovered data path.
            recovered_path = RECOVERED_DIR.replace('#SUBSITE_LC#', subsite.lower())
            recovered_path = recovered_path.replace('#SERIAL_NUM#', self.serial_num)
            start_idx = sub_dirs.find(recovered_path)

            # If this is the directory structure that has the recovered directory, add it to the URL.
            date_dirs_url = serial_num_url
            if start_idx != -1:
                end_idx = start_idx + len(recovered_path) + len(RECOVERED_DATE_FMT)
                recovered_path = join(sub_dirs[start_idx:end_idx], DATA_PATH)

                # Create the raw data URL with the recovered path
                date_dirs_url = join(serial_num_url, recovered_path)

            # If no dates were entered on the command line, get the entire  list of date directories.
            echogram_dates = self.echogram_dates
            if not echogram_dates:
                # Get all the year/month date subdirectories for this subsite and deployment.
                try:
                    date_dirs_response = self.get_dir_contents(date_dirs_url)
                except urllib2.HTTPError:
                    raise

                # Generate the list of the date directories.
                echogram_dates = dict()
                date_dirs_list = DATE_DIR_RE_MATCHER.findall(date_dirs_response)
                date_dirs_list = sorted(date_dirs_list, key=lambda x: (x[0], x[1]), reverse=True)

                # If in process mode, get the latest date that has 24 1-hour data files for echogram generation.
                if self.process_mode:
                    echogram_dates[self.get_latest_echogram_date(date_dirs_url, date_dirs_list)] = False

                # Otherwise, get all the year/month date subdirectories for this subsite and deployment.
                else:
                    for date_dir in date_dirs_list:
                        year = int(date_dir[0])
                        month = int(date_dir[1])

                        # Save the date and indicate that the entire month should be generated.
                        echogram_dates[date(year, month, 1)] = True

        return echogram_dates, date_dirs_url

    def get_deployment_dirs(self, subsite):
        """
        This method will determine the deployment directories for the subsite
        passed in.

        :param subsite: The subsite of the ZPLSC instrument.
        :return: deployments: The list of deployment directories.
        """

        deployments = [deployment for deployment in self.deployments]
        if not deployments:
            # Generate the portion of the URL up to the subsite directory.
            subsite_url = join(RAW_DATA_URL, subsite.upper())
            try:
                # Get the all the deployment sub-directories under the subsite directory.
                deployment_dirs = self.get_dir_contents(subsite_url)
            except urllib2.HTTPError:
                raise

            # Generate the list of the 24 1-hour raw data files for the given date.
            deployment_list = DEPLOYMENT_DIR_MATCHER.findall(deployment_dirs)
            deployment_list.sort()

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
        if exists(self.temp_directory):
            system('rm -f ' + join(self.temp_directory, '*'))

    def generate_zplsc_echograms(self):
        """
        This method will get the subsites, deployments and dates from the
        command line or all of the subsites, deployments and dates for the
        daily process.  It will generate the echograms based on those inputs
        and upload the echograms to the raw data server.

        :return:
        """

        # Create the temporary data file directory.
        self.temp_directory = join(expanduser(USER_HOME), TEMP_DIR)
        if not exists(self.temp_directory):
            mkdir(self.temp_directory)

        # Create the echograms for the zplsc instruments of each subsite.
        for subsite in self.subsites:
            zplsc_24_subsite_prefix = subsite + '-'

            try:
                deployments = self.get_deployment_dirs(subsite)
            except urllib2.HTTPError:
                continue

            for deployment in deployments:
                zplsc_24_deployment_prefix = zplsc_24_subsite_prefix + 'R' + str(deployment) + '-'

                try:
                    echogram_dates, date_dirs_url = self.get_date_dirs(subsite, deployment)
                except urllib2.HTTPError:
                    continue

                for date_dir, entire_month in echogram_dates.items():
                    self.zplsc_24_datafile_prefix = zplsc_24_deployment_prefix + 'sn' + self.serial_num + '-'

                    if entire_month:
                        number_of_days_in_the_month = calendar.monthrange(date_dir.year, date_dir.month)[1]
                        for day in range(number_of_days_in_the_month):
                            echogram_date = date_dir + timedelta(days=day)

                            # Aggregate the 24 raw data files for the given instrument to 1 24-hour data file.
                            try:
                                zplsc_24_datafile, zplsc_echogram_file_path = self.aggregate_raw_data(date_dirs_url,
                                                                                                      echogram_date)
                                if not zplsc_24_datafile:
                                    continue

                            except urllib2.HTTPError:
                                continue

                            # Send the 24-hour raw data file to the zplsc C Series parser to generate the echogram.
                            with open(zplsc_24_datafile) as file_handle:
                                parser = ZplscCParser(CONFIG, file_handle, self.rec_exception_callback)
                                parser.create_echogram(zplsc_echogram_file_path)

                            self.purge_temporary_files()

                    else:
                        # Aggregate the 24 raw data files for the given instrument to 1 24-hour data file.
                        try:
                            zplsc_24_datafile, zplsc_echogram_file_path = self.aggregate_raw_data(date_dirs_url,
                                                                                                  date_dir)

                            if not zplsc_24_datafile:
                                continue

                        except urllib2.HTTPError:
                            continue

                        # Send the 24-hour raw data file to the zplsc C Series parser to generate the echogram.
                        with open(zplsc_24_datafile) as file_handle:
                            parser = ZplscCParser(CONFIG, file_handle, self.rec_exception_callback)
                            parser.create_echogram(zplsc_echogram_file_path)

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
    subsites = options['<subsites>']
    deployments = options['<deployments>']
    dates = options['<dates>']
    keep_temp_files = options['--keep']

    if subsites is not None:
        subsites = subsites.split(" ")

    if deployments is not None:
        deployments = deployments.split(" ")

    if dates is not None:
        dates = dates.split(" ")

    try:
        echogram_generator = ZPLSCEchogramGenerator(subsites, deployments, dates, keep_temp_files)
        echogram_generator.generate_zplsc_echograms()
        log.info('Echogram processing completed successfully!')

    except ValueError:
        log.error('Invalid command line parameters: exiting Echogram Generator')


if __name__ == '__main__':
    main()
