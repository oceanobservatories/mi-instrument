"""
@author Joffrey Peters
@brief Parser for the plims_a_adc dataset driver.

This file contains code for the PLIMS parser and code to produce data particles
for telemetered analog-digital converter data from the PLIMS instrument.

The input file is comma-delimited ASCII data.
Each record is a line in an ADC file.
Instrument records: data_name: data newline.
Data records only produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.
"""

import os
import re

import pandas as pd

from mi.core.exceptions import RecoverableSampleException, SampleException
from mi.core.log import get_logger

log = get_logger()
from mi.core.instrument.dataset_data_particle import DataParticleKey
from mi.core.time_tools import datetime_utc_to_ntp
from mi.dataset.dataset_parser import DataSetDriverConfigKeys, SimpleParser
from mi.dataset.parser.plims_a_particles import PlimsAAdcParticleKey

# Regex pattern for extracting datetime from filename
FNAME_DTIME_PATTERN = (
        r'.*' +
        r'\w(\d\d\d\d\d\d\d\dT\d\d\d\d\d\d)_' +  # Date format
        r'.+' +
        r'(?:\r\n|\n)?'  # Newline
)
FNAME_DATE_REGEX = re.compile(FNAME_DTIME_PATTERN)


class PlimsAAdcParser(SimpleParser):
    """
    Plims A (IFCB) ADC (analog-digital converter) file parser. 
    The telemetered and recovered files have the same fields and contents, 
    and can use the same parser.
    """

    def __init__(self, config, stream_handle, exception_callback):

        super(PlimsAAdcParser, self).__init__(config, stream_handle, exception_callback)
                                                      
        self._particle_class = config[
                DataSetDriverConfigKeys.PARTICLE_CLASS]

    def parse_file(self):
        """
        Parse through the file, pulling single comma-delimited lines, and
        generating particles for complete data lines
        """

        file = self._stream_handle
        
        file_name = os.path.basename(file.name).strip('.adc')
        file_name_date_match = FNAME_DATE_REGEX.match(file.name)
        if file_name_date_match is not None:
            # convert file name date/time string to seconds since 1970-01-01 in UTC
            file_timestamp = pd.to_datetime(file_name_date_match.group(1), format='%Y%m%dT%H%M%S')
            internal_timestamp = datetime_utc_to_ntp(file_timestamp)
        else:
            se = SampleException('Could not extract date from file name: {}'.format(file.name))
            self._exception_callback(se)
            raise se

        df = pd.read_csv(file, names=PlimsAAdcParticleKey.PLIMS_A_ADC_COLUMNS, 
                            skip_blank_lines=False, 
                            error_bad_lines=True,
                            dtype=PlimsAAdcParticleKey.PLIMS_A_ADC_DTYPES)
        if df is not None and not df.empty:
            df.drop(PlimsAAdcParticleKey.PLIMS_A_ADC_DROP_COLUMNS, axis=1, inplace=True)
            df[PlimsAAdcParticleKey.SAMPLE_TIMESTAMP] = internal_timestamp
            df[PlimsAAdcParticleKey.SAMPLE_FILENAME] = file_name
        if df.isna().values.any():
            plims_adc_data = None
        else:
            # plims_adc_data = df.to_dict('records') # This works in Pandas >= 0.24.2
            # Have to work around an issue where all values are interpreted as floats if 
            # dataframe contains any floats when using the to_dict() method.
            plims_adc_data = [x._asdict() for x in df.itertuples()]
        
        if plims_adc_data is not None:
            for record in plims_adc_data:
                # Drop "Index" keys, and replace with row number key
                row_number = record.pop('Index', None)
                if row_number is not None:
                    row_number = int(row_number) + 1  # Convert to 1-based index
                else:
                    log.warning("Row number not found in record: {}".format(record))

                record[PlimsAAdcParticleKey.SAMPLE_ADC_FILE_ROW_NUMBER] = row_number
                int_timestamp = internal_timestamp + record[PlimsAAdcParticleKey.GRAB_TIME_START]
                particle = self._extract_sample(self._particle_class, None, record,
                                                internal_timestamp = int_timestamp,
                                                preferred_ts=DataParticleKey.INTERNAL_TIMESTAMP)
                if particle is not None:
                    self._record_buffer.append(particle)
                    # log.trace('Parsed particle: {}'.format(particle.generate_dict()))
        else:
            self._exception_callback(RecoverableSampleException('Incomplete data file: {}'.format(file.name)))
