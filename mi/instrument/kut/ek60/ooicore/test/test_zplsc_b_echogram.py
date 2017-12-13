#!/usr/bin/env python

import os
import urllib
from mi.instrument.kut.ek60.ooicore.zplsc_b import parse_datagram_file_wrapper

# Get the path of this test file.
local_path = os.path.abspath(os.path.dirname(__file__))

# Generate remote and local file names.
raw_file_path = 'https://rawdata.oceanobservatories.org/files/CE04OSPS/PC01B/05-ZPLSCB102/2017/06/15/'
raw_file_name = 'CE04OSPS-PC01B-05-ZPLSCB102_OOI-D20170615-T000000.raw'
remote_raw_data_file = os.path.join(raw_file_path, raw_file_name)
local_raw_data_file = os.path.join(local_path, raw_file_name)

# Download the ZPLSC-B Series raw data file.
urllib.urlretrieve(remote_raw_data_file, local_raw_data_file)

# Create the ZPLSC-B Series echogram
parse_datagram_file_wrapper(local_raw_data_file)

