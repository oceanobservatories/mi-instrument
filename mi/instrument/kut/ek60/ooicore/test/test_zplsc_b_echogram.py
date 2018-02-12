#!/usr/bin/env python

from mi.instrument.kut.ek60.ooicore.zplsc_b import parse_echogram_file_wrapper

# Generate remote and local file names.
raw_data_file = '/rsn_cabled/rsn_data/DVT_Data/pc01b/ZPLSCB102_10.33.10.143/OOI-D20170101-T000000.raw'
echogram_file = '/Users/admin/temp'

# Create the ZPLSC-B Series echogram
parse_echogram_file_wrapper(raw_data_file, echogram_file)
