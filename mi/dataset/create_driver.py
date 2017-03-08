#!/usr/bin/env python

import inspect, os
import sys, traceback

def ubar_to_camel(s):
    # make the first char upper case
    s = s[0].upper() + s[1:]
    foo =  [i for i, ltr in enumerate(s) if ltr == '_']
    # now go backwards removing the underbar and upper casing the next letter
    for i in reversed(foo):
        s = s[:i] + s[(i+1)].upper() + s[(i+2):]

    return s


basePath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))  + '/driver'
#print 'basePath is ' + basePath

if not os.path.exists(basePath):
    print 'error: driver directory not found'
    sys.exit(0)


#
print "create_driver:  This script will ask for driver identification items,"
print "                 and then build a template driver file, creating the"
print "                 target directories if necessary"


inst_name = raw_input('Enter Instrument Name ( ex: flntu_x ): ')
plat_name = raw_input('Enter Instrument Name ( ex: mmp_cds ): ')
driver_file_name = raw_input('Enter Driver File Name ( ex: flntu_x_mmp_cds_recovered_driver.py ): ')
# the driver name is the same as the filename, without the '.py'
driver_name = driver_file_name[0:-3]
auth_name = raw_input('Enter Author Name: ')


# gather the platform and instrument into an underbar delimited string
ubar_name = (inst_name + '_' + plat_name).lower()
#print 'underbar name is ' + ubar_name

# convert above to camel case
camel_case_name = ubar_to_camel(ubar_name)
#print 'camelcase name is ' + camel_case

# name a camel case version of the driver name
camel_case_driver_name = ubar_to_camel(driver_name)
#print camel_case_driver_name

# check/create the top level instrument directory
tld_dir = basePath + '/' + inst_name
if not os.path.exists(tld_dir):
    print "No instrument directory found, creating " + tld_dir
    os.makedirs(tld_dir)

# check to see if a platform has been provided.  If so, check/create.  Else set target to instrument level.
if not plat_name:
    tgt_dir = tld_dir
else:
    tgt_dir = tld_dir + '/' + plat_name
    if not os.path.exists(tgt_dir):
        print "No platform directory found, creating " + tgt_dir
        os.makedirs(tgt_dir)

# now open the target driver file.  Don't clobber.
tgt_filename = tgt_dir + '/' +  driver_file_name
if os.path.exists(tgt_filename):
    print 'error: driver file ' + tgt_filename + ' already exists'
    sys.exit(0)


f = open(tgt_filename,'w+')


f.write('#!/usr/bin/env python' + '\n')
f.write('' + '\n')
f.write('"""' + '\n')
if not plat_name:
    f.write('@package mi.dataset.driver.' + inst_name + '.'  + driver_name + '\n')
else:
    f.write('@package mi.dataset.driver.' + inst_name + '.' + plat_name + '.' + driver_name + '\n')


if not plat_name:
    f.write('@file mi-dataset/mi/dataset/driver/' + inst_name + '/' + driver_file_name + '\n')
else:
    f.write('@file mi-dataset/mi/dataset/driver/' + inst_name + '/' + plat_name + '/' + driver_file_name + '\n')

f.write('@author ' + auth_name + '\n')
f.write('@brief Driver for the ' + ubar_name + ' instrument' + '\n')
f.write('\n')
f.write('Release notes:' + '\n')
f.write('\n')
f.write('Initial Release' + '\n')
f.write('"""' + '\n')
f.write('' + '\n')
f.write('from mi.dataset.dataset_parser import DataSetDriverConfigKeys' + '\n')
f.write('from mi.dataset.dataset_driver import SimpleDatasetDriver' + '\n')
f.write('from mi.core.exceptions import NotImplementedException' + '\n')
f.write('from mi.dataset.parser.' + ubar_name + ' import ' + camel_case_name + 'Parser' + '\n')
f.write('' + '\n')
f.write('' + '\n')
f.write('def parse(unused, source_file_path, particle_data_handler):' + '\n')
f.write('    """' + '\n')
f.write('    This is the method called by Uframe' + '\n')
f.write('    :param unused' + '\n')
f.write('    :param source_file_path This is the full path and filename of the file to be parsed' + '\n')
f.write('    :param particle_data_handler Java Object to consume the output of the parser' + '\n')
f.write('    :return particle_data_handler' + '\n')
f.write('    """' + '\n')
f.write('' + '\n')
f.write('    with open(source_file_path, \'rb\') as stream_handle:' + '\n')
f.write('' + '\n')
f.write('        # create an instance of the concrete driver class defined below' + '\n')
f.write('        driver = ' + camel_case_driver_name + '(unused, stream_handle, particle_data_handler)' + '\n')
f.write('        driver.processFileStream()' + '\n')
f.write('' + '\n')
f.write('    return particle_data_handler' + '\n')
f.write('' + '\n')
f.write('' + '\n')
f.write('class ' + camel_case_driver_name + '(SimpleDatasetDriver):' + '\n')
f.write('    """' + '\n')
f.write('    Derived ' + ubar_name + ' driver class' + '\n')
f.write('    All this needs to do is create a concrete _build_parser method' + '\n')
f.write('    """' + '\n')
f.write('' + '\n')
f.write('    def _build_parser(self, stream_handle):' + '\n')
f.write('' + '\n')
f.write('        raise NotImplementedException(\"_build_parser() not overridden!\")')

print 'create_driver: done'




















