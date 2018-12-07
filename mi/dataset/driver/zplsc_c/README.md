# ZPLSC Echogram Generator
The ZPLSC Echogram Generator creates echograms (images) from the zooplankton sensor data (signal strength). The plots are
arranged by frequency band (typically four) and show signal strength (color) given depth (Y axis) along time (X axis). 
These plots are individually scaled by band to provide contrast for tracking organisms of various sizes. Note that the depth 
axis is the distance from the sensor which is typically pointed upward; sea surface up and down swells can thus be seen over 
the time period. 
## Execution

### Configuration
The necessary python modules must be available for use in the environment. On the test machine this is accomplished by 
sourcing the `engine` environment:

```bash
source activate engine
```

The configuration file provides the system defaults, such as the location of the raw data repository and the ZPLSC moorings.
This file is named zplsc_echogram_config.yml and should in the current directory in which the generator is run. Here's an 
example file:

```YAML
raw_data_dir: /omc_data/whoi/OMC
zplsc_echogram_directory: ~/ZPLSC_Echograms
zplsc_subsites:
  - CE01ISSM
  - CE06ISSM
  - CE07SHSM
  - CE09OSSM
  - CP01CNSM
  - CP03ISSM
  - CP04OSSM
```

The `raw_data_dir` must be accessible on the current systems file service and is the base location for the OMC data. The
generator will search the directory tree underneath to find raw ZPLS files.

The `zplsc_echogram_directory` is the output location for the ZPLSC echograms (PNG images). 

the `zplsc_subsites` is the list of all ZPLS files that are intended to be searched in the raw data respository. 

### Usage
```
zplsc_echogram_generator.py [<subsites>] [<deployments>] [<dates>] [--keep]
zplsc_echogram_generator.py (-h | --help)
```

*Arguments:*

`subsites`  Single or a list of subsites of ZPLSC instruments (delimit lists by "'s and space separated). If omitted, 
all subsites from the config file are used and yesterday's echograms are generated.

`deployments`  Single or a list of deployment numbers. (ex: 4 or "4 5")

`dates`  Single or a list of dates of the desired 24 hour echograms.

(Ex: 2016-10-01 for one day or 2016-11 for the entire month or "2016-10-01 2016-11")
(Note: The date can also be in these formats: YYYY/MM/DD, YYYY/MM, YYYYMMDD or YYYYMM)

*Options:*

`-h --help`  Print this help message

`--keep`  Keeps the temporary files after the echogram has been created.

For example:

`$ zplsc_echogram CE01ISSM 5 20160701`

