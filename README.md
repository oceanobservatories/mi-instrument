[![Build Status](https://travis-ci.org/oceanobservatories/mi-instrument.svg?branch=master)](https://travis-ci.org/oceanobservatories/mi-instrument)
[![codecov](https://codecov.io/gh/oceanobservatories/mi-instrument/branch/master/graph/badge.svg)](https://codecov.io/gh/oceanobservatories/mi-instrument)

```
Ocean Observatories Initiative Cyber Infrastructure (OOI CI)
Integrated Observatory Network (ION) - OOINet

Marine Integration source code repository.

(C) The Regents of the University of California, 2010-2014
See LICENSE.txt for license.

This is the repository that contains the implemention for all marine integrations
including drivers and transforms. 
```

# Get the code!
## Clone this repository
### Read only checkout
    $ git clone git://github.com/oceanobservatories/mi-instrument
### Read / write checkout
    $ git clone git@github.com:<your_github_uname>/mi-instrument

# OSX/Homebrew Instructions
## INSTALL prerequisite software
### Install homebrew

    https://brew.sh/
    brew tap homebrew/science

### Install python 2.7

    brew install python --framework --universal

### Install git

    brew install git

### Install libraries

    brew install libevent libyaml zeromq rabbitmq hdf5 pkg-config netcdf freetype spatialindex udunits

### Install pip/virtualenv

    pip install -U pip
    pip install virtualenv virtualenvwrapper

### Modify ~/.bash_profile

    add this to the end:
       export WORKON_HOME=$HOME/virtenvs
       . /usr/local/bin/virtualenvwrapper.sh
       
    source ~/.bash_profile
       
### Create a virtualenv

    mkvirtualenv --no-site-packages --python=python2.7 ooi

### Install requirements
    workon ooi
    pip install numpy cython
    pip install -r requirements.txt

# OSX/Anaconda Instructions
## Install Anaconda/Miniconda 2

https://conda.io/miniconda.html

## Create conda env

    cd into mi-instrument and then type the command below:
    conda env create -f conda_env_other.yml
    source activate mi

# Linux/Anaconda Instructions
## Install Anaconda/Miniconda 2

https://conda.io/miniconda.html

## Create conda env

    conda env create -f conda_env_linux64.yml
    source activate mi

# Run the tests

    nosetests -a UNIT --processes=4 --process-timeout=360

# Table of Contents

Source code is organized in directories by instrument vendor. The following is
a listing of currently deployed instruments by make.

```
Instrument	  location
----------------------------------------------
ADCP          teledyne.workhorse.adcp
ADCPS         teledyne.workhorse.adcp
BOTPT         noaa.botpt.ooicore
CAMDS         kml.cam.camds
CAMHD         subc_control.onecam.ooicore
CTDBP-NO      seabird.sbe16plus_v2.ctdbp_no
CTDPF-Optode  seabird.sbe16plus_v2.ctdpf_jb
CTDPF-SBE43   seabird.sbe16plus_v2.ctdpf_sbe43
D1000         mclane.ras.d1000
FLOR          wetlabs.fluorometer.flort_d
HPIES         uw.hpies.ooicore
MASSP         harvard.massp.mcu
NUTNR         satlantic.suna_deep.ooicore
OPTAA         wetlabs.ac_s.ooicore
PARAD         satlantic.par_ser_600m
PCO2W-A       sunburst.sami2_pco2.pco2a
PCO2W-B       sunburst.sami2_pco2.pco2b
PHSEN         sunburst.sami2_ph.ooicore
PPSDN         mclane.ras.ppsdn
PREST         seabird.sbe54tps
RASFL         mclane.ras.rasfl
SPKIR         satlantic.ocr_507_icsw.ooicore
THSPH         um.thsph.ooicore
TMPSF         rbr.xr_420_thermistor_24.ooicore
TRHPH         uw.bars.ooicore
VADCP         teledyne.workhorse.vadcp
VEL3D-B       nobska.mavs4.ooicore
VEL3D_B       nobska.mavs4.ooicore
VEL3D-C       nortek.vector.ooicore
VEL3D_C       nortek.vector.ooicore
VELPT         nortek.aquadopp.ooicore
ZPLSC         kut.ek60.ooicore
```


# Release Instructions

Deploying a release of the mi-instrument package involves the following steppes:

1. update version
1. push changes to repository (mi-instrument)
1. tag release
1. build anaconda package

## Update Version

Release notes (RELEASE_NOTES) should include the Version and number on a line by itself, followed by a list of issues 
with a brief description. Prepend updates to the beginning of the file. E.g.:

```
Version 1.2.3

* Issue #12345 - Fix ctdbp parser to read new format xyz
```

The setup file (setup.py) must be updated with the appropriate version:

```python
version = '1.2.3'
```

## Tag Release

Set the new tag:

```
git tag -a v0.4.0
git push upstream master --tags
```

Get the current tags from remote repository (to see the currently available tags):

```
git fetch upstream --tags
```

## Build Anaconda Package

See the [ooi-config](https://github.com/oceanobservatories/ooi-config) repository for instructions. 

# Tools

The following tools are available in the project (WORK IN PROGRESS):

| Name | Description |
| --- | --- |
| [zplsc_echogram_generator](mi/dataset/driver/zplsc_c/README.md) | Generates echograms from raw sensor data files. |
