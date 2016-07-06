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

# INSTALL prerequisite software
## Install homebrew

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    brew doctor
    brew tap homebrew/science

## Install python 2.7

    brew install python --framework --universal

## Install git

    brew install git

## Install libraries

    brew install libevent libyaml zeromq rabbitmq hdf5 pkg-config netcdf freetype spatialindex udunits

## Install pip/virtualenv

    easy_install pip
    pip install virtualenv
    pip install virtualenvwrapper
    
## Modify ~/.bash_profile

    add this to the end:
       export WORKON_HOME=$HOME/virtenvs
       . /usr/local/share/python/virtualenvwrapper.sh
       
    source ~/.bash_profile
       
## Create a virtualenv

    mkvirtualenv --no-site-packages --python=python2.7 ooi


# INSTALL

Download the lastest source from github.

## Read only checkout
    $ git clone git://github.com/ooici/marine-integrations

## Read / write checkout
    $ git clone git@github.com:<your_github_uname>/marine-integrations

## Install requirements
    $ workon ooi
    $ python install -r requirements.txt
    
## Execute unit tests
    $ nosetests -a UNIT


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
