"""
@package mi.dataset.driver.zplsc_c
@file mi/dataset/driver/zplsc_c/zplsc_c_echogram.py
@author Craig Risien/Rene Gelinas
@brief ZPLSC Echogram generation for the ooicore

Release notes:

This class supports the generation of ZPLSC-C echograms.
"""

import numpy as np
import mi.dataset.driver.zplsc_c.zplsc_functions as zf

__author__ = 'Rene Gelinas'


class ZplscCParameters(object):
    # TODO: This class should be replaced by methods to get the CCs from the system.
    # Configuration Parameters
    Salinity = 32   # Salinity in psu
    Pressure = 150  # in dbars (~ depth of instrument in meters).
    Bins2Avg = 1    # number of range bins to average - 1 is no averaging


class ZplscCCalibrationCoefficients(object):
    # TODO: This class should be replaced by methods to get the CCs from the system.
    ka = 464.3636
    kb = 3000.0
    kc = 1.893
    A = 0.001466
    B = 0.0002388
    C = 0.000000100335

    TVR = []
    VTX = []
    BP = []
    EL = []
    DS = []

    # Freq 38kHz
    TVR.append(1.691999969482e2)
    VTX.append(1.533999938965e2)
    BP.append(8.609999902546e-3)
    EL.append(1.623000030518e2)
    DS.append(2.280000038445e-2)

    # Freq 125kHz
    TVR.append(1.668999938965e2)
    VTX.append(5.8e+01)
    BP.append(1.530999969691e-2)
    EL.append(1.376999969482e2)
    DS.append(2.280000038445e-2)

    # Freq 200kHz
    TVR.append(1.688999938965e2)
    VTX.append(9.619999694824e1)
    BP.append(1.530999969691e-2)
    EL.append(1.456000061035e2)
    DS.append(2.250000089407e-2)

    # Freq 455kHz
    TVR.append(1.696000061035e2)
    VTX.append(1.301000061035e2)
    BP.append(8.609999902546e-3)
    EL.append(1.491999969482e2)
    DS.append(2.300000004470e-2)


class ZPLSCCEchogram(object):
    def __init__(self):
        self.cc = ZplscCCalibrationCoefficients()
        self.params = ZplscCParameters()

    def compute_backscatter(self, profile_hdr, chan_data, sound_speed, depth_range, sea_absorb):
        """
        Compute the backscatter volumes values for one zplsc_c profile data record.
        This code was borrowed from ASL MatLab code that reads in zplsc-c raw data
        and performs calculations in order to compute the backscatter volume in db.

        :param profile_hdr: Raw profile header with metadata from the zplsc-c instrument.
        :param chan_data: Raw frequency data from the zplsc-c instrument.
        :param sound_speed: Speed of sound at based on speed of sound, pressure and salinity.
        :param depth_range: Range of the depth of the measurements
        :param sea_absorb: Seawater absorption coefficient for each frequency
        :return: sv: Volume backscatter in db
        """

        __N = []
        if self.params.Bins2Avg > 1:
            for chan in range(profile_hdr.num_channels):
                el = self.cc.EL[chan] - 2.5/self.cc.DS[chan] + np.array(chan_data[chan])/(26214*self.cc.DS[chan])
                power = 10**(el/10)

                # Perform bin averaging
                num_bins = len(chan_data[chan])/self.params.Bins2Avg
                pwr_avg = []
                for _bin in range(num_bins):
                    pwr_avg.append(np.mean(power[_bin*self.params.Bins2Avg:(_bin+1)*self.params.Bins2Avg]))

                el_avg = 10*np.log10(pwr_avg)
                __N.append(np.round(26214*self.cc.DS[chan]*(el_avg - self.cc.EL[chan] + 2.5/self.cc.DS[chan])))

        else:
            for chan in range(profile_hdr.num_channels):
                __N.append(np.array(chan_data[chan]))

        sv = []
        for chan in range(profile_hdr.num_channels):
            # Calculate correction to Sv due to non square transmit pulse
            sv_offset = zf.compute_sv_offset(profile_hdr.frequency[chan], profile_hdr.pulse_length[chan])
            sv.append(self.cc.EL[chan]-2.5/self.cc.DS[chan] + __N[chan]/(26214*self.cc.DS[chan]) - self.cc.TVR[chan] -
                      20*np.log10(self.cc.VTX[chan]) + 20*np.log10(depth_range[chan]) +
                      2*sea_absorb[chan]*depth_range[chan] -
                      10*np.log10(0.5*sound_speed*profile_hdr.pulse_length[chan]/1e6*self.cc.BP[chan]) +
                      sv_offset)

        return sv

    def compute_echogram_metadata(self, profile_hdr):
        """
        Compute the metadata parameters needed to compute the zplsc-c volume backscatter values.

        :param  profile_hdr: Raw profile header with metadata from the zplsc-c instrument.
        :return: sound_speed : Speed of sound based on temperature, pressure and salinity.
                 depth_range : Range of depth values of the zplsc-c data.
                 sea_absorb : Sea absorption based on temperature, pressure, salinity and frequency.
        """

        # If the temperature sensor is available, compute the temperature from the counts.
        temperature = 0
        if profile_hdr.is_sensor_available:
            temperature = zf.zplsc_c_temperature(profile_hdr.temperature, self.cc.ka, self.cc.kb, self.cc.kc,
                                                 self.cc.A, self.cc.B, self.cc.C)

        sound_speed = zf.zplsc_c_ss(temperature, self.params.Pressure, self.params.Salinity)

        _m = []
        depth_range = []
        for chan in range(profile_hdr.num_channels):
            _m.append(np.array([x for x in range(1, (profile_hdr.num_bins[chan]/self.params.Bins2Avg)+1)]))
            depth_range.append(sound_speed*profile_hdr.lockout_index[0]/(2*profile_hdr.digitization_rate[0]) +
                               (sound_speed/4)*(((2*_m[chan]-1)*profile_hdr.range_samples[0]*self.params.Bins2Avg-1) /
                                                float(profile_hdr.digitization_rate[0]) +
                                                profile_hdr.pulse_length[0]/1e6))

        sea_absorb = []
        for chan in range(profile_hdr.num_channels):
            # Calculate absorption coefficient for each frequency.
            sea_absorb.append(zf.zplsc_c_absorbtion(temperature, self.params.Pressure, self.params.Salinity,
                                                    profile_hdr.frequency[chan]))

        return sound_speed, depth_range, sea_absorb
