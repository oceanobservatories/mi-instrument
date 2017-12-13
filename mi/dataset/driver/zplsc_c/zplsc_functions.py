#!/usr/bin/env python
"""
@package mi.dataset.driver.zplsc_c.zplsc_functions
@file mi.dataset.driver.zplsc_c/zplsc_functions.py
@author Rene Gelinas
@brief Module containing ZPLSC related data calculations.
"""

import numpy as np


def zplsc_b_decompress(power):
    """
    Description:

        Convert a list of compressed power values to numpy array of
        decompressed power.  This code was from the zplsc_b.py parser,
        when it only produced an echogram.

    Implemented by:

        2017-06-27: Rene Gelinas. Initial code.

    :param power: List of zplsc B series power values in a compressed format.
    :return: Decompressed numpy array of power values.
    """

    decompress_power = np.array(power) * 10. * np.log10(2) / 256.

    return decompress_power


def zplsc_c_temperature(counts, ka, kb, kc, a, b, c):
    """
    Description:

        Compute the temperature from the counts passed in.
        This Code was lifted from the ASL MatLab code LoadAZFP.m

    Implemented by:

        2017-06-23: Rene Gelinas. Initial code.

    :param counts: Raw data temperature counts from the zplsc-c raw data file.
    :param ka:
    :param kb:
    :param kc:
    :param a:
    :param b:
    :param c:
    :return: temperature
    """

    vin = 2.5 * (counts / 65535)
    r = (ka + kb*vin) / (kc - vin)
    temperature = 1 / (a + b * (np.log(r)) + c * (np.log(r)**3)) - 273

    return temperature


def zplsc_c_tilt(counts, a, b, c, d):
    """
    Description:

        Compute the tilt from the counts passed in from the zplsc A series.
        This Code was from the ASL MatLab code LoadAZFP.m

    Implemented by:

        2017-06-23: Rene Gelinas. Initial code.

    :param counts:
    :param a:
    :param b:
    :param c:
    :param d:
    :return: tilt value
    """

    tilt = a + (b * counts) + (c * counts**2) + (d * counts**3)

    return tilt


def zplsc_c_ss(t, p, s):
    """
    Description:

        Compute the ss from the counts passed in.
        This Code was from the ASL MatLab code LoadAZFP.m

    Implemented by:

        2017-06-23: Rene Gelinas. Initial code.

    :param t:
    :param p:
    :param s:
    :return:
    """

    z = t/10
    sea_c = 1449.05 + (z * (45.7 + z*((-5.21) + 0.23*z))) + ((1.333 + z*((-0.126) + z*0.009)) * (s-35.0)) + \
        (p/1000)*(16.3+0.18*(p/1000))

    return sea_c


def zplsc_c_absorbtion(t, p, s, freq):
    """
    Description:

        Calculate Absorption coeff using Temperature, Pressure and Salinity and transducer frequency.
        This Code was from the ASL MatLab code LoadAZFP.m

    Implemented by:

        2017-06-23: Rene Gelinas. Initial code.


    :param t:
    :param p:
    :param s:
    :param freq:  Frequency in KHz
    :return: sea_abs
    """

    # Calculate relaxation frequencies
    t_k = t + 273.0
    f1 = 1320.0*t_k * np.exp(-1700/t_k)
    f2 = 1.55e7*t_k * np.exp(-3052/t_k)

    # Coefficients for absorption equations
    k = 1 + p/10.0
    a = 8.95e-8 * (1 + t*(2.29e-2 - 5.08e-4*t))
    b = (s/35.0)*4.88e-7*(1+0.0134*t)*(1-0.00103*k + 3.7e-7*(k*k))
    c = 4.86e-13*(1+t*((-0.042)+t*(8.53e-4-t*6.23e-6)))*(1+k*(-3.84e-4+k*7.57e-8))
    freqk = freq*1000
    sea_abs = (a*f1*(freqk**2))/((f1*f1)+(freqk**2))+(b*f2*(freqk**2))/((f2*f2)+(freqk**2))+c*(freqk**2)

    return sea_abs


def compute_sv_offset(frequency, pulse_length):
    """
    A correction must be made to compensate for the effects of the finite response
    times of both the receiving and transmitting parts of the instrument. The magnitude
    of the correction will depend on the length of the transmitted pulse, and the response
    time (on both transmission and reception) of the instrument.

    :param frequency: Frequency in KHz
    :param pulse_length: Pulse length in uSecs
    :return:
    """

    sv_offset = 0

    if frequency > 38:  # 125,200,455,769 kHz
        if pulse_length == 300:
            sv_offset = 1.1
        elif pulse_length == 500:
            sv_offset = 0.8
        elif pulse_length == 700:
            sv_offset = 0.5
        elif pulse_length == 900:
            sv_offset = 0.3
        elif pulse_length == 1000:
            sv_offset = 0.3
    else:  # 38 kHz
        if pulse_length == 500:
            sv_offset = 1.1
        elif pulse_length == 1000:
            sv_offset = 0.7

    return sv_offset
