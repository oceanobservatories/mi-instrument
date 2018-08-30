# Version 0.6.7

* Issue #13568 - Added capability of processing a 1-hour data file
   * Added option --file - Generate an echogram from a single 1-hour file from the command line
   * Added option --all - Generate all echograms for all ZPLSC instruments
   * Added option --process - Runs once/day for all instruments, generating the latest full day echograms

# Version 0.6.6

* Issue #13533 - Added support to parse instrument recovered CTDBP/FLORTD combined data

# Version 0.6.5

* Issue #13288 - Changed conditional logic. Changed NDF to SDF.

# Version 0.6.4

* Issue #13288 - Added NTP timestamp to suna

# Version 0.6.3

* Issue #13288 - Fixed suna parser to work with uFrame, added instrument driver

# Version 0.6.2

* Issue #13245 - Corrected REGEX in the flort_dj_dcl.py parser.

# Version 0.6.1

* Issue #13288 - Added suna parser

# Version 0.6.0

* Issue #11419 - Added support for the new Neil Brown CTD
   * Added driver/parser support for the new Neil Brown CTD attached to the AUVs.
   * Added ctdav_n_auv_driver.py driver and deprecated ctdav_n_auv_recovered(telemetered)_driver(s).py drivers.
   * Added ctdav_nbosi_auv_driver.py driver and ctdav_nbosi_auv.py parser for the new Neil Brown CTD.
   * Added unit tests for the new Neil Brown CTD parser.

# Version 0.5.13

* Issue #13170 - Corrected engineering platform configurations to reflect last deployment

# Version 0.5.12

* Issue #13174 - Modified ZPLSC-B Test Method To Access Raw Data Via Remotely Mounted File System

# Version 0.5.11

* Issue #13106 - Overrode parse_file and _build_parsed_values functions
   * Removed global regex variables and instead used python text processing libraries
   * Append only float values

# Version 0.5.10

* Issue #13119 - Added support for ZPLSC C Echogram Generator To Access Raw Data Remote Mount

# Version 0.5.9

* Issue #13098 - Added individual colorbars for each frequency's echogram.
* Issue #12427 Update ADCP Parsers and Consolidate ADCP streams:
   * stream adcp_velocity_earth replaces:
       * adcp_velocity_glider  (mi/dataset/parser/adcp_pd0.py)
       * adcp_velocity_inst    (mi/dataset/parser/adcp_pd0.py)  
   * stream adcp_velocity_beam replaces:
       * adcp_pd0_beam_parsed  (mi/instrument/teledyne/workhorse/particles.py)
       * vadcp_5thbeam_pd0_beam_parsed  (mi/instrument/teledyne/workhorse/particles.py)
       * vadcp_pd0_beam_parsed  (mi/instrument/teledyne/workhorse/particles.py)
   * stream adcp_system_configuration replaces:
       * vadcp_4beam_system_configuration  (mi/instrument/teledyne/workhorse/particles.py)
   * stream adcp_system_configuration_5 replaces:
       * vadcp_5thbeam_system_configuration  (mi/instrument/teledyne/workhorse/particles.py)

# Version 0.5.8

* Issue #12584 - Added individual colobars for each frequency's echogram.
* Issue #13083 - Restored feature in zplsc_b parser to generate an echogram.

# Version 0.5.7

* Issue #12833 - Added support for offline process generated ZPLSC-C echograms.

# Version 0.5.6

* Issue #12289 - Added DO driver to generate stable dissolved oxygen stream from CTD with attached DO
* Issue #12323 - Added support for UI generated ZPLSC C-Series echograms.

# Version 0.5.5

* Issue #12832 Fixed update playback.py to assign 'zplsc_reader' variable before being referenced
 
# Version 0.5.4

* Issue #12574 update ctdpf_jb driver to parse CTD data with missing optode sensor data (CTDPFB304)

# Version 0.5.3

* Issue #12499 modified playback to be callable via ingest engine and ingest requests

# Version 0.5.2

* Issue #12435 FLOR - Consolidate Streams:
    * flort_sample now replaces:
        * flort_kn_stc_imodem_instrument
        * flort_kn_stc_imodem_instrument_recovered
        * flort_dj_dcl_instrument
        * flort_dj_dcl_instrument_recovered
        * flort_dj_cspp_instrument
        * flort_dj_sio_instrument
        * flort_dj_sio_instrument_recovered
    * flort_m_sample now replaces:
         * flort_m_glider_instrument
         * flort_m_glider_recovered
    * flort_kn_sample now replaces:
         * flort_kn_auv_instrument
         * flort_kn_auv_instrument_recovered

# Version 0.5.1

* Issue #12167 Modify ZPLSC cabled driver to produce time series data
  Changed the ZPLSC cabled driver to only produce time series data (and not echograms plot)

# Version 0.5.0

* Issue #12253 Parsers are not setting port_timestamp and #12254 Extraneous data in DCL parser particles
* Modified DCL parser:
    - port_timestamp will be set from the DCL header timestamp value
    - dcl_controller_timestamp - should set the port_timestamp (already in particle) instead of using a string
    - internal_timestamp will be set from the instrument payload
    - date_string - should use internal_timestamp (already in particle) instead of using a string
    - all timestamp string values will not be set (removed)

* Modified DCL parsers are:
    - adcpt_acfgm_dcl_pd0    			
    - adcpt_acfgm_dcl_pd8    			
    - ctdbp_cdef_dcl 				  		
    - dcl_file_common 				 		
    - metbk_a_dcl 				  		
    - pco2a_a_dcl    				
    - dosta_abcdjm_dcl  					
    - dosta_abcdjm_ctdbp_dcl 		
    - fdchp_a_dcl 					
    - flort_dj_dcl      					
    - fuelcell_eng_dcl  				
    - hyd_o_dcl							
    - pco2w_abc_dcl    				  	
    - pco2w_abc_particles    			
    - phsen_abcdef_dcl 				  
    - presf_abc_dcl  				  
    - rte_o_dcl 					
    - spkir_abj_dcl    				
    - wavss_a_dcl
    - zplsc_c_dcl
    - nutnr_b_dcl_conc 					
    - nutnr_b_dcl_full 					
    - nutnr_b_dcl_parser_base		
    - nutnr_b_particles				
    - cg_dcl_eng_dcl				
    - cspp_eng_dcl 				

# Version 0.4.2

* Issue #12191 - Need to parse CTDMO recovered_host instrument data (ctdmo_ghqr)
Added parser to parse CTDMO recovered_host instrument data

Added the following drivers:
* /mi/dataset/driver/ctdmo_ghqr/sio/ctdmo_ghqr_sio_ct_recovered_driver.py

# Version 0.4.1

* Issue #11462 - wavss_a_dcl - 
* Issue #12229 - BOTPT transducer temperature parameter values being truncated

# Version 0.4.0

* Issue #10398 - No Recovered parser/ingest queue for ZPLSC
* Issue #12137 - Fixed adcpt_acfgm_dcl_pd0 Unit Tests
* Issue #12118 - ZPLSC_C Telemetered Stream Has Extraneous Parameters
* Issue #11920 - Fixed bug in OMS alert processing
* Merged mi-dataset repository

Added the following drivers:

* mi/dataset/driver/camds/camds_abc_driver.py

# Version 0.3.106

**ServiceRegistry**

* Improved error handling

# Version 0.3.105

**Shovel**

* Added shovel status monitoring

# Version 0.3.104

**Shovel**

* Added the ability to specify a queue and routing key. If the queue does not
already exist it will be created as an exclusive queue and deleted upon disconnect.

# Version 0.3.103

**ZPLS**

* Added exception handling to echogram_thread and parse_echogram_file_wrapper

# Version 0.3.102

**ZPLS**

* Moved echogram processing to an external processing pool *Redmine 11588*

# Version 0.3.101

**ZPLS**

* Fixed bug where invalid data prevented plot creation

# Version 0.3.100

**ZPLS**

* Output PNGs now being writen to same directory as raw input files.
* Power range set dynamically from 5-95% of data values *Redmine 11311*
* Increased resolution of plots and reduced borders.

# Version 0.3.99

**ZPLS**

* Frequencies plots are now combined in a single plot. *Redmine 11310*
* Power range is now set to -25 to -80 dB. *Redmine 11311*
* Inverted transducer depth to create depth direction. *Redmine 11309*

# Version 0.2.0 (mi-dataset)

Added the following drivers:

* mi/dataset/driver/ctdbp_p/ctdbp_p_recovered_driver.py
* mi/dataset/driver/dosta_abcdjm/ctdbp_p/dosta_abcdjm_ctdbp_p_recovered_driver.py
* mi/dataset/driver/flord_g/ctdbp_p/flord_g_ctdbp_p_recovered_driver.py
