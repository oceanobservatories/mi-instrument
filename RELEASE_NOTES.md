# Version 1.0.15

* Issue #15831 - Modify ZPLS echogram metadata parser to handle legacy EK60 data file names

# Version 1.0.14

* Issue #15717 - Makes following changes to work with new Pioneer MAB instruments:
  * Creates PRTSZ, PLIMS parsers and drivers. 
  * Updates PCO2A sample parser and driver to work with new sba5 format. 
  * Updates FLORT parser to output turbd data, and creates TURBD driver. 

# Version 1.0.13

* Antelope
   * Fixes Trace time precision and calculations

# Version 1.0.12

* Issue #15493 - Implementation of Asset Management updates to enable METBK-CT recovered-instr ingestion and data products
* Issue #15543 - Fill Value in ADCPT-M Processed Waves Fourier Coefficients Data

# Version 1.0.11

* Antelope
   * Fixes Trace starttime metadata
   * Adds orbreject command to support complex Orb channel selection

# Version 1.0.10

* Antelope
   * Modifies MSEED creation to multi-trace 5 minute files
   * Removes Orb packet time gap logic related to creating very small files

# Version 1.0.9

* Issue #15539 - Fix NUTNR SUNA recovered instrument parser bug

# Version 1.0.8

* Issue #15166 - Modify ZPLSC metadata uploader for latest Echopype files
* Issue #15346 - Change in PCO2W/PHSEN (SAMI) data output due to new Rev K board

# Version 1.0.7

* Issue #15073 - Change SUNA parser to ignore dark samples
* Issue #14891 - Adjust vel3d timestamps so they are unique

# Version 1.0.6

* Issue #15120 - Updating inductive CTDMO parser to accommodate cases of incomplete raw data

# Version 1.0.5

* Issue #15354 - Create dofst driver for do_fast_sample stream

# Version 1.0.4

* Issue #15327 - Updating NUTNR-M Parser to accommodate logging changes in glider software

# Version 1.0.3

* Issue #14261 - Correct timestamps for A-file (VEL3D) (#98) 
* Issue #15268 - PRESF pressure values issues
* Issue #15058 - Fix parser error in Global HYPM WFPENG E*.DAT recovered data

# Version 1.0.2

*  Issue #13743 - Global WFP offset between recovered and telemetered timestamps (#93)

# Version 1.0.1

*  Issue #15175 - Handle data data exceptions in the nutnr/suna parser (#92)

# Version 0.9.9.1

* Issue #15166 - Create ZPLSC echogram metadata uploader

# Version 0.9.9

* Issue #15032 - Adjust timestamps for vel3d relative to dcl file time

# Version 0.9.8

* Issue #11919 - Handle parsing error for adcp_pd0 parser

# Version 0.9.7

* Issue #14261 - Separate coastal and global wfp ctd drivers

# Version 0.9.6

* Issue #14261 - Fixed incorrect WFP CTD timestamps and pressures
* Issue #14654 - Add interpolation of m_gps_lat,lon into interp_lat,lon

# Version 0.9.5

* Issue #13369 - Add recovered,telemetered drivers to split PCO2A ingestions

# Version 0.9.4

* Issue #14609 - Fixed METBK CT driver/parser to remove inductive_id from generated stream
* Issue #14625 - Fix pCO2 parsers to generate correct streams during playback ingestions

# Version 0.9.3

* Issue #14609 - Fixed METBK CT driver/parser to ingest all files

# Version 0.9.2

* Issue #14304 - Added new METBK CT parser for Seabird SBE37SM-RS485 hex data file

# Version 0.9.1

* Issue #14170 - Fixed regex expression to include signed integers on pitch and roll. 

# Version 0.9.0

* Issue #14170 - PARAD_A driver modified to handle both LONG_ASCII output from the new Seabird sensor and the original SHORT_ASCII output from the older Satlantic sensor. 

# Version 0.8.2

* Issue #13182 - Async data download behaving differently on the Data Navigation tab than on the Plotting tab
   * Added m_lat,m_lon to class GpsPositionParticleKey to capture them into glider_gps_position stream 

# Version 0.8.1

* Issue #14184 - ensure data timestamps of pco2w_abc particles are treated as 1904-based
   * Modified pco2w_abc_particles to adjust the data timestamps from 1904-based to NTP

# Version 0.8.0

* Issue #13369 - add pco2a_a_sample parser,drivers and test files for new pco2a instrument

# Version 0.7.1

* Issue #13711 - update camhd_metadata particle parser to use camera log files
   * Reworked existing parser (not in use) to work with the archive camera log files

# Version 0.7.0

* Issue #13713 - prevent cg_dcl_eng parser from hanging on ingest
   * Encoding errors will no longer result in parser exiting prematurely (all parsers)
   * Added range checking option for particle value encoding (all parsers)
   * Corrected regex resulting in catastrophic backtracking (cg_dcl_eng)
   * Optimized file parsing logic (cg_dcl_eng)

# Version 0.6.9

* Issue #13722 - Platform node deployment update
   * Updated serial numbers for platform engineering nodes for most recent deployment

# Version 0.6.8

* Issue #13598 - Hotfix to correct cabled data ingest
   * Update for 0.5.3 changed the signature for from_url without updating all usages
   * WARNING - do not use versions 0.5.3 to 0.6.7 for running any cabled instrument drivers
   * WARNING - do not use versions 0.5.3 to 0.6.7 for running OMS alert/alarm handler
   * WARNING - do not use versions 0.5.3 to 0.6.7 for running OMS extractor

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
