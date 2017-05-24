# Version 0.4.3

* Issue #12253 Parsers are not setting port timestamp
* Modified DCL parser,
*  so that they set instrument timestamp using timestamp from the encapsulated data
*  and set the port timestamp using DCL controller timestamp from the DCL wrapper

* Modified below DCL parser:
    adcpt_acfgm_dcl_pd0    
    adcpt_acfgm_dcl_pd8    
    ctdbp_cdef_dcl
    dcl_file_common
    metbk_a_dcl    
    pco2a_a_dcl    
    dosta_abcdjm_dcl  
    dosta_abcdjm_ctdbp_dcl
    fdchp_a_dcl
    flort_dj_dcl        
    fuelcell_eng_dcl  
    hyd_o_dcl
    pco2w_abc_dcl        
    pco2w_abc_particles    
    phsen_abcdef_dcl
    presf_abc_dcl    
    rte_o_dcl
    spkir_abj_dcl    
    wavss_a_dcl.
    zplsc_c_dcl.
    parsercg_dcl_eng_dcl
    cspp_eng_dcl
    nutnr_b_dcl_conc
    nutnr_b_dcl_full

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
