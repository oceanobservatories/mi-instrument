Version 0.3.106

**ServiceRegistry**

* Improved error handling

Version 0.3.105

**Shovel**

* Added shovel status monitoring

Version 0.3.104

**Shovel**

* Added the ability to specify a queue and routing key. If the queue does not
already exist it will be created as an exclusive queue and deleted upon disconnect.

Version 0.3.103

**ZPLS**

* Added exception handling to echogram_thread and parse_echogram_file_wrapper

Version 0.3.102

**ZPLS**

* Moved echogram processing to an external processing pool *Redmine 11588*

Version 0.3.101

**ZPLS**

* Fixed bug where invalid data prevented plot creation

Version 0.3.100

**ZPLS**

* Output PNGs now being writen to same directory as raw input files.
* Power range set dynamically from 5-95% of data values *Redmine 11311*
* Increased resolution of plots and reduced borders.

Version 0.3.99

**ZPLS**

* Frequencies plots are now combined in a single plot. *Redmine 11310*
* Power range is now set to -25 to -80 dB. *Redmine 11311*
* Inverted transducer depth to create depth direction. *Redmine 11309*
