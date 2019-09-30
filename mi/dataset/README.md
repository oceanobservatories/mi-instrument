Note - this information was taken from the document titled "Dataset Parser Implemementation Process" which was created in 
April of 2017 and some instructions may be outdated. This information has been preserved here as the source document may
no longer be available soon, and the content should live close to the source code to which it applies.

# Scope

This document is an overview of a suggested process of implementing a dataset drivers and parsers for OOI instruments:

# Process

## Verify The Redmine Ticket Information

Verify the Ticket requesting the parser, has the minimal information needed in the description:

* A link to the manual containing the data format for the instrument.
* One or more instrument reference designators.

## Verify The Data Format

Compare and verify that the example raw data (from the link in the description) is in the format defined in the manual 
(some instruments output multiple formats of data):

* For ASCII files it is straightforward comparing the contents of the file with the example in the manual.
* For binary data, a binary editor can be used to accomplish this.  The definition will define each parameter as a 
field (position) and the number of bytes.

Contact Ticket author or SME on any questions on the data format.

## Create/Update IDD

Find an IDD in Confluence that is closely related to the instrument being parsed as a template:

* Look for IDDs that have the same instrument designator; perhaps of a different instrument series or with/without 
additional “_dcl”, etc.  For example: When generating the IDD for `zplsc_c`, you may be able to start with `zplsc_c_dcl`.

* Check the output stream, of the potential template, to determine if it contains most or all of the output parameters 
needed in the new parser’s stream.
* Reuse of data parameters is desired.  If parameters exist that are of the same type and definition and the name is 
generic (not specific to an instrument), then it should be reused for the new stream (See 4.).
* Reuse of output streams is desired.  If an existing stream contains all of the parameters needed, even with “a few” 
additional parameters, this may be used with the additional parameters set to null.  The amount of “a few” is subjective 
and should be confirmed with SMEs.

Once an IDD is found to be used as a template, click ‘Tools > Copy’ in the upper right corner of the page. This will put 
you in a copy of the original in edit mode. Change the name to the new parser and change the “IDD Status” to “Draft” in the 
table at the top. There are 3 tabs at the top to choose different editing modes (Rich Text or Wiki Markup).

For each section of the IDD, update the descriptions based on the instrument being parsed.

For the Output Parameters (particle stream definition) section, update the parameters to the new parser’s parameters as 
defined in the data format.

* Reuse an existing stream if possible.
* Reuse existing parameters if possible.
* Only data parameters that correlate directly with raw data should be listed.  Derived parameters that are defined in 
the stream are not listed, because the parser does not calculate derived parameters.
* If new parameters need to be created, make the the name generic enough such they may be reused.

_Note_
Updating the Output Parameters and  Preload Database is an iterative process.  Investigating the Preload Database 
will need to be performed to update the Output Parameters. The IDD is a living document as implementation continues.  If a 
parameter or stream changes during development, the changes shall be reflected in the IDD.

## Update Preload Database

Access the preload-database repository by cloning it locally (if not already).

The `csv` folder contains CSV files with the definition of the parameters and the streams:

* `BinSizes.csv` - Defines the bin size for each stream.
* `nominal_depths.csv` - Defines the deployed depth of each stream.
* ParameterDefs.csv` - Defines parameters to be included in the streams definitions.
* `ParameterDictionary.csv` - Defines the streams and the parameters that are included.
* `ParameterFunctions.csv` - Defines the functions to be used to calculate the derived products.

Typically, only `ParameterDefs.csv` and `ParameterDictionary.csv` need to be updated.

The files can be opened and edited in a text editor (harder visually), which is safe when saving. They also can be edited 
in a spreadsheet application (Google Sheets, Excel, Libreoffice Calc, etc,), but caution should be taken as the end of line 
character(s) may be different than what is currently in the files when saving as a CSV file.

After editing execute, run `git diff`, to verify the changes.

If the entire file was changed, try running `dos2unix`, then run `git diff` again to verify the changes.

There is a tool in the preload-database folder, `list_stream_data.py`, that can help visualize the definition and 
relationship of the parameters and streams:

Usage:
```shell
list_stream_data.py stream <name_or_id>...
list_stream_data.py parameter <name_or_id>...
```

The names and ids can be found in the CSV files.

_Note_
Updating the Output Parameters and updating the Preload Database is an iterative process. Investigating the Preload 
Database will need to be performed to update the Output Parameters.

## Dataset Driver/Parser

Access the mi-instrument repository by cloning it locally (if not already).
The drivers and parsers can be found in 
[mi/dataset/driver](https://github.com/oceanobservatories/mi-instrument/tree/master/mi/dataset/driver) 
and [mi/dataset/parser](https://github.com/oceanobservatories/mi-instrument/tree/master/mi/dataset/parser) respectively.
In the driver directory, there is a directory for each instrument type and series, e.g., `zplsc_c`.  Any variation to an 
existing driver should be placed in a subdirectory under the existing directory, e.g., for the `zplsc_c_dcl` driver create 
a directory under `zplsc_c/dcl`.

* Copy the driver of the IDD that was used as the template and place it to the new driver directory. There is a driver for 
each stream type, i.e., telemetered and recovered. Rename the copied driver to reflect the instrument and stream type, 
e.g., `zplsc_c_recovered_driver.py`.
* In the new directory, create a resource directory and a test directory. The resource directory will contain the test raw 
data files and the YAML expected results files for the unit tests. The test directory will contain the test file for the 
driver named the same as the driver file with `test_` prepended, e.g., `test_zplsc_c_recovered_driver.py`.
* The drivers are simply a wrapper for the parser to be called from EDEX or other ingestion tool. They contain the 
references to the data particle class and the instrument parser. Typically making the reference changes should be all that 
is needed.
* The parser directory, is where all the instruments’ parsers exist. There is a directory under parser named test, which is 
where all the parsers’ unit tests exist. The parsers use the resource directory under the driver directory.

Copy the parser of the IDD that was used as the template.  Rename the copied parser to the instrument type and series as 
the driver directory is named, e.g., `zplsc_c.py`.
If the format of the existing parser’s raw data is similar to the format of the parser’s raw data being implemented, then 
the existing driver/parser’s code should be used as the template for the new driver/parser.

* The parsers share a common architecture:

> * A data particle class, e.g. `ZplscCRecoverable`, that inherits from `DataParticle`.

The data particle class overrides `_build_parsed_values` which builds the data particle from the parsed values.
`_build_parsed_values` is called indirectly during the data parsing after all the values have been parsed for a record 
and `_extract_sample` is called.

> * A parser class, e.g. `ZplscCParser`, that inherits from `SimpleParser`.

The `parse_file` method is overwritten and typically is where it iterates over the data file parsing each record.

* ASCII raw data is typically parsed using regular expressions. There are examples in many of the parsers.
* A convenient way to parse binary data is by using ctypes and structures. An example of this can be found in `zplsc_c.py`.

# Dataset Driver/Parser Unit Tests

* The YAML expect files have a specific format and can be referenced in any of the instruments’ resource directory.
* Unit tests are typically designed in the same manner:

They have a unit test case class, e.g., `ZplscCParserUnitTestCase` that inherits from `ParserUnitTestCase`.
Each test case:

* Opens the data file for that test and passes it to the parser.
* Calls parser.get_records to get the desired number of records.
* Asserts the expected number of records.
* Calls `assert_particles` to verify the contents of the particles to the contents of the YAML file for that test.

# Integration Testing

## Local Integration Testing

To test locally, you must have the following installed:
* Cassandra
* QPID
* Uframe
* Ingest_engine

Run the following to get the system running:
```bash
source ~/uframes/ooi/bin/edex-server
all start
```

From the ingest_engine directory:
```bash
./manage-streamng start
```

In a separate console, to view the consumed messages in Qpid. This will continuously output the msg/byte counts:

```bash
watch -n 1 "qpid-stat -q | egrep -i 'queue|<stream name>' "
```

Now that the system is running, you can ingest the file using the ingest utility. E.g.: 
```bash
./ingest_file.py Ingest.zplsc-c_recovered CE01ISSM-MFD37-07-ZPLSCC000 recovered 5 /omc_data/.../zplsc.raw
```

These ingest details are archived in the [ingestion-csvs](https://github.com/ooi-integration/ingestion-csvs) repository. 

* Output from `ingest_file.py` will indicate that the data file has been sent.
* The Qpid display will indicate 1 message in and 1 message out.
* Verify no exceptions in the log files found at:
> * `<uframe install dir>ooi/uframe-1.0/edex/logs/edex-ooi-YYYYMMDD.log`
* You can also run the command: `edex watch`
* Verify the data in cassandra by executing the following command:
```bash
cqlsh -k ooi -e ‘select * from <stream name>;’
```

## uframe-5-test Integration Testing:
These steps assume that you are logged into the asadev account on `uframe-5-test` and the system is currently running. Any 
development test machine is sufficient (e.g. `uframe-3-test`). 

On your development machine, `tar` the `~/uframes/ooi/uframe-1.0/edex` and the `mi-instrument` directories.

On `uframe-5-test`, stop edex and the ingest-engine:
```bash
cd ~/OOI
./edex.sh stop
cd ~/OOI/engines/ingest_engine
./manage-streamng stop
```

Set up the new edex:
* Port the edex tarball to uframe-5-test.
* Extract the edex tarball to the ~/OOI directory, renaming the folder to differentiate it from any other edex deployed 
there, e.g., `edex-rpg-2017-04-01`
* Update the softlink for edex to point to the new edex folder, e.g.,

```bash
cd ~/OOI
ln -sf edex-rpg-2017-04-01 edex
```

Set the virtual environment by entering:

```bash
source activate instruments-merged
```

Set up the new mi-instrument:
* Extract the mi-instrument tarball to the ~/src directory, renaming the folder to differentiate it from any other 
mi-instrument deployed there, e.g., mi-instrument-rpg-2017-04-01.
* Install the new mi-instrument:
```bash
cd mi-instrument-rpg-2017-04-01
pip install --upgrade .
```

On `uframe-5-test`, stop EDEX and Ingest Engine:
```bash
cd ~/OOI
./edex.sh start
cd ~/OOI/engines/ingest_engine
./manage-streamng start
```

In a separate console run the qpid tool to monitor the queues:
```bash
cd ~/OOI/tools
./qpid-stat.py
```

Ingest the instruments raw data:
```bash
cd ~/OOI/tools
```

Follow the steps above for local integration testing, starting with the file ingest. 
