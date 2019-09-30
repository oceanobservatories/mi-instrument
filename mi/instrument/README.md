# Updating Instrument Driver Code

## Plan Stream Updates

If any modification to stream parameters are required, they should be analyzed first, then documented in Confluence: 
https://confluence.oceanobservatories.org/display/instruments/OOI+Instruments The Confluence pages are organized by 
instrument type (access requires an account on the Confluence server). 

Some general advice:
* Adding additional parameters to an existing stream is possible. The existing streams in Cassandra will have additional 
columns appended (those columns will simply be empty). As new data are read into the system, the columns will populate
with the new data.
* Modification of derived parameters in a stream is possible. These values can be added or removed without requiring any
updates to Cassandra (or edex) as these values are not saved in Cassandra (they are computed on the fly by Stream Engine).
* Parameters names must match the names used in the driver code. Verify this is the case when changing names. 
* Changing the parameter names in a stream is not possible. This requires the entire table to be removed from Cassandra. 
If the desire is to replace a stream with a new stream (with different parameters), the following process should be followed:
> * Create a new stream with the modified parameter name(s).
> * Update the driver to use the new stream (including preload, Cassandra and EDEX updates).
> * Deprecate the old stream.
> * Install the new driver on the secondary driver machine.
> * Stop the old driver, start the new one.
> * Verify the data output for the new stream.
> * Backfill data for the new stream.
> * Verify data for backfill are correct (and match old stream).
> * Purge the old stream data.
> * Remove the old stream (if not in use by other drivers/parsers).

## Modify Stream Definition

Updates to the stream are made in a two-part process. First, updates are made to the stream and parameter definitions in 
Preload (https://github.com/oceanobservatories/preload-database/tree/master/csv). 

## Modify Driver Code

Instrument driver code is located in https://github.com/oceanobservatories/mi-instrument/tree/master/mi/instrument and is 
organized by manufacturer. 

## Verify Data Ingest

Verify the appropriate columns names have been applied to cassandra. Issue the following commands on the edex host machine:

```
cqlsh -k ooi
cqlsh> select * from parad_sa_sample limit 1;
```

The return will include each parameter as a column name with one row of data: 
```cqlsh
 subsite  | node  | sensor       | bin        | method   | time                       | deployment | id                                   | checksum | driver_timestamp           | elapsed_time            | ingestion_timestamp        | internal_timestamp | par        | port_timestamp             | preferred_timestamp | provenance                           | quality_flag | serial_number
----------+-------+--------------+------------+----------+----------------------------+------------+--------------------------------------+----------+----------------------------+-------------------------+----------------------------+--------------------+------------+----------------------------+---------------------+--------------------------------------+--------------+---------------
 RS01SBPS | SF01A | 3C-PARADA101 | 3777278400 | streamed | 3777278400.080306529998779 |          0 | 079efad1-ebde-41f0-80cf-12d45b39b6ec |      198 | 3777278400.193185806274414 | 7726123.959999999962747 | 3777278404.529000282287598 |                  0 | 2156818496 | 3777278400.080306529998779 |      port_timestamp | 894e0deb-ecfe-4a23-bfe9-7c5730872a69 |           ok |          0556
```

# Setting up Test Instruments

## Initial Setup

### Configure Test Services

* setup rabbitmq queue
* setup consul
* setup shovel

Setup Port Agent
Setup Driver


