# Kite Morphlines Processor

## What is Morphlines?

Morphlines is an open sour framework, which performs in-memory container of transformation commands in oder to perform tasks such as loading, parsing, transforming, or otherwise processing a single record.
A morphline is a rich configuration file containing a set of commands that consumes any kind of data from any kind of data source, processes the data and loads the results into a Hadoop component.

## How can it be used within NiFi?

Given the wide scope of commands provided by Morphlines; the Morphlines processor can be used for multiple applications such as real time log parsing, data enrichment, data transformation and data storage (Solr). The processor also allows data enrichment from flowfiles attributes.

## What is needed to use the processor?

Two properties are required to run the processor; namely, the absolute local path of the Morphlines configuration file containing the set of commands. The Morphlines ID of the set of transformations the user wants to run.
Optionally, the user can chose what should be the output of the processor given that all the output of transformations are stored in memory. Also, the user can add properties to enrich the data: the properties can be from the flowfiles attributes or can be any hard coded values.

## What is the performance the user can expect from the processor?

The performance greatly depends on the commands that will be ran. Ballpark-wise, simple commands such as readLine or readAvro or grok can process O(100k) records per second per CPU core. A command that does almost nothing runs up to O(5 M) records/sec/core. Complex commands such as GeoIP which returns Geolocation information for a given IP address process O(10k) records/sec/core.

## Reference

Kite SDK Morphlines: http://kitesdk.org/docs/1.1.0/morphlines/
