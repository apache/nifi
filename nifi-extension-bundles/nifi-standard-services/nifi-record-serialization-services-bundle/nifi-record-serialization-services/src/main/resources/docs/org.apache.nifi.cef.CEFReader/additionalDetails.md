<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# CEFReader

The CEFReader Controller Service serves as a mean to read and interpret CEF messages.

The reader supports CEF Version 23. The expected format and the extension fields known by the Extension Dictionary are
defined by the description of the ArcSight Common Event Format. The reader allows to work with Syslog prefixes and
custom extensions. A couple of CEF message examples the reader can work with:

```
CEF:0|Company|Product|1.2.3|audit-login|Successful login|3| 
Oct 12 04:16:11 localhost CEF:0|Company|Product|1.2.3|audit-login|Successful login|3| 
Oct 12 04:16:11 localhost CEF:0|Company|Product|1.2.3|audit-login|Successful login|3|cn1Label=userid spt=46117 cn1=99999 cfp1=1.23  dst=127.0.0.1 c6a1=2345:0425:2CA1:0000:0000:0567:5673:23b5 dmac=00:0D:60:AF:1B:61 start=1479152665000 end=Jan 12 2017 12:23:45 dlat=456.789 loginsequence=123
```

### Raw message

It is possible to preserve the original message in the produced record. This comes in handy when the message contains a
Syslog prefix which is not part of the Record instance. In order to preserve the raw message, the "Raw Message Field"
property must be set. The reader will use the value of this property as field name and will add the raw message as
custom extension field. The value of the "Raw Message Field" must differ from the header fields and the extension fields
known by the CEF Extension Dictionary. If the property is empty, the raw message will not be added.

When using predefined schema, the field defined by the "Raw Message Field" must appear in it as a STRING record field.
In case of the schema is inferred, the field will be automatically added as an additional custom extension, regardless
of the Inference Strategy.

## Schemas and Type Coercion

When a record is parsed from incoming data, it is separated into fields. Each of these fields is then looked up against
the configured schema (by field name) in order to determine what the type of the data should be. If the field is not
present in the schema, that field is omitted from the Record. If the field is found in the schema, the data type of the
received data is compared against the data type specified in the schema. If the types match, the value of that field is
used as-is. If the schema indicates that the field should be of a different type, then the Controller Service will
attempt to coerce the data into the type specified by the schema. If the field cannot be coerced into the specified
type, an Exception will be thrown.

The following rules apply when attempting to coerce a field value from one data type to another:

* Any data type can be coerced into a String type.
* Any numeric data type (Byte, Short, Int, Long, Float, Double) can be coerced into any other numeric data type.
* Any numeric value can be coerced into a Date, Time, or Timestamp type, by assuming that the Long value is the number
  of milliseconds since epoch (Midnight GMT, January 1, 1970).
* A String value can be coerced into a Date, Time, or Timestamp type, if its format matches the configured "Date
  Format," "Time Format," or "Timestamp Format."
* A String value can be coerced into a numeric value if the value is of the appropriate type. For example, the String
  value `8` can be coerced into any numeric type. However, the String value `8.2` can be coerced into a Double or Float
  type but not an Integer.
* A String value of "true" or "false" (regardless of case) can be coerced into a Boolean value.
* A String value that is not empty can be coerced into a Char type. If the String contains more than 1 character, the
  first character is used and the rest of the characters are ignored.
* Any "date/time" type (Date, Time, Timestamp) can be coerced into any other "date/time" type.
* Any "date/time" type can be coerced into a Long type, representing the number of milliseconds since epoch (Midnight
  GMT, January 1, 1970).
* Any "date/time" type can be coerced into a String. The format of the String is whatever DateFormat is configured for
  the corresponding property (Date Format, Time Format, Timestamp Format property).

If none of the above rules apply when attempting to coerce a value from one data type to another, the coercion will fail
and an Exception will be thrown.

## Schema inference

While NiFi's Record API does require that each Record have a schema, it is often convenient to infer the schema based on
the values in the data, rather than having to manually create a schema. This is accomplished by selecting a value of "
Infer Schema" for the "Schema Access Strategy" property. When using this strategy, the Reader will determine the schema
by first parsing all data in the FlowFile, keeping track of all fields that it has encountered and the type of each
field. Once all data has been parsed, a schema is formed that encompasses all fields that have been encountered.

A common concern when inferring schemas is how to handle the condition of two values that have different types. For
example, a custom extension field might have a Float value in one record and String in another. In these cases, the
inferred will contain a CHOICE data type with FLOAT and STRING options. Records will be allowed to have either value for
the particular field.

CEF format comes with specification not only to the message format but also has directives for the content. Because of
this, the data type of some fields are not determined by the actual value(s) in the FlowFile but by the CEF format. This
includes header fields, which always have to appear and comply to the data types defined in the CEF format. Also,
extension fields from the Extension Dictionary might or might not appear in the generated schema based on the FlowFile
content but in case an extension field is added its data type is bound by the CEF format. Custom extensions have no
similar restrictions, their presence in the schema is completely depending on the FlowFile content.

Schema inference in CEFReader supports multiple possible strategies for convenience. These strategies determine which
fields should be included to the schema from the incoming CEF messages. With this, one might filter out custom
extensions for example. It is important to mention that this will have serious effect on every further steps in the
record procession. For example using an Inference Strategy which omits fields together with ConvertRecord Processor will
result Records with only the part of the original fields.

### Headers only

Using this strategy will result a schema which contains only the header fields from the incoming message. All other
fields (standard of custom extensions) will be ignored. The type of these fields are defined by the CEF format and
regardless of the content of the message used as a template, their data type is also defined by the format.

### Headers and extensions

Additionally to the header fields, this strategy will include standard extensions from the messages in the FlowFile.
This means, not all standard extensions will be part of the outgoing Record but the ones the Schema Inference found in
the incoming messages. The data type of these Record fields are determined by the CEF format, ignoring the actual value
in the observed field.

### With custom extensions inferred

While the type of the header and standard extension fields are bound by the CEF format, it is possible to add further
fields to the message called "custom extensions". These fields are not part of the "Extension Dictionary", thus their
data type is not predefined. Using "With custom extensions inferred" Inference Strategy, the CEFReader tries to
determine the possible data type for these custom extension fields based on their value.

### With custom extensions as strings

In some cases it is undesirable to let the Reader determine the type of the custom extensions. For convenience CEFReader
provides an Inference Strategy which regardless of their value, consider custom extension fields as String data.
Otherwise this strategy behaves like the "With custom extensions inferred".

## Caching of Inferred Schemas

This Record Reader requires that if a schema is to be inferred, that all records be read in order to ensure that the
schema that gets inferred is applicable for all records in the FlowFile. However, this can become expensive, especially
if the data undergoes many different transformations. To alleviate the cost of inferring schemas, the Record Reader can
be configured with a "Schema Inference Cache" by populating the property with that name. This is a Controller Service
that can be shared by Record Readers and Record Writers.

Whenever a Record Writer is used to write data, if it is configured with a "Schema Cache," it will also add the schema
to the Schema Cache. This will result in an identifier for that schema being added as an attribute to the FlowFile.

Whenever a Record Reader is used to read data, if it is configured with a "Schema Inference Cache", it will first look
for a "schema.cache.identifier" attribute on the FlowFile. If the attribute exists, it will use the value of that
attribute to lookup the schema in the schema cache. If it is able to find a schema in the cache with that identifier,
then it will use that schema instead of reading, parsing, and analyzing the data to infer the schema. If the attribute
is not available on the FlowFile, or if the attribute is available but the cache does not have a schema with that
identifier, then the Record Reader will proceed to infer the schema as described above.

The end result is that users are able to chain together many different Processors to operate on Record-oriented data.
Typically, only the first such Processor in the chain will incur the "penalty" of inferring the schema. For all other
Processors in the chain, the Record Reader is able to simply lookup the schema in the Schema Cache by identifier. This
allows the Record Reader to infer a schema accurately, since it is inferred based on all data in the FlowFile, and still
allows this to happen efficiently since the schema will typically only be inferred once, regardless of how many
Processors handle the data.

## Handling invalid events

An event is considered invalid if it is malformed in a way that the underlying CEF parser cannot read it properly.
CEFReader has two ways to deal with malformed events determined by the usage of the property "Invalid Field". If the
property is not set, the reading will fail at the time of reading the first invalid event. If the property is set, a
product of the read will be a record with single field. The field is named based on the property and the value of the
field will be the original event text. By default, the "Invalid Field" property is not set.

When the "Invalid Field" property is set, the read records might contain both records representing well formed CEF
events and malformed ones as well. As of this, further steps might be needed in order to separate these before further
processing.