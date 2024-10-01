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

# YamlTreeReader

The YamlTreeReader Controller Service reads a YAML Object and creates a Record object either for the entire YAML Object
tree or a subpart (see "Starting Field Strategies" section). The Controller Service must be configured with a Schema
that describes the structure of the YAML data. If any field exists in the YAML that is not in the schema, that field
will be skipped. If the schema contains a field for which no YAML field exists, a null value will be used in the
Record (or the default value defined in the schema, if applicable).

If the root element of the YAML is a YAML Array, each YAML Object within that array will be treated as its own separate
Record. If the root element is a YAML Object, the YAML will all be treated as a single Record.

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
  the corresponding property (Date Format, Time Format, Timestamp Format property). If no value is specified, then the
  value will be converted into a String representation of the number of milliseconds since epoch (Midnight GMT, January
  1, 1970).

If none of the above rules apply when attempting to coerce a value from one data type to another, the coercion will fail
and an Exception will be thrown.

## Schema Inference

While NiFi's Record API does require that each Record have a schema, it is often convenient to infer the schema based on
the values in the data, rather than having to manually create a schema. This is accomplished by selecting a value of "
Infer Schema" for the "Schema Access Strategy" property. When using this strategy, the Reader will determine the schema
by first parsing all data in the FlowFile, keeping track of all fields that it has encountered and the type of each
field. Once all data has been parsed, a schema is formed that encompasses all fields that have been encountered.

A common concern when inferring schemas is how to handle the condition of two values that have different types. For
example, consider a FlowFile with the following two records:

```json
[
  {
    "name": "John",
    "age": 8,
    "values": "N/A"
  },
  {
    "name": "Jane",
    "age": "Ten",
    "values": [
      8,
      "Ten"
    ]
  }
]
```

It is clear that the "name" field will be inferred as a STRING type. However, how should we handle the "age" field?
Should the field be an CHOICE between INT and STRING? Should we prefer LONG over INT? Should we just use a STRING?
Should the field be considered nullable?

To help understand how this Record Reader infers schemas, we have the following list of rules that are followed in the
inference logic:

* All fields are inferred to be nullable.
* When two values are encountered for the same field in two different records (or two values are encountered for an
  ARRAY type), the inference engine prefers to use a "wider" data type over using a CHOICE data type. A data type "A" is
  said to be wider than data type "B" if and only if data type "A" encompasses all values of "B" in addition to other
  values. For example, the LONG type is wider than the INT type but not wider than the BOOLEAN type (and BOOLEAN is also
  not wider than LONG). INT is wider than SHORT. The STRING type is considered wider than all other types except MAP,
  RECORD, ARRAY, and CHOICE.
* If two values are encountered for the same field in two different records (or two values are encountered for an ARRAY
  type), but neither value is of a type that is wider than the other, then a CHOICE type is used. In the example above,
  the "values" field will be inferred as a CHOICE between a STRING or an ARRRAY<STRING>.
* If the "Time Format," "Timestamp Format," or "Date Format" properties are configured, any value that would otherwise
  be considered a STRING type is first checked against the configured formats to see if it matches any of them. If the
  value matches the Timestamp Format, the value is considered a Timestamp field. If it matches the Date Format, it is
  considered a Date field. If it matches the Time Format, it is considered a Time field. In the unlikely event that the
  value matches more than one of the configured formats, they will be matched in the order: Timestamp, Date, Time. I.e.,
  if a value matched both the Timestamp Format and the Date Format, the type that is inferred will be Timestamp. Because
  parsing dates and times can be expensive, it is advisable not to configure these formats if dates, times, and
  timestamps are not expected, or if processing the data as a STRING is acceptable. For use cases when this is
  important, though, the inference engine is intelligent enough to optimize the parsing by first checking several very
  cheap conditions. For example, the string's length is examined to see if it is too long or too short to match the
  pattern. This results in far more efficient processing than would result if attempting to parse each string value as a
  timestamp.
* The MAP type is never inferred. Instead, the RECORD type is used.
* If a field exists but all values are null, then the field is inferred to be of type STRING.

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

## Starting Field Strategies

When using YamlTreeReader, two different starting field strategies can be selected. With the default Root Node strategy,
the YamlTreeReader begins processing from the root element of the YAML and creates a Record object for the entire YAML
Object tree, while the Nested Field strategy defines a nested field from which to begin processing.

Using the Nested Field strategy, a schema corresponding to the nested YAML part should be specified. In case of schema
inference, the YamlTreeReader will automatically infer a schema from nested records.

### Root Node Strategy

Consider the following YAML is read with the default Root Node strategy:

```yml
- id: 17
  name: John
  child:
    id: "1"
  dob: 10-29-1982
  siblings:
    - name: Jeremy
      id: 4
    - name: Julia
      id: 8
- id: 98
  name: Jane
  child:
    id: 2
  dob: 08-30-1984
  gender: F
  siblingIds: []
  siblings: []
```

Also, consider that the schema that is configured for this YAML is as follows (assuming that the AvroSchemaRegistry
Controller Service is chosen to denote the Schema):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": [
        "int",
        "null"
      ]
    },
    {
      "name": "name",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "child",
      "type": [
        {
          "type": "record",
          "name": "childType",
          "fields": [
            {
              "name": "id",
              "type": [
                "int",
                "string",
                "null"
              ]
            }
          ]
        },
        "null"
      ]
    },
    {
      "name": "dob",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "siblings",
      "type": [
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "siblingsType",
            "fields": [
              {
                "name": "name",
                "type": [
                  "string",
                  "null"
                ]
              },
              {
                "name": "id",
                "type": [
                  "int",
                  "null"
                ]
              }
            ]
          }
        },
        "null"
      ]
    },
    {
      "name": "gender",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "siblingIds",
      "type": [
        {
          "type": "array",
          "items": "string"
        },
        "null"
      ]
    }
  ]
}
```

Let us also assume that this Controller Service is configured with the "Date Format" property set to "MM-dd-yyyy", as
this matches the date format used for our YAML data. This will result in the YAML creating two separate records, because
the root element is a YAML array with two elements.

The first Record will consist of the following values:

Field NameField Valueid17nameJohngender_null_dob11-30-1983siblings_array with two elements, each of which is itself a
Record:_

| Field Name | Field Value |
|------------|-------------|
| name       | Jeremy      |

_and:_

| Field Name | Field Value |
|------------|-------------|
| name       | Julia       |

The second Record will consist of the following values:

| Field Name | Field Value   |
|------------|---------------|
| id         | 98            |
| name       | Jane          |
| gender     | F             |
| dob        | 08-30-1984    |
| siblings   | _empty array_ |

### Nested Field Strategy

Using the Nested Field strategy, consider the same YAML where the specified Starting Field Name is "siblings". The
schema that is configured for this YAML is as follows:

```json
{
  "namespace": "nifi",
  "name": "siblings",
  "type": "record",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "id",
      "type": "int"
    }
  ]
}
```

The first Record will consist of the following values:

| Field Name | Field Value |
|------------|-------------|
| name       | Jeremy      |
| id         | 4           |

The second Record will consist of the following values:

| Field Name | Field Value |
|------------|-------------|
| name       | Julia       |
| id         | 8           |

## Schema Application Strategies

When using YamlTreeReader with "Nested Field Strategy" and the "Schema Access Strategy" is not "Infer Schema", it can be
configured for the entire original YAML ("Whole document" strategy) or for the nested field section ("Selected part"
strategy).