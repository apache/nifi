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

# ProtobufReader

The ProtobufReader Controller Service reads and parses a Protocol Buffers Message from binary format and creates a
Record object. The Controller Service must be configured with the same '.proto' file that was used for the Message
encoding, and the fully qualified name of the Message type including its package (e.g. mypackage.MyMessage). The Reader
will always generate one record from the input data which represents the provided Protocol Buffers Message type. Further
information about Protocol Buffers can be found here: [protobuf.dev](https://protobuf.dev/)

## Data Type Mapping

When a record is parsed from incoming data, the Controller Service is going to map the Proto Message field types to the
corresponding NiFi data types. The mapping between the provided Message fields and the encoded input is always based on
the field tag numbers. When a field is defined as 'repeated' then it's data type will be an array with data type of it's
originally specified type. The following tables show which proto field type will correspond to which NiFi field type
after the conversion.

### Scalar Value Types

| Proto Type | Proto Wire Type  | NiFi Data Type |
|------------|------------------|----------------|
| double     | fixed64          | double         |
| float      | fixed32          | float          |
| int32      | varint           | int            |
| int64      | varint           | long           |
| uint32     | varint           | long           |
| uint64     | varint           | bigint         |
| sint32     | varint           | long           |
| sint64     | varint           | long           |
| fixed32    | fixed32          | long           |
| fixed64    | fixed64          | bigint         |
| sfixed32   | varint           | int            |
| sfixed64   | varint           | long           |
| bool       | varint           | boolean        |
| string     | length delimited | string         |
| bytes      | length delimited | array\[byte\]  |

### Composite Value Types

| Proto Type | Proto Wire Type  | NiFi Data Type |
|------------|------------------|----------------|
| message    | length delimited | record         |
| enum       | varint           | enum           |
| map        | length delimited | map            |
| oneof      | \-               | choice         |

## Schemas and Type Coercion

When a record is parsed from incoming data, it is separated into fields. Each of these fields is then looked up against
the configured schema (by field name) in order to determine what the type of the data should be. If the field is not
present in the schema, that field will be stored in the Record's value list on its original type. If the field is found
in the schema, the data type of the received data is compared against the data type specified in the schema. If the
types match, the value of that field is used as-is. If the schema indicates that the field should be of a different
type, then the Controller Service will attempt to coerce the data into the type specified by the schema. If the field
cannot be coerced into the specified type, an Exception will be thrown.

The following rules apply when attempting to coerce a field value from one data type to another:

* Any data type can be coerced into a String type.
* Any numeric data type (Int, Long, Float, Double) can be coerced into any other numeric data type.
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

If none of the above rules apply when attempting to coerce a value from one data type to another, the coercion will fail
and an Exception will be thrown.

## Schema Access Strategy

Beside the common Schema Access strategies like getting the schema from property value or accessing it from Schema
Registry, the ProtobufReader Controller Service offers another access strategy option called "Generate from Proto file".
When using this strategy, the Reader will generate the Record Schema from the provided '.proto' file and Message type.
This is a recommended strategy when the user doesn't want to manually create the schema or when no type coercion is
needed.

## Protobuf Any Field Type

Protocol Buffers offers further Message types called Well-Known Types. These are additionally provided messages that
defines complex structured types and wrappers for scalar types. The Any type is one of these Well-Known Types which is
used to store an arbitrary serialized Message along with a URL that describes the type of the serialized Message. Since
the Message type and the embedded Message will be available only when the Any Message is already populated with data,
the ProtobufReader needs to do this Message processing at data conversion time. The Reader is capable to generate schema
for the embedded Message in the Any field and replace it in the result Record schema.

### Example

There is a Message called 'TestMessage' which has only one field that is an Any typed field. There is another Message
called 'NestedMessage' that we would like to add as serialized Message in the value of 'anyField'.

```
message Any {
    string type_url = 1;
    bytes value = 2; 
}

message TestMessage {
    google.protobuf.Any anyField = 3; 
}

message NestedMessage {
    string field_1 = 1;
    string field_2 = 2;     
    string field_3 = 3;
}
```
``

With normal data conversion our result would look like this:

```json
{
  anyField: {
    type_url: "type.googleapis.com/NestedMessage"
    value: [
      84,
      101,
      115,
      116,
      32,
      98,
      121,
      116,
      101,
      115
    ]
  }
}
```

Result after the Protobuf Reader replaces the Any Message's fields with the processed embedded Message:

```json
{
  anyField: {
    field_1: "value 1",
    field_2: "value 2",
    field_3: "value 3"
  }
}
```