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

# XMLRecordSetWriter

The XMLRecordSetWriter Controller Service writes record objects to XML. The Controller Service must be configured with a
schema that describes the structure of the record objects. Multiple records are wrapped by a root node. The name of the
root node can be configured via property. If no root node is configured, the writer expects only one record for each
FlowFile (that will not be wrapped). As Avro does not support defining attributes for records, this writer currently
does not support writing XML attributes.

## Example: Simple records

```
RecordSet (
  Record (
    Field "name1" = "value1",
    Field "name2" = 42
  ),
  Record (
    Field "name1" = "value2",
    Field "name2" = 84
  )
)
```

This record can be described by the following schema:

```json
{
  "name": "test",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "name1",
      "type": "string"
    },
    {
      "name": "name2",
      "type": "int"
    }
  ]
}
```

Assuming that "root\_name" has been configured as the name for the root node and "record\_name" has been configured as
the name for the record nodes, the writer will write the following XML:

```xml
<root_name>
    <record_name>
        <name1>value1</name1>
        <name2>42</name2>
    </record_name>
    <record_name>
        <name1>value2</name1>
        <name2>84</name2>
    </record_name>
</root_name>
```

The writer furthermore can be configured how to treat null or missing values in records:

```
RecordSet (
  Record (
    Field "name1" = "value1",
    Field "name2" = null
  ),
  Record (
    Field "name1" = "value2",
  )
)
```

If the writer is configured always to suppress missing or null values, only the field of name "name1" will appear in the
XML. If the writer ist configured only to suppress missing values, the field of name "name2" will appear in the XML as a
node without content for the first record. If the writer is configured never to suppress anything, the field of name "
name2" will appear in the XML as a node without content for both records.

## Example: Arrays

The writer furthermore can be configured how to write arrays:

```
RecordSet (
  Record (
    Field "name1" = "value1",
    Field "array_field" = [ 1, 2, 3 ]
  )
)
```

This record can be described by the following schema:

```json
{
  "name": "test",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "array_field",
      "type": {
        "type": "array",
        "items": int
      }
    },
    {
      "name": "name1",
      "type": "string"
    }
  ]
}
```

If the writer is configured not to wrap arrays, it will transform the record to the following XML:

```xml
<root_name>
    <record_name>
        <name1>value1</name1>
        <array_field>1</array_field>
        <array_field>2</array_field>
        <array_field>3</array_field>
    </record_name>
</root_name>
```

If the writer is configured to wrap arrays using the field name as wrapper and "elem" as tag name for element nodes, it
will transform the record to the following XML:

```xml
<root_name>
    <record_name>
        <name1>value1</field2>
        <array_field>
            <elem>1</elem>
            <elem>2</elem>
            <elem>3</elem>
        </array_field>
    </record_name>
</root_name>
```

If the writer is configured to wrap arrays using "wrap" as wrapper and the field name as tag name for element nodes, it
will transform the record to the following XML:

```xml

<root_name>
    <record_name>
        <name1>value1</field2>
        <wrap>
            <array_field>1</array_field>
            <array_field>2</array_field>
            <array_field>3</array_field>
        </wrap>
    </record_name>
</root_name>
```