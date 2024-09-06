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

# ValidateRecord

## Examples for the effect of Force Types From Reader's Schema property

The processor first reads the data from the incoming FlowFile using the specified Record Reader, which uses a schema.
Then, depending on the value of the Schema Access Strategy property, the processor can either use the reader's schema,
or a different schema to validate the data against. After that, the processor writes the data into the outgoing FlowFile
using the specified Record Writer. If the data is valid, the validation schema is used by the writer. If the data is
invalid, the writer uses the reader's schema. The **Force Types From Reader's Schema** property affects the first step:
how strictly the reader's schema should be applied when reading the data from the incoming FlowFile. By affecting how
the data is read, the value of the Force Types From Reader's Schema property also has an effect on what the output of
the ValidateRecord processor is, and also whether the output is forwarded to the **valid** or the **invalid**
relationship. Below are two examples where the value of this property affects the output significantly.

In both examples the input is in XML format and the output is in JSON. In the examples we assume that the same schema is
used for reading, validation and writing.

### Example 1

Schema:

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "field1",
      "type": "string"
    },
    {
      "name": "field2",
      "type": "string"
    }
  ]
}
```

Input:

```xml
<test>
    <field1>
        <sub_field>content</sub_field>
    </field1>
    <field2>content_of_field_2</field2>
</test>
```

Output if **Force Types From Reader's Schema = true** (forwarded to the **invalid** relationship):

```json
[
  {
    "field2": "content_of_field_2"
  }
]
```

Output if **Force Types From Reader's Schema = false** (forwarded to the **invalid** relationship):

```json
[
  {
    "field1": {
      "sub_field": "content"
    },
    "field2": "content_of_field_2"
  }
]
```

As you can see, the FlowFile is forwarded to the invalid relationship in both cases, since the input data does not match
the provided Avro schema. However, if **Force Types From Reader's Schema = true**, only those fields appear in the
output that comply with the schema. If **Force Types From Reader's Schema = false**, all fields appear in the output
regardless of whether they comply with the schema or not.

### Example 2

Schema:

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "field1",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "field2",
      "type": {
        "type": "array",
        "items": "string"
      }
    }
  ]
}
```

Input:

```xml
<test>
    <field1>content_1</field1>
    <field2>content_2</field2>
    <field2>content_3</field2>
</test>
```

Output if **Force Types From Reader's Schema = true** (forwarded to the **valid** relationship):

```json
[
  {
    "field1": [
      "content_1"
    ],
    "field2": [
      "content_2",
      "content_3"
    ]
  }
]
```

Output if **Force Types From Reader's Schema = false** (forwarded to the **invalid** relationship):

```json
[
  {
    "field1": "content_1",
    "field2": [
      "content_2",
      "content_3"
    ]
  }
]
```

The schema expects two fields (field1 and field2), both of type ARRAY. field1 only appears once in the input XML
document. If **Force Types From Reader's Schema = true**, the processor forces this field to be in a type that complies
with the schema. So it is put in an array with one element. Since this type coercion can be done, the output is routed
to the **valid** relationship. If **Force Types From Reader's Schema = false** the processor does not try to apply type
coercion, thus field1 appears in the output as a single value. According to the schema, the processor expects an array
for field1, but receives a single element so the output is routed to the **invalid** relationship.

Schema compliance (and getting routed to the **valid** or the **invalid** relationship) does not depend on what Writer
is used to produce the output of the ValidateRecord processor. Let us suppose that we used the same schema and input as
in **Example 2**, but instead of JsonRecordSetWriter, we used XMLRecordSetWriter to produce the output. Both in case of
**Force Types From Reader's Schema = true** and **Force Types From Reader's Schema = false** the output is:

```xml
<test>
    <field1>content_1</field1>
    <field2>content_2</field2>
    <field2>content_3</field2>
</test>
```

However, if **Force Types From Reader's Schema = true** this output is routed to the **valid** relationship and if *
*Force Types From Reader's Schema = false** it is routed to the **invalid** relationship.