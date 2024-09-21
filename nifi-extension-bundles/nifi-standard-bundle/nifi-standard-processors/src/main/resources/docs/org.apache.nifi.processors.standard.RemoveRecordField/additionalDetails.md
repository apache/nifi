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

# RemoveRecordField

## RemoveRecordField processor usage with examples

The RemoveRecordField processor is capable of removing fields from a NiFi record. The fields that should be removed from
the record are identified by a RecordPath expression. To learn about RecordPath, please read the RecordPath Guide.

RemoveRecordField will update all Records within the FlowFile based upon the RecordPath(s) configured for removal. The
Schema associated with the Record Reader configured to read the FlowFile content will be updated based upon the same
RecordPath(s) and considering the values remaining within the Record's Fields after removal. This updated schema can be
used for output if the Record Writer has a Schema Access Strategy of Inherit Record Schema, otherwise the schema updates
will be lost and the Records output using the Schema configured upon the Writer.

Below are some examples that are intended to explain how to use the processor. In these examples the input data, the
record schema, the output data and the output schema are all in JSON format for easy understanding. We assume that the
processor is configured to use a JsonTreeReader and JsonRecordSetWriter controller service, but of course the processor
works with other Reader and Writer controller services as well. In the examples it is also assumed that the record
schema is provided explicitly as a FlowFile attribute (avro.schema attribute), and the Reader uses this schema to work
with the FlowFile. The Writer's Schema Access Strategy is "Inherit Record Schema" so that all modifications made to the
schema by the processor are considered by the Writer. Schema Write Strategy of the Writer is set to "Set 'avro.schema'
Attribute" so that the output FlowFile contains the schema as an attribute value.

### Example 1:

**Removing a field from a simple record**

Input data:

```json
{
  "id": 1,
  "name": "John Doe",
  "dateOfBirth": "1980-01-01"
}
```

Input schema:

```json
{
  "type": "record",
  "name": "PersonRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "dateOfBirth",
      "type": "string"
    }
  ]
}
```

Field to remove:

/dateOfBirth

In this case the _dateOfBirth_ field is removed from the record as well as the schema.

Output data:

```json
{
  "id": 1,
  "name": "John Doe"
}
```

Output schema:

```json
{
  "type": "record",
  "name": "PersonRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}
```

Note, that removing a field from a record differs from setting a field's value to _null_. With RemoveRecordField a field
is completely removed from the record and its schema regardless of the field being nullable or not.

### Example 2:

**Removing fields from a complex record**

Let's suppose we have an input record that contains a _homeAddress_ and a _mailingAddress_ field both of which contain a
_zip_ field and we want to remove the _zip_ field from both of them.

Input data:

```json
{
  "id": 1,
  "name": "John Doe",
  "homeAddress": {
    "zip": 1111,
    "street": "Main",
    "number": 24
  },
  "mailingAddress": {
    "zip": 1121,
    "street": "Airport",
    "number": 12
  }
}
```

Input schema:

```json
{
  "name": "PersonRecord",
  "type": "record",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "homeAddress",
      "type": {
        "name": "address",
        "type": "record",
        "fields": [
          {
            "name": "zip",
            "type": "int"
          },
          {
            "name": "street",
            "type": "string"
          },
          {
            "name": "number",
            "type": "int"
          }
        ]
      }
    },
    {
      "name": "mailingAddress",
      "type": "address"
    }
  ]
}
```

The _zip_ field from both addresses can be removed by specifying "Field to remove 1" property on the processor as "
/homeAddress/zip" and adding a dynamic property with the value "/mailingAddress/zip". Or we can use a wildcard
expression in the RecordPath in "Field To Remove 1" (no need to specify a dynamic property).

Field to remove:

/\*/zip

The _zip_ field is removed from both addresses.

Output data:

```json
{
  "id": 1,
  "name": "John Doe",
  "homeAddress": {
    "street": "Main",
    "number": 24
  },
  "mailingAddress": {
    "street": "Airport",
    "number": 12
  }
}
```

The _zip_ field is removed from the schema of both the _homeAddress_ field and the _mailingAddress_ field. However, if
only "/homeAddress/zip" was specified to be removed, the schema of _mailingAddress_ would be intact regardless of the
fact that originally these two addresses shared the same schema.

### Example 3:

**Arrays**

Let's suppose we have an input record that contains an array of addresses.

Input data:

```json
{
  "id": 1,
  "name": "John Doe",
  "addresses": [
    {
      "zip": 1111,
      "street": "Main",
      "number": 24
    },
    {
      "zip": 1121,
      "street": "Airport",
      "number": 12
    }
  ]
}
```

Input schema:

```json
{
  "name": "PersonRecord",
  "type": "record",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "addresses",
      "type": {
        "type": "array",
        "items": {
          "name": "address",
          "type": "record",
          "fields": [
            {
              "name": "zip",
              "type": "int"
            },
            {
              "name": "street",
              "type": "string"
            },
            {
              "name": "number",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
```

* Case 1: removing one element from the array

Field to remove:

/addresses\[0\]

Output data:

```json
{
  "id": 1,
  "name": "John Doe",
  "addresses": [
    {
      "zip": 1121,
      "street": "Airport",
      "number": 12
    }
  ]
}
```

Output schema:

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "addresses",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "addressesType",
          "fields": [
            {
              "name": "zip",
              "type": "int"
            },
            {
              "name": "street",
              "type": "string"
            },
            {
              "name": "number",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
```

The first element of the array is removed. The schema of the output data is structurally the same as the input schema.
Note that the name "PersonRecord" of the input schema changed to "nifiRecord" and the name "address" changed to "
addressesType". This is normal, NiFi generates these names for the output schema. These name changes occur regardless of
the schema actually being modified or not.

* Case 2: removing all elements from the array

Field to remove:

/addresses\[\*\]

Output data:

```json
{
  "id": 1,
  "name": "John Doe",
  "addresses": []
}
```

All elements of the array are removed, the result is an empty array. The output schema is the same as in Case 1, no
structural changes made to the schema.

* Case 3: removing a field from certain elements of the array

Field to remove:

/addresses\[0\]/zip

Output data:

```json
{
  "id": 1,
  "name": "John Doe",
  "addresses": [
    {
      "zip": null,
      "street": "Main",
      "number": 24
    },
    {
      "zip": 1121,
      "street": "Airport",
      "number": 12
    }
  ]
}
```

The output schema is the same as in Case 1, no structural changes. The _zip_ field of the array's first element is set
to _null_ since the value had to be deleted but the schema could not be modified since deletion is not applied to all
elements in the array. In a case like this, the value of the field is set to _null_ regardless of the field being
nullable or not.

* Case 4: removing a field from all elements of an array

Field to remove:

/addresses\[\*\]/zip

Output data:

```json
{
  "id": 1,
  "name": "John Doe",
  "addresses": [
    {
      "street": "Main",
      "number": 24
    },
    {
      "street": "Airport",
      "number": 12
    }
  ]
}
```

Output schema:

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "addresses",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "addressesType",
          "fields": [
            {
              "name": "street",
              "type": "string"
            },
            {
              "name": "number",
              "type": "int"
            }
          ]
        }
      }
    }
  ]
}
```

The _zip_ field is removed from all elements of the array, and the schema is modified, the _zip_ field is removed from
the array's element type.

The examples shown in Case 1, Case 2, Case 3 and Case 4 apply to both kinds of collections: arrays and maps. The schema
of an array or a map is only modified if the field removal applies to all elements of the collection. Selecting all
elements of an array can be performed with the \[\*\] as well as the \[0..-1\] operator.

**Important note:** if there are e.g. 3 elements in the addresses array, and "/addresses\[\*\]/zip" is removed, then the
_zip_ field is removed from the schema because it applies explicitly for all elements regardless of the actual number of
elements in the array. However, if the path says "/addresses\[0,1,2\]/zip" then the schema is NOT modified (even though
\[0,1,2\] means all the elements in this particular array), because it selects the first, second and third elements
individually and does not express the intention to apply removal to all elements of the array regardless of the number
of elements.

* Case 5: removing multiple elements from an array

Fields to remove:

/addresses\[0\]
/addresses\[0\]

In this example we want to remove the first two elements of the array. To do that we need to specify two separate path
expressions, each pointing to one array element. Each removal is executed on the result of the previous removal, and
removals are executed in the order in which the properties containing record paths are specified on the Processor.
First, "/addresses\[0\]" is removed, that is the address with zip code 1111 in the example. After this removal, the
addresses array has a new first element, which is the second element of the original array (the address with zip code
1121). To remove this element, we need to issue "/addresses\[0\]" again. Trying to remove "/addresses\[0,1\]", or
filtering array elements with predicates when the target of the removal is multiple different array elements may produce
unexpected results.

* Case 6: array within an array

Let's suppose we have a complex input record that has an array within an array.

Input data:

```json
{
  "id": 1,
  "people": [
    {
      "id": 11,
      "addresses": [
        {
          "zip": 1111,
          "street": "Main",
          "number": 24
        },
        {
          "zip": 1121,
          "street": "Airport",
          "number": 12
        }
      ]
    },
    {
      "id": 22,
      "addresses": [
        {
          "zip": 2222,
          "street": "Ocean",
          "number": 24
        },
        {
          "zip": 2232,
          "street": "Sunset",
          "number": 12
        }
      ]
    },
    {
      "id": 33,
      "addresses": [
        {
          "zip": 3333,
          "street": "Dawn",
          "number": 24
        },
        {
          "zip": 3323,
          "street": "Spring",
          "number": 12
        }
      ]
    }
  ]
}
```

The following table summarizes what happens to the record and the schema for different RecordPaths

| Field To Remove                   | Is the schema modified? | What happens to the record?                                                             |
|-----------------------------------|-------------------------|-----------------------------------------------------------------------------------------|
| /people\[0\]/addresses\[1\]/zip   | No                      | The _zip_ field of the first person's second address is set to _null_.                  |
| /people\[\*\]/addresses\[1\]/zip  | No                      | The _zip_ field of all people's second address is set to _null_.                        |
| /people\[0\]/addresses\[\*\]/zip  | No                      | The _zip_ field of the first person's every address is set to _null_.                   |
| /people\[\*\]/addresses\[\*\]/zip | Yes                     | The _zip_ field every person's every address is removed (from the schema AND the data). |

The rules and examples shown for arrays apply for maps as well.

### Example 4:

**Choice datatype**

Let's suppose we have an input schema that contains a field of type CHOICE.

Input data:

```json
{
  "id": 12,
  "name": "John Doe"
}
```

Input schema:

```json
{
  "type": "record",
  "name": "nameRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": [
        "string",
        {
          "type": "record",
          "name": "nameType",
          "fields": [
            {
              "name": "firstName",
              "type": "string"
            },
            {
              "name": "lastName",
              "type": "string"
            }
          ]
        }
      ]
    }
  ]
}
```

In this example, the schema specifies the _name_ field as CHOICE, but in the data it is a simple string. If we remove "
/name/firstName" then there is no modifications to the data, but the schema is modified, the _firstName_ field gets
removed from the schema only.