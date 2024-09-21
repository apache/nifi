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

# UpdateRecord

UpdateRecord makes use of the NiFi RecordPath Domain-Specific Language (DSL) to allow the user to
indicate which field(s) in the Record should be updated. Users do this by adding a User-defined Property to the
Processor's configuration. The name of the User-defined Property must be the RecordPath text that should be evaluated
against each Record. The value of the Property specifies what value should go into that selected Record field.

When specifying the replacement value (the value of the User-defined Property), the user is able to specify a literal
value such as the number `10`; an Expression Language Expression to reference FlowFile attributes, such as`${filename}`;
or another RecordPath path from which to retrieve the desired value from the Record itself. Whether the value entered
should be interpreted as a literal or a RecordPath path is determined by the value of the <Replacement Value Strategy>
Property.

If a RecordPath is given and does not match any field in an input Record, that Property will be skipped and all other
Properties will still be evaluated. If the RecordPath matches exactly one field, that field will be updated with the
corresponding value. If multiple fields match the RecordPath, then all fields that match will be updated. If the
replacement value is itself a RecordPath that does not match, then a `null` value will be set for the field. For
instances where this is not the desired behavior, RecordPath predicates can be used to filter the fields that match so
that no fields will be selected. See RecordPath Predicates for more information.

Below, we lay out some examples in order to provide clarity about the Processor's behavior. For all the examples below,
consider the example to operate on the following set of 2 (JSON) records:

```json
[
  {
    "id": 17,
    "name": "John",
    "child": {
      "id": "1"
    },
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "Jeremy",
        "id": 4
      },
      {
        "name": "Julia",
        "id": 8
      }
    ]
  },
  {
    "id": 98,
    "name": "Jane",
    "child": {
      "id": 2
    },
    "gender": "F",
    "siblingIds": [],
    "siblings": []
  }
]
```

For brevity, we will omit the corresponding schema and configuration of the RecordReader and RecordWriter. Otherwise,
consider the following set of Properties are configured for the Processor and their associated outputs.

### Example 1 - Replace with Literal

Here, we will replace the name of each Record with the name 'Jeremy' and set the gender to 'M':

| Property Name              | Property Value |
|----------------------------|----------------|
| Replacement Value Strategy | Literal Value  |
| /name                      | Jeremy         |
| /gender                    | M              |

This will yield the following output:

```json
[
  {
    "id": 17,
    "name": "Jeremy",
    "child": {
      "id": "1"
    },
    "gender": "M",
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "Jeremy",
        "id": 4
      },
      {
        "name": "Julia",
        "id": 8
      }
    ]
  },
  {
    "id": 98,
    "name": "Jeremy",
    "child": {
      "id": 2
    },
    "gender": "M",
    "siblingIds": [],
    "siblings": []
  }
]
```

Note that even though the first record did not have a "gender" field in the input, one will be added after the "child"
field, as that's where the field is located in the schema.

### Example 2 - Replace with RecordPath

This example will replace the value in one field of the Record with the value from another field. For this example,
consider the following set of Properties:

| Property Name              | Property Value      |
|----------------------------|---------------------|
| Replacement Value Strategy | Record Path Value   |
| /name                      | /siblings\[0\]/name |

This will yield the following output:

```json
[
  {
    "id": 17,
    "name": "Jeremy",
    "child": {
      "id": "1"
    },
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "Jeremy",
        "id": 4
      },
      {
        "name": "Julia",
        "id": 8
      }
    ]
  },
  {
    "id": 98,
    "name": null,
    "child": {
      "id": 2
    },
    "gender": "F",
    "siblingIds": [],
    "siblings": []
  }
]
```

### Example 3 - Replace with Relative RecordPath

In the above example, we replaced the value of field based on another RecordPath. That RecordPath was an "absolute
RecordPath," meaning that it starts with a "slash" character (`/`) and therefore it specifies the path from the "root"
or "outermost" element. However, sometimes we want to reference a field in such a way that we defined the RecordPath
relative to the field being updated. This example does just that. For each of the siblings given in the "siblings"array,
we will replace the sibling's name with their id's. To do so, we will configure the processor with the following
properties:

| Property Name              | Property Value    |
|----------------------------|-------------------|
| Replacement Value Strategy | Record Path Value |
| /siblings\[\*\]/name       | ../id             |

Note that the RecordPath that was given for the value starts with `..`, which is a reference to the parent. We do this
because the field that we are going to update is the "name" field of the sibling. To get to the associated "id" field,
we need to go to the "name" field's parent and then to its "id" child field. The above example results in the following
output:

```json
[
  {
    "id": 17,
    "name": "Jeremy",
    "child": {
      "id": "1"
    },
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "Jeremy",
        "id": 4
      },
      {
        "name": "Julia",
        "id": 8
      }
    ]
  },
  {
    "id": 98,
    "name": null,
    "child": {
      "id": 2
    },
    "gender": "F",
    "siblingIds": [],
    "siblings": []
  }
]
```

### Example 4 - Replace Multiple Values

This example will replace the value of all fields that have the name "id", regardless of where in the Record hierarchy
the field is found. The value that it uses references the Expression Language, so for this example, let's assume that
the incoming FlowFile has an attribute named "replacement.id" that has a value of "91":

| Property Name              | Property Value    |
|----------------------------|-------------------|
| Replacement Value Strategy | Literal Value     |
| //id                       | ${replacement.id} |

This will yield the following output:

```json
[
  {
    "id": 91,
    "name": "John",
    "child": {
      "id": "91"
    },
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "Jeremy",
        "id": 91
      },
      {
        "name": "Julia",
        "id": 91
      }
    ]
  },
  {
    "id": 91,
    "name": "Jane",
    "child": {
      "id": 91
    },
    "gender": "F",
    "siblingIds": [],
    "siblings": []
  }
]
```

It is also worth noting that in this example, some of the "id" fields were of type STRING, while others were of type
INT. This is okay because the RecordReaders and RecordWriters should handle these simple type coercions for us.

### Example 5 - Use Expression Language to Modify Value

This example will capitalize the value of all 'name' fields, regardless of where in the Record hierarchy the field is
found. This is done by referencing the 'field.value' variable in the Expression Language. We can also access the
field.name variable and the field.type variable.

| Property Name              | Property Value           |
|----------------------------|--------------------------|
| Replacement Value Strategy | Literal Value            |
| //name                     | ${field.value:toUpper()} |

This will yield the following output:

```json
[
  {
    "id": 17,
    "name": "JOHN",
    "child": {
      "id": "1"
    },
    "siblingIds": [
      4,
      8
    ],
    "siblings": [
      {
        "name": "JEREMY",
        "id": 4
      },
      {
        "name": "JULIA",
        "id": 8
      }
    ]
  },
  {
    "id": 98,
    "name": "JANE",
    "child": {
      "id": 2
    },
    "gender": "F",
    "siblingIds": [],
    "siblings": []
  }
]
```