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

# PartitionRecord

PartitionRecord allows the user to separate out records in a FlowFile such that each outgoing FlowFile consists only of
records that are "alike." To define what it means for two records to be alike, the Processor makes use of
NiFi's RecordPath DSL.

In order to make the Processor valid, at least one user-defined property must be added to the Processor. The value of
the property must be a valid RecordPath. Expression Language is supported and will be evaluated before attempting to
compile the RecordPath. However, if Expression Language is used, the Processor is not able to validate the RecordPath
beforehand and may result in having FlowFiles fail processing if the RecordPath is not valid when being used.

Once one or more RecordPath's have been added, those RecordPath's are evaluated against each Record in an incoming
FlowFile. In order for Record A and Record B to be considered "like records," both of them must have the same value for
all RecordPath's that are configured. Only the values that are returned by the RecordPath are held in Java's heap. The
records themselves are written immediately to the FlowFile content. This means that for most cases, heap usage is not a
concern. However, if the RecordPath points to a large Record field that is different for each record in a FlowFile, then
heap usage may be an important consideration. In such cases, SplitRecord may be useful to split a large FlowFile into
smaller FlowFiles before partitioning.

Once a FlowFile has been written, we know that all the Records within that FlowFile have the same value for the fields
that are described by the configured RecordPath's. As a result, this means that we can promote those values to FlowFile
Attributes. We do so by looking at the name of the property to which each RecordPath belongs. For example, if we have a
property named `country` with a value of `/geo/country/name`, then each outbound FlowFile will have an attribute named
`country` with the value of the `/geo/country/name` field. The addition of these attributes makes it very easy to
perform tasks such as routing, or referencing the value in another Processor that can be used for configuring where to
send the data, etc. However, for any RecordPath whose value is not a scalar value (i.e., the value is of type Array,
Map, or Record), no attribute will be added.

## Examples

To better understand how this Processor works, we will lay out a few examples. For the sake of these examples, let's
assume that our input data is JSON formatted and looks like this:

```json
[
  {
    "name": "John Doe",
    "dob": "11/30/1976",
    "favorites": [
      "spaghetti",
      "basketball",
      "blue"
    ],
    "locations": {
      "home": {
        "number": 123,
        "street": "My Street",
        "city": "New York",
        "state": "NY",
        "country": "US"
      },
      "work": {
        "number": 321,
        "street": "Your Street",
        "city": "New York",
        "state": "NY",
        "country": "US"
      }
    }
  },
  {
    "name": "Jane Doe",
    "dob": "10/04/1979",
    "favorites": [
      "spaghetti",
      "football",
      "red"
    ],
    "locations": {
      "home": {
        "number": 123,
        "street": "My Street",
        "city": "New York",
        "state": "NY",
        "country": "US"
      },
      "work": {
        "number": 456,
        "street": "Our Street",
        "city": "New York",
        "state": "NY",
        "country": "US"
      }
    }
  },
  {
    "name": "Jacob Doe",
    "dob": "04/02/2012",
    "favorites": [
      "chocolate",
      "running",
      "yellow"
    ],
    "locations": {
      "home": {
        "number": 123,
        "street": "My Street",
        "city": "New York",
        "state": "NY",
        "country": "US"
      },
      "work": null
    }
  },
  {
    "name": "Janet Doe",
    "dob": "02/14/2007",
    "favorites": [
      "spaghetti",
      "reading",
      "white"
    ],
    "locations": {
      "home": {
        "number": 1111,
        "street": "Far Away",
        "city": "San Francisco",
        "state": "CA",
        "country": "US"
      },
      "work": null
    }
  }
]
```

### Example 1 - Partition By Simple Field

For a simple case, let's partition all the records based on the state that they live in. We can add a property named
`state` with a value of `/locations/home/state`. The result will be that we will have two outbound FlowFiles. The first
will contain an attribute with the name `state` and a value of `NY`. This FlowFile will consist of 3 records: John Doe,
Jane Doe, and Jacob Doe. The second FlowFile will consist of a single record for Janet Doe and will contain an attribute
named `state` that has a value of `CA`.

### Example 2 - Partition By Nullable Value

In the above example, there are three different values for the work location. If we use a RecordPath of
`/locations/work/state` with a property name of `state`, then we will end up with two different FlowFiles. The first
will contain records for John Doe and Jane Doe because they have the same value for the given RecordPath. This FlowFile
will have an attribute named `state` with a value of `NY`.

The second FlowFile will contain the two records for Jacob Doe and Janet Doe, because the RecordPath will evaluate to
`null` for both of them. This FlowFile will have no `state` attribute (unless such an attribute existed on the incoming
FlowFile, in which case its value will be unaltered).

### Example 3 - Partition By Multiple Values

Now let's say that we want to partition records based on multiple different fields. We now add two properties to the
PartitionRecord processor. The first property is named `home` and has a value of `/locations/home`. The second property
is named `favorite.food` and has a value of `/favorites[0]` to reference the first element in the "favorites" array.

This will result in three different FlowFiles being created. The first FlowFile will contain records for John Doe and
Jane Doe. It will contain an attribute named "favorite.food" with a value of "spaghetti." However, because the second
RecordPath pointed to a Record field, no "home" attribute will be added. In this case, both of these records have the
same value for both the first element of the "favorites" array and the same value for the home address. Janet Doe has
the same value for the first element in the "favorites" array but has a different home address. Similarly, Jacob Doe has
the same home address but a different value for the favorite food.

The second FlowFile will consist of a single record: Jacob Doe. This FlowFile will have an attribute named "
favorite.food" with a value of "chocolate." The third FlowFile will consist of a single record: Janet Doe. This FlowFile
will have an attribute named "favorite.food" with a value of "spaghetti."