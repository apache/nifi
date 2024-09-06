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

# PutDynamoDBRecord

## Description

_PutDynamoDBRecord_ intends to provide the capability to insert multiple Items into a DynamoDB table from a
record-oriented FlowFile. Compared to the _PutDynamoDB_, this processor is capable to process data based other than JSON
format too and prepared to add multiple fields for a given Item. Also, _PutDynamoDBRecord_ is designed to insert bigger
batches of data into the database.

## Data types

The list data types supported by DynamoDB does not fully overlap with the capabilities of the Record data structure.
Some conversions and simplifications are necessary during inserting the data. These are:

* Numeric values are stored using a floating-point data structure within Items. In some cases this representation might
  cause issues with the accuracy.
* Char is not a supported type within DynamoDB, these fields are converted into String values.
* Enum types are stored as String fields, using the name of the given enum.
* DynamoDB stores time and date related information as Strings.
* Internal record structures are converted into maps.
* Choice is not a supported data type, regardless of the actual wrapped data type, values enveloped in Choice are
  handled as Strings.
* Unknown data types are handled as stings.

## Limitations

Working with DynamoDB when batch inserting comes with two inherit limitations. First, the number of inserted Items is
limited to 25 in any case. In order to overcome this, during one execution, depending on the number of records in the
incoming FlowFile, _PutDynamoDBRecord_ might attempt multiple insert calls towards the database server. Using this
approach, the flow does not have to work with this limitation in most cases.

Having multiple external actions comes with the risk of having an unforeseen result at one of the steps. For example
when the incoming FlowFile is consists of 70 records, it will be split into 3 chunks, with a single insert operation for
every chunk. The first two chunks contains 25 Items to insert per chunk, and the third contains the remaining 20. In
some cases it might occur that the first two insert operation succeeds but the third one fails. In these cases we
consider the FlowFile "partially processed" and we will transfer it to the "failure" or "unprocessed" Relationship
according to the nature of the issue. In order to keep the information about the successfully processed chunks the
processor assigns the _"dynamodb.chunks.processed"_ attribute to the FlowFile, which has the number of successfully
processed chunks as value.

The most common reason for this behaviour comes from the other limitation the inserts have with DynamoDB: the database
has a build in supervision over the amount of inserted data. When a client reaches the "throughput limit", the server
refuses to process the insert request until a certain amount of time. More
information [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html).
From the perspective of the _PutDynamoDBRecord_ we consider these cases as temporary issues and the FlowFile will be
transferred to the "unprocessed" Relationship after which the processor will yield in order to avoid further throughput
issues. (Other kinds of failures will result transfer to the "failure" Relationship)

## Retry

It is suggested to loop back the "unprocessed" Relationship to the _PutDynamoDBRecord_ in some way. FlowFiles
transferred to that relationship considered as healthy ones might be successfully processed in a later point. It is
possible that the FlowFile contains such a high number of records, what needs more than two attempts to fully insert.
The attribute "dynamodb.chunks.processed" is "rolled" through the attempts, which means, after each trigger it will
contain the sum number of inserted chunks making it possible for the later attempts to continue from the right point
without duplicated inserts.

## Partition and sort keys

The processor supports multiple strategies for assigning partition key and sort key to the inserted Items. These are:

### Partition Key Strategies

#### Partition By Field

The processors assign one of the record fields as partition key. The name of the record field is specified by the "
Partition Key Field" property and the value will be the value of the record field with the same name.

#### Partition By Attribute

The processor assigns the value of a FlowFile attribute as partition key. With this strategy all the Items within a
FlowFile will share the same partition key value, and it is suggested to use for tables also having a sort key in order
to meet the primary key requirements of the DynamoDB. The property "Partition Key Field" defines the name of the Item
field and the property "Partition Key Attribute" will specify which attribute's value will be assigned to the partition
key. With this strategy the "Partition Key Field" must be different from the fields consisted by the incoming records.

#### Generated UUID

By using this strategy the processor will generate a UUID identifier for every single Item. This identifier will be used
as value for the partition key. The name of the field used as partition key is defined by the property "Partition Key
Field". With this strategy the "Partition Key Field" must be different from the fields consisted by the incoming
records. When using this strategy, the partition key in the DynamoDB table must have String data type.

### Sort Key Strategies

#### None

No sort key will be assigned to the Item. In case of the table definition expects it, using this strategy will result
unsuccessful inserts.

#### Sort By Field

The processors assign one of the record fields as sort key. The name of the record field is specified by the "Sort Key
Field" property and the value will be the value of the record field with the same name. With this strategy the "Sort Key
Field" must be different from the fields consisted by the incoming records.

#### Generate Sequence

The processor assigns a generated value to every Item based on the original record's position in the incoming FlowFile (
regardless of the chunks). The first Item will have the sort key 1, the second will have sort key 2 and so on. The
generated keys are unique within a given FlowFile. The name of the record field is specified by the "Sort Key Field"
attribute. With this strategy the "Sort Key Field" must be different from the fields consisted by the incoming records.
When using this strategy, the sort key in the DynamoDB table must have Number data type.

## Examples

### Using fields as partition and sort key

#### Setup

* Partition Key Strategy: Partition By Field
* Partition Key Field: class
* Sort Key Strategy: Sort By Field
* Sort Key Field: size

Note: both fields have to exist in the incoming records!

#### Result

Using this pair of strategies will result Items identical to the incoming record (not counting the representational
changes from the conversion). The field specified by the properties are added to the Items normally with the only
difference of flagged as (primary) key items.

#### Input

```json
[
  {
    "type": "A",
    "subtype": 4,
    "class": "t",
    "size": 1
  }
]
```

#### Output (stylized)

* type: String field with value "A"
* subtype: Number field with value 4
* class: String field with value "t" and serving as partition key
* size: Number field with value 1 and serving as sort key

### Using FlowFile filename as partition key with generated sort key

#### Setup

* Partition Key Strategy: Partition By Attribute
* Partition Key Field: source
* Partition Key Attribute: filename
* Sort Key Strategy: Generate Sequence
* Sort Key Field: sort

#### Result

The FlowFile's filename attribute will be used as partition key. In this case all the records within the same FlowFile
will share the same partition key. In order to avoid collusion, if FlowFiles contain multiple records, using sort key is
suggested. In this case a generated sequence is used which is guaranteed to be unique within a given FlowFile.

#### Input

```json
[
  {
    "type": "A",
    "subtype": 4,
    "class": "t",
    "size": 1
  },
  {
    "type": "B",
    "subtype": 5,
    "class": "m",
    "size": 2
  }
]
```

#### Output (stylized)

##### First Item

* source: String field with value "data46362.json" and serving as partition key
* type: String field with value "A"
* subtype: Number field with value 4
* class: String field with value "t"
* size: Number field with value 1
* sort: Number field with value 1 and serving as sort key

##### Second Item

* source: String field with value "data46362.json" and serving as partition key
* type: String field with value "B"
* subtype: Number field with value 5
* class: String field with value "m"
* size: Number field with value 2
* sort: Number field with value 2 and serving as sort key

### Using generated partition key

#### Setup

* Partition Key Strategy: Generated UUID
* Partition Key Field: identifier
* Sort Key Strategy: None

#### Result

A generated UUID will be used as partition key. A different UUID will be generated for every Item.

#### Input

```json
[
  {
    "type": "A",
    "subtype": 4,
    "class": "t",
    "size": 1
  }
]
```

#### Output (stylized)

* identifier: String field with value "872ab776-ed73-4d37-a04a-807f0297e06e" and serving as partition key
* type: String field with value "A"
* subtype: Number field with value 4
* class: String field with value "t"
* size: Number field with value 1