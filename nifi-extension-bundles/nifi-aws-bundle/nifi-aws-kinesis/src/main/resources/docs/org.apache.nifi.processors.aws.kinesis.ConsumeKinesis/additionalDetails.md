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

# ConsumeKinesis

## IAM permissions required for ConsumeKinesis

You must add the following permissions to the IAM role or user configured in _AWS Credentials Provider Service_.

| Service | Actions | Resources (ARNs) | Purpose | When needed |
|---------|---------|------------------|---------|-------------|
| Amazon Kinesis Data Streams | ListShards | `arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}` | Lists the shards of the data stream. | All consumer types |
| Amazon Kinesis Data Streams | GetRecords<br>GetShardIterator | `arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}` | Reads records from a shard. | Shared-throughput consumers only |
| Amazon Kinesis Data Streams | DescribeStream<br>RegisterStreamConsumer | `arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}` | Resolves the data stream ARN and registers the enhanced fan-out consumer. | Enhanced fan-out consumers only |
| Amazon Kinesis Data Streams | DescribeStreamConsumer<br>SubscribeToShard<br>DeregisterStreamConsumer | `arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/{Application Name}`<br>`arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/{Application Name}:*` | Checks whether the enhanced fan-out consumer is registered and active, and subscribes to shards. When the processor is removed, attempts to deregister a consumer. | Enhanced fan-out consumers only |
| Amazon DynamoDB | CreateTable<br>DescribeTable<br>GetItem<br>UpdateItem<br>Query | `arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}` | Creates the checkpoint table and manages shard leases and checkpoints in it. | All consumer types |
| Amazon DynamoDB | DescribeTable | `arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}_migration` | Checks for an unfinished checkpoint table migration on startup. | All consumer types |
| Amazon DynamoDB | CreateTable<br>DeleteTable<br>Scan<br>GetItem<br>PutItem<br>UpdateItem | `arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}`<br>`arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}_migration` | Copies checkpoints into a table with the current schema and removes the old and temporary tables. | Only when migrating a checkpoint table created by an earlier version of `ConsumeKinesis` |

The following example policy includes permissions for both consumer types. It includes `DescribeTable` on the migration table because the processor checks for unfinished migrations on startup, but excludes the additional permissions needed to perform a checkpoint migration.

```json
{
    "Version":"2012-10-17",		 	 	 
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:ListShards",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:DescribeStream",
                "kinesis:RegisterStreamConsumer"
            ],
            "Resource": [
                "arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStreamConsumer",
                "kinesis:SubscribeToShard",
                "kinesis:DeregisterStreamConsumer"
            ],
            "Resource": [
                "arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/{Application Name}",
                "arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/{Application Name}:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable"
            ],
            "Resource": [
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}_migration"
            ]
        }
    ]
}
```

**Note:** Replace "{Region}", "{Account}", "{Stream Name}", and "{Application Name}" in the ARNs with your own AWS region, 
AWS account number, Kinesis data stream name, and `ConsumeKinesis` _Application Name_ property respectively.

## Consumer Type

Comparison of different Consumer Types from [Amazon Kinesis Streams documentation](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html):

| Characteristics           | Shared throughput consumers without enhanced fan-out                                                                                                                                                                 | Enhanced fan-out consumers                                                                                                                                                                           |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Read throughput           | Fixed at a total of 2 MB/sec per shard. If there are multiple consumers reading from the same shard, they all share this throughput. The sum of the throughputs they receive from the shard doesn't exceed 2 MB/sec. | Scales as consumers register to use enhanced fan-out. Each consumer registered to use enhanced fan-out receives its own read throughput per shard, up to 2 MB/sec, independently of other consumers. |
| Message propagation delay | An average of around 200 ms if you have one consumer reading from the stream. This average goes up to around 1000 ms if you have five consumers.                                                                     | Typically an average of 70 ms whether you have one consumer or five consumers.                                                                                                                       |
| Cost                      | Not applicable                                                                                                                                                                                                       | There is a data retrieval cost and a consumer-shard hour cost. For more information, see [Amazon Kinesis Data Streams Pricing](https://aws.amazon.com/kinesis/data-streams/pricing/?nc=sn&loc=3).    |

## Record processing

When _Processing Strategy_ property is set to _RECORD_, _ConsumeKinesis_ operates in Record mode.
In this mode, the processor reads records from Kinesis streams using the configured _Record Reader_,
and writes them to FlowFiles using the configured _Record Writer_.

The processor tries to optimize the number of FlowFiles created by batching multiple records with the same schema
into a single FlowFile.

### Schema changes

_ConsumeKinesis_ supports dynamically changing Kinesis record schemas. When a record with new schema is encountered,
the currently open FlowFile is closed and a new FlowFile is created for the new schema. Thanks to this, the processor
preserves record ordering for a single Shard, even with record schema changes.

**Please note**, the processor relies on _Record Reader_ to provide correct schema for each record.
Using a Reader with schema inference may produce a lot of different schemas, which may lead to excessive FlowFile creation.
If performance is a concern, it is recommended to use a Reader with a predefined schema or schema registry.

### Output Strategies

This processor offers multiple strategies configured via processor property _Output Strategy_ for converting Kinesis records into FlowFiles.

- _Use Content as Value_ (the default) writes only a Kinesis record value to a FlowFile.
- _Use Wrapper_ writes a Kinesis Record value as well as metadata into separate fields of a FlowFile record.
- _Inject Metadata_ writes a Kinesis Record value to a FlowFile record and adds a sub-record to it with metadata.

The written metadata includes the following fields:
- _stream_: The name of the Kinesis stream the record was received from.
- _shardId_: The identifier of the shard the record was received from.
- _sequenceNumber_: The sequence number of the record.
- _subSequenceNumber_: The subsequence number of the record, used when multiple smaller records are aggregated into a single Kinesis record. If a record was not part of a batch, this value will be 0.
- _shardedSequenceNumber_: A combination of the sequence number and subsequence number. This can be used to uniquely identify a record within a shard.
- _partitionKey_: The partition key of the record.
- _approximateArrival_: The approximate arrival timestamp of the record (in milliseconds since epoch).

The record schema that is used when _Use Wrapper_ is selected is as follows (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "value",
      "type": [
        {
          < Fields as determined by the Record Reader for a Kinesis message >
        },
        "null"
      ]
    },
    {
      "name": "kinesisMetadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "stream", "type": ["string", "null"] },
            { "name": "shardId", "type": ["string", "null"] },
            { "name": "sequenceNumber", "type": ["string", "null"] },
            { "name": "subSequenceNumber", "type": ["long", "null"] },
            { "name": "shardedSequenceNumber", "type": ["string", "null"] },
            { "name": "partitionKey", "type": ["string", "null"] },
            { "name": "approximateArrival", "type": [ { "type": "long", "logicalType": "timestamp-millis" }, "null" ] }
          ]
        },
        "null"
      ]
    }
  ]
}
```

The record schema that is used when "Inject Metadata" is selected is as follows (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    < Fields as determined by the Record Reader for a Kinesis message >,
    {
      "name": "kinesisMetadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "stream", "type": ["string", "null"] },
            { "name": "shardId", "type": ["string", "null"] },
            { "name": "sequenceNumber", "type": ["string", "null"] },
            { "name": "subSequenceNumber", "type": ["long", "null"] },
            { "name": "shardedSequenceNumber", "type": ["string", "null"] },
            { "name": "partitionKey", "type": ["string", "null"] },
            { "name": "approximateArrival", "type": [ { "type": "long", "logicalType": "timestamp-millis" }, "null" ] }
          ]
        },
        "null"
      ]
    }
  ]
}
```

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy _Use Wrapper_ is selected:

```json
[
  {
    "value": {
      "address": "1234 First Street",
      "zip": "12345",
      "account": {
        "name": "Acme",
        "number": "AC1234"
      }
    },
    "kinesisMetadata" : {
      "stream" : "stream-name",
      "shardId" : "shardId-000000000000",
      "sequenceNumber" : "123456789",
      "subSequenceNumber" : 3,
      "shardedSequenceNumber" : "12345678900000000000000000003",
      "partitionKey" : "123",
      "approximateArrival" : 1756459596788
    }
  }
]
```

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy _Inject Metadata_ is selected:

```json
[
  {
    "address": "1234 First Street",
    "zip": "12345",
    "account": {
      "name": "Acme",
      "number": "AC1234"
    },
    "kinesisMetadata" : {
      "stream" : "stream-name",
      "shardId" : "shardId-000000000000",
      "sequenceNumber" : "123456789",
      "subSequenceNumber" : 3,
      "shardedSequenceNumber" : "12345678900000000000000000003",
      "partitionKey" : "123",
      "approximateArrival" : 1756459596788
    }
  }
]
```
