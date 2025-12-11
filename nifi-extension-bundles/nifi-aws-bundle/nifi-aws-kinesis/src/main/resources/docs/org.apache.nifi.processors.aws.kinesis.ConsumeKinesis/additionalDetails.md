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

Cloudwatch permissions are needed only when _Metrics Publishing_ is set to _CloudWatch_.

| Service                     | Actions                                                                                               | Resources (ARNs)                                                                                                                                                                                                                                                             | Purpose                                                                                                                                                                                                 |
|-----------------------------|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Amazon Kinesis Data Streams | DescribeStream<br>DescribeStreamSummary<br>RegisterStreamConsumer                                     | Kinesis data stream from which `ConsumeKinesis` will process the data.<br>`arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}`                                                                                                                                          | Before attempting to read records, the consumer checks if the data stream exists, if it's active, and if the shards are contained in the data stream. Registers consumers to a shard.                   |
| Amazon Kinesis Data Streams | GetRecords<br>GetShardIterator<br>ListShards                                                          | Kinesis data stream from which `ConsumeKinesis` will process the data.<br>`arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}`                                                                                                                                          | Reads records from a shard.                                                                                                                                                                             |
| Amazon Kinesis Data Streams | SubscribeToShard<br>DescribeStreamConsumer                                                            | Kinesis data stream from which `ConsumeKinesis` will process the data. Add this action only if you use enhanced fan-out (EFO) consumers.<br>`arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/*`                                                             | Subscribes to a shard for enhanced fan-out (EFO) consumers.                                                                                                                                             |
| Amazon DynamoDB             | CreateTable<br>DescribeTable<br>UpdateTable<br>Scan<br>GetItem<br>PutItem<br>UpdateItem<br>DeleteItem | Lease table (metadata table in DynamoDB created by `ConsumeKinesis`.<br>`arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}`                                                                                                                                       | These actions are required for `ConsumeKinesis` to manage the lease table created in DynamoDB.                                                                                                          |
| Amazon DynamoDB             | CreateTable<br>DescribeTable<br>Scan<br>GetItem<br>PutItem<br>UpdateItem<br>DeleteItem                | Worker metrics and coordinator state table (metadata tables in DynamoDB) created by `ConsumeKinesis`.<br>`arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}-WorkerMetricStats`<br>`arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}-CoordinatorState` | These actions are required for `ConsumeKinesis` to manage the worker metrics and coordinator state metadata tables in DynamoDB.                                                                         |
| Amazon DynamoDB             | Query                                                                                                 | Global secondary index on the lease table.<br>`arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}/index/*`                                                                                                                                                         | This action is required for `ConsumeKinesis` to read the global secondary index of the lease table created in DynamoDB.                                                                                 |
| Amazon CloudWatch           | PutMetricData                                                                                         | `*`                                                                                                                                                                                                                                                                          | Upload metrics to CloudWatch that are useful for monitoring the application. The asterisk (*) is used because there is no specific resource in CloudWatch on which the PutMetricData action is invoked. |

The following is an example policy document for `ConsumeKinesis`.

```json
{
    "Version":"2012-10-17",		 	 	 
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:DescribeStreamSummary",
                "kinesis:RegisterStreamConsumer",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:ListShards"
            ],
            "Resource": [
                "arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:SubscribeToShard",
                "kinesis:DescribeStreamConsumer"
            ],
            "Resource": [
                "arn:aws:kinesis:{Region}:{Account}:stream/{Stream Name}/consumer/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:UpdateTable",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:Scan"
            ],
            "Resource": [
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:Scan"
            ],
            "Resource": [
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}-WorkerMetricStats",
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}-CoordinatorState"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:{Region}:{Account}:table/{Application Name}/index/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
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
