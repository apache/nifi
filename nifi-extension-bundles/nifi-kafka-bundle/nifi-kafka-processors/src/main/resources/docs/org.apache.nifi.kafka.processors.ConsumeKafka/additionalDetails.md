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

# ConsumeKafka

### Output Strategies

This processor offers multiple output strategies (configured via processor property 'Output Strategy') for converting Kafka records into FlowFiles.

- 'Use Content as Value' (the default) emits FlowFile records containing only the Kafka record value.
- 'Use Wrapper' emits FlowFile records containing the Kafka record key, value, and headers, as well as additional metadata from the Kafka record.
- 'Inject Metadata' emits FlowFile records containing the Kafka record value into which a sub record field has been added to hold metadata, headers and key information.

The record schema that is used when "Use Wrapper" is selected is as follows (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [{
      "name": "key",
      "type": [{
          < Schema is determined by the Key Record Reader, or will be "string" or "bytes", depending on the "Key Format" property (see below for more details) >
        }, "null"]
    },
    {
      "name": "value",
      "type": [
        {
          < Schema is determined by the Record Reader >
        },
        "null"
      ]
    },
    {
      "name": "headers",
      "type": [
        { "type": "map", "values": "string", "default": {}},
        "null"]
    },
    {
      "name": "metadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "topic", "type": ["string", "null"] },
            { "name": "partition", "type": ["int", "null"] },
            { "name": "offset", "type": ["int", "null"] },
            { "name": "timestamp", "type": ["long", "null"] }
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
      < Fields as determined by the Record Reader for the Kafka message >,
    {
      "name": "kafkaMetadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "topic", "type": ["string", "null"] },
            { "name": "partition", "type": ["int", "null"] },
            { "name": "offset", "type": ["int", "null"] },
            { "name": "timestamp", "type": ["long", "null"] },
            {
              "name": "headers",
              "type": [ { "type": "map", "values": "string", "default": {}}, "null"]
            },
            {
              "name": "key",
              "type": [{
                < Schema is determined by the Key Record Reader, or will be "string" or "bytes", depending on the "Key Format" property (see below for more details) >
              }, "null"]
            }
          ]
        },
        "null"
      ]
    }
  ]
}
```

If the Output Strategy property is set to 'Use Wrapper' or 'Inject Metadata', an additional processor configuration property ('Key Format') is activated. This property is used to specify how the Kafka Record's key should be written out to the FlowFile. The possible values for 'Key Format' are as follows:

- 'Byte Array' supplies the Kafka Record Key as a byte array, exactly as they are received in the Kafka record.
- 'String' converts the Kafka Record Key bytes into a string using the UTF-8 character encoding. (Failure to parse the key bytes as UTF-8 will result in the record being routed to the 'parse.failure' relationship.)
- 'Record' converts the Kafka Record Key bytes into a deserialized NiFi record, using the associated 'Key Record Reader' controller service. If the Key Format property is set to 'Record', an additional processor configuration property name 'Key Record Reader' is made available. This property is used to specify the Record Reader to use in order to parse the Kafka Record's key as a Record.

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy "Use Wrapper" is selected:

```json
[
  {
    "key": {
      "name": "Acme",
      "number": "AC1234"
    },
    "value": {
      "address": "1234 First Street",
      "zip": "12345",
      "account": {
        "name": "Acme",
        "number": "AC1234"
      }
    },
    "headers": {
      "attributeA": "valueA",
      "attributeB": "valueB"
    },
    "metadata": {
      "topic": "accounts",
      "partition": 0,
      "offset": 0,
      "timestamp": 0
    }
  }
]
```

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy "Inject Metadata" is selected:

```json
[
  {
    "address": "1234 First Street",
    "zip": "12345",
    "account": {
      "name": "Acme",
      "number": "AC1234"
    },
    "kafkaMetadata": {
      "topic": "accounts",
      "partition": 0,
      "offset": 0,
      "timestamp": 0,
      "headers": {
        "attributeA": "valueA",
        "attributeB": "valueB"
      },
      "key": {
        "name": "Acme",
        "number": "AC1234"
      }
    }
  }
]
```
