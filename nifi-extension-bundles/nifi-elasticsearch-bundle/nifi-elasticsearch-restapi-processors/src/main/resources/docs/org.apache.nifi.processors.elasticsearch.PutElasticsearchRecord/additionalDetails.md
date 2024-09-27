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

# PutElasticsearchRecord

This processor is for accessing the Elasticsearch Bulk API. It provides the ability to configure bulk operations on a
per-record basis which is what separates it from PutElasticsearchJson. For example, it is possible to define multiple
commands to index documents, followed by deletes, creates and update operations against the same index or other indices
as desired.

As part of the Elasticsearch REST API bundle, it uses a controller service to manage connection information and that
controller service is built on top of the official Elasticsearch client APIs. That provides features such as automatic
master detection against the cluster which is missing in the other bundles.

This processor builds one Elasticsearch Bulk API body per record set. Care should be taken to split up record sets into
appropriately-sized chunks so that NiFi does not run out of memory and the requests sent to Elasticsearch are not too
large for it to handle. When failures do occur, this processor is capable of attempting to write the records that failed
to an output record writer so that only failed records can be processed downstream or replayed.

### Per-Record Actions

The index, operation and (optional) type fields are configured with default values that can be overridden using record
path operations that find an index or type value in the record set. The ID and operation type (create, index, update,
upsert or delete) can also be extracted in a similar fashion from the record set. A "@timestamp" field can be added to
the data either using a default or by extracting it from the record set. This is useful if the documents are being
indexed into an Elasticsearch Data Stream.

#### Example - per-record actions

The following is an example of a document exercising all of these features:

```json
{
  "metadata": {
    "id": "12345",
    "index": "test",
    "type": "message",
    "operation": "index"
  },
  "message": "Hello, world",
  "from": "john.smith",
  "ts": "2021-12-03'T'14:00:00.000Z"
}
```

```json
{
  "metadata": {
    "id": "12345",
    "index": "test",
    "type": "message",
    "operation": "delete"
  }
}
```

The record path operations below would extract the relevant data:

* /metadata/id
* /metadata/index
* metadata/type
* metadata/operation
* /ts

### Dynamic Templates

Index and Create operations can use Dynamic Templates from the Record, record path operations can be configured to find
the Dynamic Templates from the record set. Dynamic Templates fields in Records must either be a Map, child Record or a
string that can be parsable as a JSON object.

#### Example - Index with Dynamic Templates

```json
{
  "message": "Hello, world",
  "dynamic_templates": "{\"message\": \"keyword_lower\"}"
}
```

The record path operation below would extract the relevant Dynamic Templates:

* /dynamic\_templates

Would create Elasticsearch action:

```json
{
  "index": {
    "_id": "1",
    "_index": "test",
    "dynamic_templates": {
      "message": "keyword_lower"
    }
  }
}
```

```json
{
  "doc": {
    "message": "Hello, world"
  }
}
```

### Update/Upsert Scripts

Update and Upsert operations can use a script from the Record, record path operations can be configured to find the
script from the record set. Scripts must contain all the elements required by Elasticsearch, e.g. source and lang.
Script fields in Records must either be a Map, child Record or a string that can be parsable as a JSON object.

If a script is defined for an upset, any fields remaining in the Record will be used as the upsert fields in the
Elasticsearch action. If no script is defined, all Record fields will be used as the update doc (or doc\_as\_upsert for
upsert operations).

#### Example - Update without Script

```json
{
  "message": "Hello, world",
  "from": "john.smith"
}
```

Would create Elasticsearch action:

```json
{
  "update": {
    "_id": "1",
    "_index": "test"
  }
}
```

```json
{
  "doc": {
    "message": "Hello, world",
    "from": "john.smith"
  }
}
```

#### Example - Upsert with Script

```json
{
  "counter": 1,
  "script": {
    "source": "ctx._source.counter += params.param1",
    "lang": "painless",
    "params": {
      "param1": 1
    }
  }
}
```

The record path operation below would extract the relevant script:

* /script

Would create Elasticsearch action:

```json
{
  "update": {
    "_id": "1",
    "_index": "test"
  }
}
```

```json
{
  "script": {
    "source": "ctx._source.counter += params.param1",
    "lang": "painless",
    "params": {
      "param1": 1
    }
  },
  "upsert": {
    "counter": 1
  }
}
```

### Bulk Action Header Fields

Dynamic Properties can be defined on the processor with _BULK:_ prefixes. The value of the Dynamic Property is a record
path operation to find the field value from the record set. Users must ensure that only known Bulk action fields are
sent to Elasticsearch for the relevant index operation defined for the Record, Elasticsearch will reject invalid
combinations of index operation and Bulk action fields.

#### Example - Update with Retry on Conflict

```json
{
  "message": "Hello, world",
  "from": "john.smith",
  "retry": 3
}
```

The Dynamic Property and record path operation below would extract the relevant field:

* BULK:retry\_on\_conflict = /retry

Would create Elasticsearch action:

```json
{
  "update": {
    "_id": "1",
    "_index": "test",
    "retry_on_conflict": 3
  }
}
```

```json
{
  "doc": {
    "message": "Hello, world",
    "from": "john.smith"
  }
}
```

### Index Operations

Valid values for "operation" are:

* create
* delete
* index
* update
* upsert