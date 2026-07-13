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

# PutElasticsearchJson

This processor is for accessing the Elasticsearch Bulk API. It provides the ability to configure bulk operations on a
per-FlowFile basis, which is what separates it from PutElasticsearchRecord.

As part of the Elasticsearch REST API bundle, it uses a controller service to manage connection information and that
controller service is built on top of the official Elasticsearch client APIs. That provides features such as automatic
master detection against the cluster which is missing in the other bundles.

This processor builds one Elasticsearch Bulk API body per (batch of) FlowFiles. Care should be taken to batch FlowFiles
into appropriately-sized chunks so that NiFi does not run out of memory and the requests sent to Elasticsearch are not
too large for it to handle. When failures do occur, this processor is capable of attempting to route the FlowFiles that
failed to an errors queue so that only failed FlowFiles can be processed downstream or replayed.

The index, operation and (optional) type fields are configured with default values. The ID (optional unless the
operation is "index") can be set as an attribute on the FlowFile(s).

### Field Path Mode

The **Identifier Field**, **Index Field** and **Timestamp Field** properties each name a field within the document
whose value is extracted (and used as the document ID, index name, or `@timestamp` respectively). The **Field Path
Mode** property controls how those values are interpreted:

* **Literal Field Name** (the default) &mdash; each value is the exact name of a top-level field. A `/` or `\` is just
  a character in the field name, so a value like `@metadata/id` matches a top-level field literally named
  `@metadata/id`.
* **Nested Field Path** &mdash; each value is a `/`-delimited path into nested objects, as described below.

#### Literal Field Name (default)

For the document:

```json
{
  "@metadata/id": "abc",
  "message": "Hello, world"
}
```

an Identifier Field of `@metadata/id` selects `abc` &mdash; the top-level field whose name is literally
`@metadata/id`. The same value against `{"@metadata": {"id": "abc"}}` selects nothing, because there is no
top-level field with that exact name.

#### Nested Field Path

The remainder of this section applies when **Field Path Mode** is set to **Nested Field Path**. For the document:

```json
{
  "@metadata": {
    "id": "abc",
    "index": "my-index"
  },
  "message": "Hello, world"
}
```

* the Identifier Field path `@metadata/id` selects `abc`
* the Index Field path `@metadata/index` selects `my-index`

#### Field names that contain a slash

Because `/` separates path segments, a field whose name literally contains a `/` must have that slash escaped as `\/`,
and a literal backslash must be escaped as `\\`. For example, for the document `{"a/b": "abc"}` the path `a\/b` selects
the top-level field named `a/b`.

#### Removal and pruning

When the corresponding **Retain** property is set to `false`, the field is removed from the document body after its
value is extracted. For a nested path, any parent object that is left empty by the removal is also pruned. For example,
extracting and removing `@metadata/id` from `{"@metadata": {"id": "abc"}, "message": "Hello, world"}` leaves
`{"message": "Hello, world"}`, with the now-empty `@metadata` object removed.

### Dynamic Templates

Index and Create operations can use Dynamic Templates. The Dynamic Templates property must be parsable as a JSON object.

#### Example - Index with Dynamic Templates

```json
{
  "message": "Hello, world"
}
```

The Dynamic Templates property below would be parsable:

```json
{
  "message": "keyword_lower"
}
```

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

Update and Upsert operations can use a script. Scripts must contain all the elements required by Elasticsearch, e.g.
source and lang. The Script property must be parsable as a JSON object.

If a script is defined for an upset, the Flowfile content will be used as the upsert fields in the Elasticsearch action.
If no script is defined, the FlowFile content will be used as the update doc (or doc\_as\_upsert for upsert operations).

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
  "counter": 1
}
```

The script property below would be parsable:

```json
{
  "source": "ctx._source.counter += params.param1",
  "lang": "painless",
  "params": {
    "param1": 1
  }
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

Dynamic Properties can be defined on the processor with _BULK:_ prefixes. Users must ensure that only known Bulk action
fields are sent to Elasticsearch for the relevant index operation defined for the FlowFile, Elasticsearch will reject
invalid combinations of index operation and Bulk action fields.

#### Example - Update with Retry on Conflict

```json
{
  "message": "Hello, world",
  "from": "john.smith"
}
```

With the Dynamic Property below:

* BULK:retry\_on\_conflict = 3

Would create Elasticsearch action:

```json
{
  "update": {
    "_id": "1",
    "_index": "test",
    "retry_on_conflict": "3"
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