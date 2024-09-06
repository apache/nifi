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

# PutMongoBulkOperation

## Description:

This processor runs bulk updates against MongoDB collections. The flowfile content is expected to be a JSON array with
bulk write operations as described in
the [manual for db.collection.bulkWrite](https://www.mongodb.com/docs/manual/reference/method/db.collection.bulkWrite/).

You can use all (currently 6) operators described there. The flowfile content is returned as-is. You can merge many
operations into one - and get massive performance improvements.

## Example:

The following is an example flowfile content that does two things: insert a new document, and update all documents where
value of _hey_ is greater than zero.

```json
[
  {
    "insertOne": {
      "document": {
        "ho": 42
      }
    }
  },
  {
    "updateMany": {
      "filter": {
        "hey": {
          "$gt": 0
        }
      },
      "update": {
        "$inc": {
          "hey": 2
        }
      }
    }
  }
]
```
