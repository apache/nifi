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

# PutMongo

## Description:

This processor is a general purpose processor for inserting, upserting and updating MongoDB collections.

## Inserting Documents

Each flowfile is assumed to contain only a single MongoDB document to be inserted. The contents must be valid JSON. The
input the Mongo shell accepts should not be confused with valid JSON. It does not support batch writes at this time.

## Updating and Upserting

### Update Modes

There are two methods for choosing what gets written to a document when updating:

* Whole document - the entire document is replaced with the contents of the flowfile.
* With Operators Enabled - the document in the flowfile content will be assumed to have update operators such as _$set_
  and will be used to update particular fields. The whole document will not be replaced.

There are two ways to update:

* Update Key - use one or more keys from the document.
* Update Query - use a totally separate query that is not derived from the document.

### Update Key

The update key method takes keys from the document and builds a query from them. It will attempt to parse the _\_id_
field as an _ObjectID_ type if that is one of the keys that is specified in the configuration field. Multiple keys can
be specified by separating them with commas. This configuration field supports Expression Language, so it can be derived
in part or entirely from flowfile attributes.

### Update Query

The update query method takes a valid JSON document as its value and uses it to find one or more documents to update.
This field supports Expression Language, so it can be derived in part or entirely from flowfile attributes. It is
possible, for instance, to put an attribute named _update\_query_ on a flowfile and specify _${update\_query}_ in the
configuration field, so it will load the value from the flowfile.

### Upserts

If the upsert mode is enabled, PutMongo will insert a new document that matches the search criteria (be it a
user-supplied query or one built from update keys) and give it the properties that are specified in the JSON document
provided in the flowfile content. This feature should be used carefully, as it can result in incomplete data being added
to MongoDB.