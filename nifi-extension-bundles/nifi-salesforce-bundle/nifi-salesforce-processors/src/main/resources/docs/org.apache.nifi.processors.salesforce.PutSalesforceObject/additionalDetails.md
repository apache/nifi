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

# PutSalesforceObject

### Description

Objects in Salesforce are database tables, their rows are known as records, and their columns are called fields. The
PutSalesforceObject creates a new a Salesforce record in a Salesforce object. The Salesforce object must be set as the 
"objectType" attribute of an incoming flowfile.
Check [Salesforce documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_list.htm)
for object types and metadata. The processor utilizes NiFi record-based processing to allow arbitrary input format.

#### Example

If the "objectType" is set to "Account", the following JSON input will create two records in the Account object with the
names "SampleAccount1" and "SampleAccount2".

```json
[
  {
    "name": "SampleAccount1",
    "phone": "1111111111",
    "website": "www.salesforce1.com",
    "numberOfEmployees": "100",
    "industry": "Banking"
  },
  {
    "name": "SampleAccount2",
    "phone": "22222222",
    "website": "www.salesforce2.com",
    "numberOfEmployees": "200",
    "industry": "Banking"
  }
]
```