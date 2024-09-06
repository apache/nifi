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

# DeleteMongo

## Description:

This processor deletes from Mongo using a user-provided query that is provided in the body of a flowfile. It must be a
valid JSON document. The user has the option of deleting a single document or all documents that match the criteria.
That behavior can be configured using the related configuration property. In addition, the processor can be configured
to regard a failure to delete any documents as an error event, which would send the flowfile with the query to the
failure relationship.

### Example Query

```json
{
  "username": "john.smith",
  "recipient": "jane.doe"
}
```
