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

# ElasticSearchLookupService

## Description:

This lookup service uses ElasticSearch as its data source. Mappings in LookupRecord map record paths to paths within an
ElasticSearch document. Example:

```
/user/name => user.contact.name
```

That would map the record path _/user/name_ to an embedded document named _contact_ with a field named _name_.

The query that is assembled from these is a boolean query where all the criteria are under the _must_ list. In addition,
wildcards are not supported right now and all criteria are translated into literal _match_ queries.

## Post-Processing

Because an ElasticSearch result might be structured differently than the record which will be enriched by this service,
users can specify an additional set of mappings on this lookup service that map JsonPath operations to record paths.
Example:

```
$.user.contact.email => /user/email_address
```

Would copy the field _email_ from the embedded document _contact_ into the record at that path.