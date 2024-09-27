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

# ExecuteGraphQuery

## Description:

This processor is designed to work with Gremlin and Cypher queries. The query is specified in the configuration
parameter labeled Query, and parameters can be configured using dynamic properties on the processor. All Gremlin and
Cypher CRUD operations are supported by this processor. It will stream the entire result set into a single flowfile as a
JSON array.

## More Information

* [Cypher Query Language introduction](https://neo4j.com/developer/cypher-query-language/).
* [Gremlin Query DSL documentation](http://tinkerpop.apache.org/docs/current/reference/#connecting-gremlin).