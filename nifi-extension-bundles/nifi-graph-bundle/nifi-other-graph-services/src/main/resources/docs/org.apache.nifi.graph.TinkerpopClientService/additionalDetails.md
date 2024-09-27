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

# GremlinClientService

## Description:

This client service configures a connection to a Gremlin Server and allows Gremlin queries to be executed against the
Gremlin Server. For more information on Gremlin and Gremlin Server, see
the [Apache Tinkerpop](http://tinkerpop.apache.org/) project.

This client service supports two differnt modes of operation: Script Submission and Bytecode Submission, described
below.

## Script Submission

Script submission is the default way to interact with the gremlin server. This takes the input script and
uses [Script Submission](https://tinkerpop.apache.org/docs/current/reference/#gremlin-go-scripts) to interact with the
gremlin server. Because the script is shipped to the gremlin server as a string, only simple queries are recommended (
count, path, etc.) as there are no complex serializers available in this operation. This also means that NiFi will not
be opinionated about what is returned, whatever the response from the tinkerpop server is, the response will be
deserialized assuming common Java types. In the case of a Map return, the values will be returned as a record in the
FlowFile response, in all other cases, the return of the query will be coerced into a Map with key "result" and value
being the result of your script submission for that specific response.

### Serialization Issues in Script Submission

A common issue when creating Gremlin scripts for first time users is to accidentally return an unserializable object.
Gremlin is a Groovy DSL and so it behaves like compiled Groovy including returning the last statement in the script.
This is an example of a Gremlin script that could cause unexpected failures:

    g.V().hasLabel("person").has("name", "John Smith").valueMap()

The _valueMap()_ step is not directly serializable and will fail. To fix that you have two potential options:

    //Return a Map
    g.V().hasLabel("person").has("name", "John Smith").valueMap().next()

Alternative:

    g.V().hasLabel("person").has("name", "John Smith").valueMap()
    true //Return boolean literal

## Bytecode Submission

Bytecode submission is the more flexible of the two submission method and will be much more performant in a production
system. When combined with the Yaml connection settings and a custom jar, very complex graph queries can be run directly
within the NiFi JVM, leveraging custom serializers to decrease serialization overhead.

Instead of submitting a script to the gremlin server, requiring string serialization on both sides of the string result
set, the groovy script is compiled within the NiFi JVM. This compiled script has the bindings of g (the
GraphTraversalSource) and log (the NiFi logger) injected into the compiled code. Utilizing g, your result set is
contained within NiFi and serialization should take care of the overhead of your responses drastically decreasing the
likelihood of serialization errors.

As the result returned cannot be known by NiFi to be a specific type, your groovy script **must** rerun a Map<String,
Object>, otherwise the response will be ignored. Here is an example:

    Object results = g.V().hasLabel("person").has("name", "John Smith").valueMap().collect()
    [result: results]

This will break up your response objects into an array within your result key, allowing further processing within nifi
if necessary.