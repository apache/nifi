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

# RestLookupService

## General

This lookup service has the following optional lookup coordinate keys:

* request.method; defaults to 'get', valid values:
    * delete
    * get
    * post
    * put
* request.body; contains a string representing JSON, XML, etc. to be sent with any of those methods except for "get".
* mime.type; specifies media type of the request body, required when 'body' is passed.
* \*; any other keys can be configured to pass variables to resolve target URLs. See 'Dynamic URLs' section below.

The record reader is used to consume the response of the REST service call and turn it into one or more records. The
record path property is provided to allow for a lookup path to either a nested record or a single point deep in the REST
response. Note: a valid schema must be built that encapsulates the REST response accurately in order for this service to
work.

## Headers

Headers are supported using dynamic properties. Just add a dynamic property and the name will be the header name and the
value will be the value for the header. Expression language powered by input from the variable registry is supported.

## Dynamic URLs

The URL property supports expression language through the lookup key/value pairs configured on the component using this
lookup service (e.g. LookupRecord processor). The configuration specified by the user will be passed through to the
expression language engine for evaluation. Note: flowfile attributes will be disregarded here for this property.

Ex. URL: _http://example.com/service/${user.name}/friend/${friend.id}_, combined with example record paths at
LookupRecord processor:

* user.name => "/example/username"
* friend.id => "/example/first\_friend"

Would dynamically produce an endpoint of _http://example.com/service/john.smith/friend/12345_

### Using Environment Properties with URLs

In addition to the lookup key/value pairs, environment properties / system variables can be referred from expression
languages configured at the URL property.

Ex. URL: _http://\${apiServerHostname}:\${apiServerPort}/service/\${user.name}/friend/\${friend.id}_, combined with the
previous example record paths, and environment properties:

* apiServerHostname => "test.example.com"
* apiServerPort => "8080"

Would dynamically produce an endpoint of _http://test.example.com:8080/service/john.smith/friend/12345_