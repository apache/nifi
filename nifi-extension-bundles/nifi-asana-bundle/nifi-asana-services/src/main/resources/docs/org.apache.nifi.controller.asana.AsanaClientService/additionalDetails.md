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

# AsanaClientService

### Description

This service manages credentials to Asana, and provides a common API for the processors to work on the user specified
workspace (or organization).

### Using a custom Asana instance

If you have an Asana instance running on your custom domain, then you need to specify the _API URL_ of that instance.
For example: `https://asana.example.com/api/1.0`

### Authentication

Asana supports a few methods of authenticating with the API. Simple cases are usually handled with a personal access
token, while multi-user apps utilize OAuth.

Asana provides three ways to authenticate:

* [OAuth](https://developers.asana.com/docs/oauth)
* [Personal access token (PAT)](https://developers.asana.com/docs/personal-access-token)
* [OpenID Connect](https://developers.asana.com/docs/openid-connect)

**Note:** This service currently only supports _Personal access tokens_ as authentication method.

Personal access tokens (PATs) are a useful mechanism for accessing the API in scenarios where OAuth would be considered
overkill, such as access from the command line and personal scripts or applications. A user can create many, but not
unlimited, personal access tokens. When creating a token, you must give it a description to help you remember what you
created the token for.  
Remember to keep your tokens secret and treat them just like passwords. Your tokens act on your behalf when interacting
with the API.

You can generate a personal access token from the [Asana developer console](https://app.asana.com/0/developer-console).
See the [Authentication quick start](https://developers.asana.com/docs/authentication-quick-start) for detailed
instructions on getting started with PATs.

### Workspaces & Organizations

A _workspace_ is the highest-level organizational unit in Asana. All projects and tasks have an associated workspace.

An _organization_ is a special kind of _workspace_ that represents a company. In an organization, you can group your  
projects into teams. You can read more about how organizations work on
the [Asana Guide](https://asana.com/guide/help/workspaces/basics).

You can read more about how objects are organized in Asana in
the [developer guide](https://developers.asana.com/docs/object-hierarchy).

### Further reading about Asana

* [Asana Academy](https://academy.asana.com)
* [Asana Guide](https://asana.com/guide)
* [Asana Developer Documentation](https://developers.asana.com/docs)
* [Java client library for the Asana API](https://github.com/Asana/java-asana/)