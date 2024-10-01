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

# GcpSecretManagerParameterProvider

### Mapping GCP Secrets to Parameter Contexts

The GcpSecretManagerParameterProvider maps a Secret to a Parameter, which can be grouped by adding a "group-name" label.
To create a compatible secret from the GCP Console:

1. From the Secret Manager service, click the "Create Secret" button
2. Enter the Secret name. This is the name of a parameter. Enter a value.
3. Under "Labels", add a label with a Key of "group-name" and a value of the intended Parameter Group name.

Alternatively, from the command line, run a command like the following:

```
printf "[Parameter Value]" | gcloud secrets create --labels=group-name="[Parameter Group Name]" "[Parameter Name]" --data-file=-
```

In this example, \[Parameter Group Name\] should be the intended name of the Parameter Group, \[Parameter Name\] should
be the parameter name, and \[Parameter Value\] should be the value of the parameter.

### Configuring the Parameter Provider

GCP Secrets must be explicitly matched in the "Group Name Pattern" property in order for them to be fetched. This
prevents more than the intended Secrets from being pulled into NiFi.