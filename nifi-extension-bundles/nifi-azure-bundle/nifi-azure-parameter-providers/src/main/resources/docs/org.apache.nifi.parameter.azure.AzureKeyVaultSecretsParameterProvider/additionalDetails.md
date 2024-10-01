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

# AzureKeyVaultSecretsParameterProvider

### Mapping Azure Key Vault Secrets to Parameter Contexts

The AzureKeyVaultSecretsParameterProvider maps a Secret to a Parameter, which can be grouped by adding a "group-name"
tag. To create a compatible secret from the Azure Portal:

1. Go to the "Key Vault" service
2. Create your own Key Vault
3. In your own Key Vault, navigate to "Secrets"
4. Create a secret with the name corresponding to the parameter name, and the value corresponding to the parameter
   value. Under "Tags", add a tag with a Key of "group-name" and a value of the intended Parameter Group name.

Alternatively, from the command line, run a command like the following once you have a Key Vault:

```
az keyvault secret set --vault-name [Vault Name] --name [Parameter Name] --value [Parameter Value] --tags group-name=[Parameter Group Name]
```

In this example, \[Parameter Group Name\] should be the intended name of the Parameter Group, \[Parameter Name\] should
be the parameter name, and \[Parameter Value\] should be the value of the parameter. \[Vault Name\] should be the name
you chose for your Key Vault in Azure

### Configuring the Parameter Provider

Azure Key Vault Secrets must be explicitly matched in the "Group Name Pattern" property in order for them to be fetched.
This prevents more than the intended Secrets from being pulled into NiFi.