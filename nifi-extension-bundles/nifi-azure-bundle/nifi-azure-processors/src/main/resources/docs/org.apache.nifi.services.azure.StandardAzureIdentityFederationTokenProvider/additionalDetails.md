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

# StandardAzureIdentityFederationTokenProvider

The *StandardAzureIdentityFederationTokenProvider* exchanges workload identity tokens from external identity providers for Azure AD access tokens using Azure Identity SDK's `ClientAssertionCredential`. This approach provides built-in token caching, automatic refresh, and robust error handling.

Components such as the ADLS and Azure Storage credentials controller services reference it when the **Credentials Type** is set to **Access Token**.

> **Note**: Microsoft Entra requires a single resource (`*.default`) per client credentials request. Configure one controller service per Azure resource you need to access.


## Configuration workflow

1. **Client Assertion Provider** – Select a controller service that retrieves the external workload identity token. The token is passed to Azure AD as the `client_assertion` parameter.
2. **Tenant ID** and **Client ID** – Provide the Microsoft Entra tenant and application (client) ID for the federated app registration.
3. **Scope** – Defaults to `https://storage.azure.com/.default`. Adjust to match the resource you are targeting; Azure AD only allows a single resource (`*.default`) per token request.

At runtime the service uses `ClientAssertionCredential` to exchange the client assertion for an Azure AD access token via `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token`. The Azure Identity SDK handles token caching and automatic refresh when tokens expire. The returned Azure AD access token is propagated to the calling component.

Ensure the federated app registration has the necessary Azure RBAC roles (for example *Storage Blob Data Contributor* and *Azure Event Hubs Data Receiver/Sender* as appropriate) and that the client assertion provider refreshes assertions before they expire so new Azure access tokens can be obtained. Create separate controller service instances if you need tokens for different Azure resources.

## Event Hub components

- `GetAzureEventHub`, `PutAzureEventHub`, and `ConsumeAzureEventHub` support the **Identity Federation** authentication strategy for Event Hubs connections. Configure this controller service with a scope such as `https://eventhubs.azure.net/.default`.
- `ConsumeAzureEventHub` also supports Identity Federation for the Blob Storage checkpoint store. Configure a separate controller service instance using the Storage scope (for example `https://storage.azure.com/.default`).


## Entra ID setup summary

1. **Create or reuse an app registration** for NiFi in Microsoft Entra ID.
2. **Add a federated credential** (Certificates & secrets → Federated credentials) matching your issuer/subject. Set the audience to `api://AzureADTokenExchange`.
3. **Assign RBAC roles** to that app registration, such as `Storage Blob Data Reader`/`Storage Blob Data Contributor` on the storage account.
4. Record the **Tenant ID** and **Client ID** for configuring the controller service in NiFi.


## Scope examples

- `https://storage.azure.com/.default` – Azure Storage operations only.
- `https://eventhubs.azure.net/.default` – Event Hubs operations.
- `https://management.azure.com/.default` – Azure Resource Manager APIs.
