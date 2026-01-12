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

The *StandardAzureIdentityFederationTokenProvider* provides Azure `TokenCredential` for workload identity federation. It exchanges tokens from external identity providers for Azure AD credentials using Azure Identity SDK's `ClientAssertionCredential`. This approach provides built-in token caching, automatic refresh, and robust error handling.

Components such as the ADLS and Azure Storage credentials controller services reference this provider when the **Credentials Type** is set to **Access Token**.


## Configuration workflow

1. **Client Assertion Provider** – Select a controller service that retrieves the external workload identity token. The token is passed to Azure AD as the `client_assertion` parameter.
2. **Tenant ID** and **Client ID** – Provide the Microsoft Entra tenant and application (client) ID for the federated app registration.

At runtime the service provides a `TokenCredential` backed by `ClientAssertionCredential`. When the consuming component requests a token (specifying the appropriate scope), the credential exchanges the client assertion for an Azure AD access token via `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token`. The Azure Identity SDK handles token caching and automatic refresh when tokens expire.

Ensure the federated app registration has the necessary Azure RBAC roles (for example *Storage Blob Data Contributor* and *Azure Event Hubs Data Receiver/Sender* as appropriate) and that the client assertion provider refreshes assertions before they expire so new Azure access tokens can be obtained.


## Azure Resource Scopes

Different Azure services require different scopes when requesting tokens. The scope is determined automatically by the consuming component based on the Azure service being accessed:

- `https://storage.azure.com/.default` – Azure Storage operations (Blob, ADLS, Queue).
- `https://eventhubs.azure.net/.default` – Event Hubs operations.
- `https://management.azure.com/.default` – Azure Resource Manager APIs.

> **Note**: Microsoft Entra requires a single resource (`*.default`) per client credentials request.


## Event Hub components

- `GetAzureEventHub`, `PutAzureEventHub`, and `ConsumeAzureEventHub` support the **Identity Federation** authentication strategy for Event Hubs connections.
- `ConsumeAzureEventHub` also supports Identity Federation for the Blob Storage checkpoint store.


## Entra ID setup summary

1. **Create or reuse an app registration** for NiFi in Microsoft Entra ID.
2. **Add a federated credential** (Certificates & secrets → Federated credentials) matching your issuer/subject. Set the audience to `api://AzureADTokenExchange`.
3. **Assign RBAC roles** to that app registration, such as `Storage Blob Data Reader`/`Storage Blob Data Contributor` on the storage account.
4. Record the **Tenant ID** and **Client ID** for configuring the controller service in NiFi.
