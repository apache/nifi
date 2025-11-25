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

# Azure DevOps Flow Registry Client – Service Principal Setup

This component stores NiFi versioned flows in Azure DevOps Git using the Azure DevOps Git REST API. It authenticates with Microsoft Entra service principals through OAuth 2.0 client credentials.

## Entra ID (Microsoft Entra) – Service Principal
1. App registrations → New registration → name (e.g., `NiFi Git DevOps Registry Client`); single-tenant is fine; no redirect URI.
2. Record `Directory (tenant) ID` and `Application (client) ID`.
3. Certificates & secrets → New client secret. Note the provided **Value**.

## Azure DevOps
1. Organization must use the same tenant.
2. Organization Settings → Users → Add users → paste the **Application (client) ID**; Access Level = **Basic**; add to the right projects/groups (e.g., Contributors). Do not send email.

## NiFi Management Controller Services
- **StandardWebClientServiceProvider**: default timeouts are usually fine; set proxy/SSL if needed. Enable.
- **StandardOauth2AccessTokenProvider**:
  - Token Endpoint (replace with value "Directory (tenant) ID"): `https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/token`
  - Grant Type: `Client Credentials`
  - Client ID: Application (client) ID
  - Client Secret: value from the created client secret
  - Scope: `https://app.vssps.visualstudio.com/.default`
  - Configure proxy/SSL if required.
  - Optional: Verify to confirm a token can be obtained
  - Enable

## NiFi Flow Registry Client
- Type: `AzureDevOpsFlowRegistryClient`
- Azure DevOps API URL: `https://dev.azure.com`
- Organization, Project, Repository Name
- Default Branch: must already exist (e.g., `main`).
- Repository Path: optional subfolder; **no leading or trailing `/`**.
- Authentication Strategy: `Service Principal`.
- OAuth2 Access Token Provider: choose the configured `StandardOauth2AccessTokenProvider`.
- Web Client Service: choose the `StandardWebClientServiceProvider`.
- Optionally Verify to confirm read/write.
