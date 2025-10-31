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

# GCPCredentialsControllerService

The **GCPCredentialsControllerService** centralizes all authentication strategies used by NiFi components that
interact with Google Cloud. Each strategy exposes only the properties it requires, which lets administrators swap
approaches without touching downstream processors. This guide summarizes every supported strategy.

---

## Application Default Credentials

Application Default Credentials (ADC) allow NiFi to inherit credentials exposed through the runtime environment,
including:
- The `GOOGLE_APPLICATION_CREDENTIALS` environment variable referencing a service-account key file
- `gcloud auth application-default login` on development machines
- Cloud Shell or other Google-managed environments that inject ADC automatically

No extra properties are required. Confirm that the account supplying the ADC token has the IAM roles needed by the
processors referencing this controller service.

---

## Service Account Credentials (JSON File)

Use this strategy when the service-account key material is stored on disk. Configure the **Service Account JSON
File** property to point at the JSON key file. NiFi reads the file when the controller service is enabled and
caches the Google credentials.

**Best practices**
- Restrict filesystem permissions so only the NiFi service user can read the key.
- Rotate keys regularly and delete unused keys from the Google Cloud Console.
- When impersonating a domain user, set **Delegation Strategy** to *Delegated Account* and provide **Delegation
  User** so that NiFi calls Google APIs on behalf of that user.

---

## Service Account Credentials (JSON Value)

This strategy embeds the entire service-account JSON document directly inside the controller-service property. The
value is marked sensitive and can be injected through Parameter Contexts to separate credentials from flow
definitions.

**Best practices**
- Store the JSON value in a Parameter Context referenced by this property so you can swap credentials per
  environment.
- Use NiFi’s Sensitive Property encryption in `nifi.properties` to encrypt the stored JSON on disk.

---

## Compute Engine Credentials

Select **Compute Engine Credentials** when NiFi runs on a Google-managed runtime (Compute Engine, GKE, etc.) and
should use the instance’s attached service account. Google automatically refreshes the metadata server tokens, so
no additional properties are required.

**Best practices**
- Grant the instance service account only the roles required by your flows.
- If multiple NiFi nodes share the same instance template, verify that all nodes have access to the same IAM
  permissions or configure Workload Identity Federation for finer control.

---

## Workload Identity Federation

Workload Identity Federation (WIF) exchanges an external identity-provider token for a short-lived Google Cloud
access token via Google’s Security Token Service (STS). The controller service configures Google’s
`IdentityPoolCredentials`, allowing Google client libraries to refresh Google Cloud tokens automatically.

### 1. Configure Workload Identity Federation in Google Cloud

```bash
# Create a pool (only once per environment)
gcloud iam workload-identity-pools create nifi-pool \
  --project=MY_PROJECT_ID \
  --location=global \
  --display-name="NiFi Pool"

# Create a provider bound to your IdP (example for OIDC named myidp)
gcloud iam workload-identity-pools providers create-oidc myidp \
  --project=MY_PROJECT_ID \
  --location=global \
  --workload-identity-pool=nifi-pool \
  --display-name="My Identity Provider" \
  --issuer-uri="https://identity.myidp.com/oauth2/..." \
  --allowed-audiences="//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/providers/myidp" \
  --attribute-mapping="google.subject=assertion.sub"
```

Record the audience string printed by the command; it must be copied into NiFi’s **Audience** property exactly:

```
//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/providers/myidp
```

### 2. Authorize the workload identity principal for Google Cloud resources

The STS-issued access token represents the workload identity principal itself. Grant IAM roles to that identity on
projects or specific resources:

```bash
# Project scoped
gcloud projects add-iam-policy-binding MY_PROJECT_ID \
  --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/subject/IDENTITY_SUBJECT" \
  --role="roles/storage.objectViewer"

# Bucket scoped (example)
gcloud storage buckets add-iam-policy-binding gs://MY_BUCKET \
  --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/subject/IDENTITY_SUBJECT" \
  --role="roles/storage.objectViewer"
```

`IDENTITY_SUBJECT` must match the claim you mapped in the provider (for example `assertion.sub`). Service-account
impersonation is not yet supported, so grant roles directly to the workload identity principal.

### 3. Configure NiFi properties (Workload Identity strategy selected)

| Property | Guidance |
| --- | --- |
| **Audience** | Paste the provider resource name recorded above. |
| **Scope** | Defaults to `https://www.googleapis.com/auth/cloud-platform`; supply space- or comma-separated scopes if you need fewer permissions. |
| **STS Token Endpoint** | Optional override for the Google STS endpoint; leave blank to use `https://sts.googleapis.com/v1/token`. |
| **Subject Token Provider** | Controller Service that retrieves the upstream workload identity token (JWT or access token). The token must contain the claims referenced by your attribute mapping. |
| **Subject Token Type** | Defaults to `urn:ietf:params:oauth:token-type:jwt`. Choose the alternate access-token type only when the upstream provider issues OAuth access tokens instead of JWTs. |
| **Proxy Configuration Service** | Optional controller service allowing NiFi to reach STS through HTTP/SOCKS proxies. |

Once these properties are set, enable GCPCredentialsControllerService. Processors referencing it immediately obtain
`IdentityPoolCredentials`, and Google’s libraries refresh access tokens automatically using the configured subject
-token provider.

### Verification workflow

1. Enable or refresh the Subject Token Provider controller service.
2. Use the **Verify** action on GCPCredentialsControllerService. Successful verification confirms that NiFi can
   exchange the subject token with Google STS using the configured proxy, audience, and scopes.
3. Enable dependent processors. No additional controller services are required.

### Troubleshooting

| Symptom | Guidance |
| --- | --- |
| `403 Caller does not have storage.objects.list` | Confirm the workload identity principal has the required IAM role: `gcloud projects get-iam-policy` / `gcloud storage buckets get-iam-policy`. Ensure the attribute mapping emits the same subject referenced in IAM. |
| STS errors during verification | Double-check the **Audience** string and **STS Token Endpoint**. Use DEBUG logs or the Verify dialog output to inspect the STS response. Ensure the subject token includes the mapped claims. |
| Access token rejected by Google APIs | Call the API directly with the federated token (for example, `curl -H "Authorization: Bearer TOKEN" https://storage.googleapis.com/...`). If it still fails, revisit IAM bindings or scope selection. |
| Need to rotate upstream tokens | The controller service requests a fresh subject token 60 seconds before expiry. Trigger **Refresh** on the Subject Token Provider to invalidate cached tokens immediately. |

---

With every strategy available from a single controller-service configuration, NiFi users can migrate between
service-account keys, Compute Engine, Application Default Credentials, and Workload Identity Federation without
introducing new controller services or updating processor properties. Adjust the authentication strategy once and
all dependent processors automatically pick up the new credentials.
