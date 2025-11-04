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

# StandardGCPIdentityFederationTokenProvider

The **StandardGCPIdentityFederationTokenProvider** exchanges a workload identity token issued by an
external identity provider for a short-lived Google Cloud access token using the Google Security Token Service (STS).
It configures Google’s `IdentityPoolCredentials`, allowing the Google client libraries to refresh access tokens automatically.
The returned token is returned to the `GCPCredentialsControllerService` when the `GCP Identity Federation Token Provider`
property is configured. Identity federation is treated as a mutually exclusive primary strategy; keep the other
credential strategies (Application Default, Compute Engine, Service Account JSON) unset.

---

## End-to-end Google Cloud configuration

### 1. Configure Workload Identity Federation

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
  --allowed-audiences="https://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/providers/myidp" \
  --attribute-mapping="google.subject=assertion.sub"
```

The audience value you supply here is the string that must be copied into the NiFi controller service property:

```
//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/providers/myidp
```

### 2. Authorize the workload identity principal for GCP resources

The access token returned by STS represents the workload identity principal itself. Grant it the required roles on
each resource (project or bucket):

```bash
# Project scoped
gcloud projects add-iam-policy-binding MY_PROJECT_ID \
  --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/subject/IDENTITY_SUBJECT" \
  --role="roles/storage.objectViewer"

# Bucket scoped (if preferred)
gcloud storage buckets add-iam-policy-binding gs://MY_BUCKET \
  --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/nifi-pool/subject/IDENTITY_SUBJECT" \
  --role="roles/storage.objectViewer"
```

Replace `IDENTITY_SUBJECT` with the value carried in the upstream token’s `sub` claim (or the attribute you mapped).

Service account impersonation is not yet supported; the controller service uses the workload identity principal directly.
You can still validate the bindings with:

```bash
gcloud storage objects list gs://MY_BUCKET \
  --impersonate-service-account=nifi-workload@MY_PROJECT_ID.iam.gserviceaccount.com
```

If that command lists the objects, the federation pipeline is functioning.

---

## NiFi controller service configuration

| Property | Guidance |
| --- | --- |
| **Audience** | Use the exact string recorded when creating the provider (`//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/providers/<PROVIDER_ID>`). |
| **Scope** | Defaults to `https://www.googleapis.com/auth/cloud-platform`. Reduce the scope if only specific APIs are required. |
| **STS Token Endpoint** | Optional override. Leave empty to use the default `https://sts.googleapis.com/v1/token`. |
| **Subject Token Provider** | Controller service that retrieves the external JWT/access token. The token should expose the claims referenced in the provider’s attribute mapping. |
| **Subject Token Type** | Defaults to `urn:ietf:params:oauth:token-type:jwt`. Change only if the upstream provider returns a different token type supported by STS. |
| **Proxy Configuration Service** | Optional. Configure when NiFi must reach Google STS through an HTTP or SOCKS proxy during verification or when other components reuse this controller service directly. |

Once the controller service is enabled, configure the `GCPCredentialsControllerService` to reference it in
`GCP Identity Federation Token Provider`. Keep the other credential strategies unconfigured.

---

## Verification workflow

1. Enable or refresh the subject-token provider controller service.
2. Verify the **StandardGCPIdentityFederationTokenProvider**. Successful verification confirms that the subject token is usable and that Google STS returned an access token using the configured transport (including any proxy settings).
3. Verify the `GCPCredentialsControllerService` or enable dependent processors.

---

## Troubleshooting

### 403 “Caller does not have storage.objects.list” (or similar)

- Ensure the workload identity principal has the required IAM role:
  ```bash
  gcloud projects get-iam-policy MY_PROJECT_ID --filter="principal://.../subject/IDENTITY_SUBJECT"
  gcloud storage buckets get-iam-policy gs://MY_BUCKET
  ```
- Confirm the provider’s attribute mapping matches the IAM binding (`google.subject=assertion.sub` vs `attribute.sub=...`).
- Re-verify with `gcloud storage objects list ... --impersonate-service-account=...` after updating IAM.

### STS verification fails

- Double-check the audience string and token endpoint.
- Use DEBUG logging (or the Verify action) to inspect the STS error body.
- Confirm the subject token contains the claims referenced in the provider mapping.

### Access token is rejected when calling Google APIs

- Inspect the STS token:
  ```bash
  curl -H "Authorization: Bearer ACCESS_TOKEN" \
       "https://storage.googleapis.com/storage/v1/b/MY_BUCKET/o?maxResults=1"
  ```
- If the response is still 403, revisit IAM bindings for the workload identity principal.

### Need to rotate upstream subject tokens frequently

- Configure the upstream Subject Token Provider with its own refresh/rotation policy.
- The controller service requests a fresh subject token whenever the previous token is within 60 seconds of expiry; use the **Refresh** action if you need to invalidate the cached subject token immediately.

---

With the steps above, NiFi’s federated controller service should obtain Google Cloud access tokens that carry the same
permissions as the workload identity subject you configured. Future enhancements will add optional service-account
impersonation if you need the downstream token to assume the service account’s identity directly.
