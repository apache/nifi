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

# JWT Bearer OAuth 2.0 Access Token Provider

## Description

The `JWTBearerOAuth2AccessTokenProvider` provides an implementation of the `OAuth2AccessTokenProvider` in order to support
the JWT Bearer Flow.

## Configuration Details

Every service exposing APIs where the OAuth 2.0 JWT Bearer Flow is used for authentication may have some nuances in
terms of configuration and requirements for the private key used to sign the JWT. For this reason, this controller
service supports sensitive dynamic properties providing a way to specify custom JWT claims (using dynamic properties
with a key prefixed by `CLAIM.`) as well as custom form parameters for the request against the access token API
(using dynamic properties with a key prefixed by `FORM.`).

Below are some configuration examples for some well known SaaS solutions.

### Google Identity ([source](https://developers.google.com/identity/protocols/oauth2/service-account#httprest))

| Property name/key        | Property value |
| ------------------------ | -------------- |
| Token Endpoint           | `https://oauth2.googleapis.com/token` |
| Signing Algorithm        | `RS256` |
| Issuer                   | The email address of the service account |
| Subject                  | (optional) The email address of the user for which the application is requesting delegated access |
| Audience                 | `https://oauth2.googleapis.com/token` |
| Scope                    | A space-delimited list of the permissions that the application requests |
| JWT ID                   | not set |
| Set JWT Header x5t       | `false` |
| Key ID                   | The key ID of the service account key |
| Grant Type               | `urn:ietf:params:oauth:grant-type:jwt-bearer` |
| Assertion Parameter Name | `assertion` |

### Salesforce ([source](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_jwt_flow.htm&type=5))

| Property name/key        | Property value |
| ------------------------ | -------------- |
| Token Endpoint           | `https://my-instance.develop.my.salesforce.com/services/oauth2/token` |
| Signing Algorithm        | `RS256` |
| Issuer                   | The issuer must contain the OAuth `client_id` (Consumer Key) of the connected app for which you registered the certificate |
| Subject                  | (optional) If you’re implementing this flow for an Experience Cloud site, the subject must contain the user’s username |
| Audience                 | The audience identifies the authorization server as an intended audience. It can be `https://login.salesforce.com` or `https://test.salesforce.com` for sandboxes |
| Scope                    | not set |
| JWT ID                   | not set |
| Set JWT Header x5t       | `false` |
| Key ID                   | not set |
| Grant Type               | `urn:ietf:params:oauth:grant-type:jwt-bearer` |
| Assertion Parameter Name | `assertion` |

### Microsoft ([source](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#second-case-access-token-request-with-a-certificate))

| Property name/key        | Property value |
| ------------------------ | -------------- |
| Token Endpoint           | `https://login.microsoftonline.com/<tenantId>/oauth2/v2.0/token` |
| Signing Algorithm        | `PS256` |
| Issuer                   | Use the GUID application ID |
| Subject                  | Use the same value as issuer |
| Audience                 | `https://login.microsoftonline.com/<tenantId>/oauth2/v2.0/token` |
| Scope                    | not set |
| JWT ID                   | `${UUID()}` |
| Set JWT Header x5t       | `true` |
| SSL Context Service      | SSL Context Service to provide the public certificate in order to have its thumbprint in the JWT header |
| Key ID                   | not set |
| Grant Type               | `client_credentials` |
| Assertion Parameter Name | `client_assertion` |
| `FORM.client_id`           | The application ID that's assigned to your app |
| `FORM.tenant`              | The directory tenant the application plans to operate against, in GUID or domain-name format |
| `FORM.scope`               | The value passed for the scope parameter in this request should be the resource identifier (application ID URI) of the resource you want, suffixed with `.default`. All scopes included must be for a single resource. Including scopes for multiple resources will result in an error. For the Microsoft Graph example, the value is `https://graph.microsoft.com/.default` |
| `FORM.client_assertion_type` | `urn:ietf:params:oauth:client-assertion-type:jwt-bearer` |

### Box ([source](https://developer.box.com/guides/authentication/jwt/without-sdk/#3-create-jwt-assertion))

| Property name/key        | Property value |
| ------------------------ | -------------- |
| Token Endpoint           | `https://api.box.com/oauth2/token` |
| Signing Algorithm        | `RS256`, `RS384`, or `RS512` |
| Issuer                   | The Box Application's OAuth client ID |
| Subject                  | The Box Enterprise ID if this app is to act on behalf of the Service Account of that application, or the User ID if this app wants to act on behalf of another user. |
| Audience                 | `https://api.box.com/oauth2/token` |
| Scope                    | not set |
| JWT ID                   | `${UUID()}` |
| Set JWT Header x5t       | `false` |
| Key ID                   | The ID of the public key used to sign the JWT. Not required, though essential when multiple key pairs are defined for an application. |
| Grant Type               | `urn:ietf:params:oauth:grant-type:jwt-bearer` |
| Assertion Parameter Name | `assertion` |
| JWT Expiration Time      | `1 minute` (cannot be more than 60 seconds) |
| `CLAIM.box_sub_type`       | `enterprise` or `user` depending on the type of token being requested in the `sub` claim |
| `FORM.client_id`           | Client ID |
| `FORM.client_secret`       | Client Secret |
