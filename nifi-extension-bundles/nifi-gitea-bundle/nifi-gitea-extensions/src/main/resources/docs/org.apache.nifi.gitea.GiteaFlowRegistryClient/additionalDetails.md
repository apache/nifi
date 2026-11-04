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

# Gitea Flow Registry Client

This component stores versioned flows in a Gitea repository using the Gitea REST API. Forgejo provides the same REST API,
so the component also works with Forgejo instances, including Codeberg.

The component communicates over HTTP or HTTPS with the REST API under `/api/v1`. It does not use the Git protocol or SSH.

## Repository Layout

Each top-level directory of the repository, or of the configured Repository Path, is a bucket. Each flow is stored as a
JSON file named after the flow identifier inside its bucket directory. Flow versions are the commit SHAs that changed the
flow file, and branches of the repository are available as registry branches.

When the repository has no top-level directories and the Access Token has write access, the client creates a `default`
bucket containing a `.keep` file.

## Access Token

Create an Access Token in the user settings of Gitea or Forgejo under Applications:

- `read:repository` is required to list buckets and import flows
- `write:repository` is required to commit, delete and branch flows

The client reports write access based on the repository permissions of the token owner. Repository permissions do not
reflect token scopes, so a token limited to `read:repository` passes verification for write access but commits fail
with HTTP 403. Archived repositories and pull mirrors are always read-only.

Branch protection rules apply to commits made through the REST API. Commits to a protected branch fail unless the token
owner is allowed to push to that branch.

## Web Client Service

Configure a `StandardWebClientServiceProvider` and select it in the Web Client Service property. Certificates for TLS
and proxy settings are configured on the Web Client Service. The SSL Context Service property of this component is not
used.

## Gitea API URL

Set the Gitea API URL to the base URL of the instance, such as `https://gitea.example.com` or `https://codeberg.org`.
When the instance runs under a context path, include the path, such as `https://example.com/gitea`. A URL ending with
`/api/v1` is also accepted.

## Commit Authors

When Commit Author Source is set to Application User, the identity of the NiFi user is sent as both the author name and
the author email. The owner of the Access Token remains the committer.

## Commit History

For a given flow, the client retrieves at most the last 50 commits to limit API calls. Older versions are not listed but
remain available to process groups that reference them.

## Names

Branch names and bucket directory names containing `&` or `+` are not supported because these characters are not
encoded in query parameters.

## Compatibility

The client has been tested with Gitea 1.20 to 1.27 and Forgejo 11 to 16. Gitea 1.21 updates branch listings
asynchronously, so a newly created branch can take a few seconds to appear.
