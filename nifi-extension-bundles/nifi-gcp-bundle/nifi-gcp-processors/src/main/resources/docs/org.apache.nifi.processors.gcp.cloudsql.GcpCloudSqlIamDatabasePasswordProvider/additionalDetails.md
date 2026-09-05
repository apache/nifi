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

## Summary

`GcpCloudSqlIamDatabasePasswordProvider` generates a short-lived Cloud SQL IAM login token and supplies it as the
database password for a DBCP service. Use it when you want NiFi to connect to Cloud SQL without storing a long-lived
database password in NiFi.

The provider works with Cloud SQL for PostgreSQL and Cloud SQL for MySQL. When a DBCP service references this provider,
the static DBCP password property is ignored.

## Usage

1. Configure `GCPCredentialsControllerService` so NiFi can obtain Google credentials.
2. Create and enable `GcpCloudSqlIamDatabasePasswordProvider`.
3. Set **GCP Credentials Provider Service** to the credentials service.
4. Configure the DBCP service with the JDBC URL, driver, database user, and **Database Password Provider** set to
   `GcpCloudSqlIamDatabasePasswordProvider`.
5. Run **Verify** on the provider, then run **Verify** on the DBCP service.

Create the Cloud SQL IAM database user separately and grant the database privileges required by your application.

## Workload Identity Federation

For Workload Identity Federation, `GCPCredentialsControllerService` must be configured with **Target Service Account**.
The workload identity principal must also have `roles/iam.workloadIdentityUser` on that target service account.

Without target service account impersonation, token acquisition for Cloud SQL IAM authentication will not succeed.

## PostgreSQL Configuration

Cloud SQL for PostgreSQL expects the DBCP **Database User** to match the IAM identity used for login. Configure the
DBCP service with a PostgreSQL JDBC driver and PostgreSQL JDBC URL. Configure TLS in the DBCP service as required for
your environment.

| Setting | Value |
|---|---|
| Driver Class Name | `org.postgresql.Driver` |
| JDBC URL | `jdbc:postgresql://<HOST>:5432/<DATABASE>?sslmode=require` |
| Database User for Google user | full email address |
| Database User for service account | service-account email without `.gserviceaccount.com` |

Example service-account mapping:

- Target service account: `nifi-sa@my-project.iam.gserviceaccount.com`
- DBCP **Database User**: `nifi-sa@my-project.iam`

## MySQL Configuration

Cloud SQL for MySQL uses the full service-account email when the IAM database user is created, but the JDBC login name
must be only the portion before `@`. Configure the DBCP service with a compatible MySQL Connector/J driver and MySQL
JDBC URL. Configure TLS in the DBCP service as required for your environment.

| Setting | Value |
|---|---|
| Driver Class Name | `com.mysql.cj.jdbc.Driver` |
| Driver Location(s) | compatible MySQL Connector/J driver jar provided to the DBCP service |
| JDBC URL | `jdbc:mysql://<HOST>:3306/<DATABASE>?sslMode=REQUIRED` |
| Database User | service-account identifier before `@` |

Example service-account mapping:

- IAM database user created in Cloud SQL: `nifi-sa@my-project.iam.gserviceaccount.com`
- DBCP **Database User**: `nifi-sa`

## Verify and Troubleshooting

`GcpCloudSqlIamDatabasePasswordProvider` **Verify** checks that NiFi can obtain a token. DBCP **Verify** checks the
actual database connection using the configured URL, driver, TLS settings, database user, and password provider.

If provider **Verify** fails:

- Confirm the referenced `GCPCredentialsControllerService` is enabled.
- For Workload Identity Federation, confirm **Target Service Account** is set and the workload identity principal has
  `roles/iam.workloadIdentityUser` on that service account.

If provider **Verify** succeeds but DBCP **Verify** fails, token acquisition is working and the problem is in the JDBC
connection configuration, network path, TLS settings, driver setup, database user, or database privileges.