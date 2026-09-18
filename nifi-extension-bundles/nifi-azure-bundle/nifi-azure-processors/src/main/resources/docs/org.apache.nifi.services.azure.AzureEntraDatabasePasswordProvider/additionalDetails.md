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

`AzureEntraDatabasePasswordProvider` acquires a short-lived Microsoft Entra access token and supplies it as the
database password for a DBCP service. Use it to connect NiFi to Azure Database for PostgreSQL Flexible Server or Azure
Database for MySQL Flexible Server without storing a long-lived database password in NiFi. Other database products and
sovereign Azure clouds are not supported.

The provider supplies a password when DBCP creates a new physical connection. Existing pooled connections are not
reauthenticated when the token expires.

## Usage

1. Configure an Azure Credentials Service that can obtain credentials for the public Azure cloud.
2. Create and enable `AzureEntraDatabasePasswordProvider`.
3. Set **Azure Credentials Service** to the configured credentials service.
4. Configure the DBCP service with the JDBC URL, driver, database user, and **Database Password Provider** set to
   `AzureEntraDatabasePasswordProvider`.
5. Run **Verify** on the provider, then run **Verify** on the DBCP service.

Create the Microsoft Entra principal in the database separately and grant the database privileges required by NiFi.

## Workload Identity Federation

For Workload Identity Federation, configure `StandardAzureCredentialsControllerService` with **Credentials Strategy**
set to **Identity Federation** and select a `StandardAzureIdentityFederationTokenProvider`. The federation token
provider accepts an OAuth2 access token provider that supplies the external client assertion. The federated identity
credential in Microsoft Entra must match the assertion issuer, subject, and audience.

The password provider requests the public-cloud Azure OSS RDBMS resource. It does not require the Azure JDBC
authentication plugins and does not depend on a particular external assertion issuer.

## PostgreSQL Configuration

Configure Microsoft Entra authentication on Azure Database for PostgreSQL Flexible Server and create the database
role that corresponds to the Entra principal. Set the DBCP **Database User** to that mapped role name.

The token is sent using PostgreSQL cleartext-password authentication and must be protected by TLS. Use direct port
5432 as the baseline and set `sslmode=verify-full`, with the client configured to trust the server certificate chain.

| Setting | Value |
|---|---|
| Driver Class Name | `org.postgresql.Driver` |
| JDBC URL | `jdbc:postgresql://<SERVER>.postgres.database.azure.com:5432/<DATABASE>?sslmode=verify-full` |
| Database User | Microsoft Entra principal's mapped PostgreSQL role name |

Azure's built-in PgBouncer on port 6432 must be validated separately. Large Entra tokens containing many group claims
can exceed intermediary protocol buffers even when the same token works against direct port 5432. This provider does
not claim unrestricted PgBouncer compatibility.

## MySQL Configuration

Configure Microsoft Entra authentication on Azure Database for MySQL Flexible Server and create an Entra user or alias
for the principal. Set the DBCP **Database User** to the mapped alias. MySQL user names are limited to 32 characters, so
an alias can be required for a longer Entra principal name.

MySQL Connector/J must use TLS, verify the server certificate, and permit the `mysql_clear_password` authentication
mechanism used to transmit the token. Configure Connector/J with a trust store containing the issuing CA certificate,
use `sslMode=VERIFY_IDENTITY`, and set `allowCleartextPasswords=true`. The Azure MySQL JDBC authentication plugin is not
required when this provider supplies the token.

| Setting | Value |
|---|---|
| Driver Class Name | `com.mysql.cj.jdbc.Driver` |
| Driver Location(s) | compatible MySQL Connector/J driver jar provided to the DBCP service |
| JDBC URL | `jdbc:mysql://<SERVER>.mysql.database.azure.com:3306/<DATABASE>?sslMode=VERIFY_IDENTITY&allowCleartextPasswords=true` |
| Database User | Microsoft Entra principal's mapped MySQL user or alias |

## Verify and Troubleshooting

`AzureEntraDatabasePasswordProvider` **Verify** checks that the configured Azure Credentials Service can acquire a
nonblank token for the Azure OSS RDBMS resource. It does not validate the JDBC URL, database server, network
path, TLS configuration, driver, principal mapping, or database grants. DBCP **Verify** checks the actual database
connection using those settings.

If provider **Verify** fails, confirm that the Azure Credentials Service and any upstream identity-federation services
are enabled and configured for the intended Entra application and tenant. If provider **Verify** succeeds but DBCP
**Verify** fails, investigate the JDBC connection, network, TLS, driver, database principal mapping, and grants.