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

`AwsRdsIamDatabasePasswordProvider` generates [Amazon RDS IAM authentication tokens](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.html) each time a JDBC connection is requested. The Controller Service implements the NiFi `DatabasePasswordProvider` API, so it can be referenced from DBCP controller services to avoid storing long-lived database passwords in NiFi.

## Usage

1. Configure an `AWSCredentialsProviderControllerService` so the password provider can obtain AWS credentials (for example, using an IAM role or `AssumeRoleWithWebIdentity`).
2. Create an `AwsRdsIamDatabasePasswordProvider` and reference the credentials provider service. Configure the AWS region. Host, port, and database user are inherited from the JDBC URL and “Database User” properties on the referencing DBCP service.
3. Update the DBCP controller service to set the _Database Password Provider_ property to the new IAM provider. The static _Password_ property is ignored when a provider is configured.
4. Ensure your JDBC URL enables TLS and includes the SSL parameters recommended by AWS (for example, `ssl=true&sslmode=verify-full` for PostgreSQL).

Each time the DBCP service needs to create a physical JDBC connection, a fresh IAM token is generated and supplied as the password. Existing pooled connections remain valid until the database closes them, so standard NiFi pooling properties such as “Maximum Connection Lifetime” still apply.

## Example Setup

### PostgreSQL role and privileges

Connect to the `nifi` database as a superuser and run:

```sql
CREATE ROLE nifi_app LOGIN PASSWORD 'temporary';
GRANT rds_iam TO nifi_app;

GRANT CONNECT ON DATABASE nifi TO nifi_app;
GRANT USAGE ON SCHEMA public TO nifi_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nifi_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nifi_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON TABLES TO nifi_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL PRIVILEGES ON SEQUENCES TO nifi_app;
```

### IAM permissions

Attach a policy like the following to your IAM role (for example `myAuroraPostgresRole`). Replace `<region>`, `<account-id>`, and `<db-resource-id>` with your values:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": "arn:aws:rds-db:<region>:<account-id>:dbuser:<db-resource-id>/nifi_app"
    }
  ]
}
```

Ensure the role’s trust policy allows the NiFi host (EC2, EKS, etc.) to assume it.

### CLI verification

Generate an IAM auth token and connect with `psql`:

```bash
TOKEN=$(aws rds generate-db-auth-token \
  --hostname database-1-instance-1.ccfuwyso6lcz.us-east-1.rds.amazonaws.com \
  --port 5432 \
  --region us-east-1 \
  --username nifi_app)

PGPASSWORD="$TOKEN" psql \
  "host=database-1-instance-1.ccfuwyso6lcz.us-east-1.rds.amazonaws.com \
   port=5432 user=nifi_app dbname=nifi \
   sslmode=verify-full sslrootcert=/path/to/rds-combined-ca-bundle.pem"
```

When that works, configure NiFi’s DBCP service with:

- `Database Connection URL`: `jdbc:postgresql://database-1-instance-1.ccfuwyso6lcz.us-east-1.rds.amazonaws.com:5432/nifi?ssl=true&sslmode=verify-full`
- `Database User`: `nifi_app`
- `Database Password Provider`: `AwsRdsIamDatabasePasswordProvider`

NiFi will then mint IAM tokens automatically for each new JDBC connection.
