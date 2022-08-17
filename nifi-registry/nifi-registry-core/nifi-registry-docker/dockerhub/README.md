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

# Docker Image Quickstart

## Capabilities
This image currently supports running in standalone mode either unsecured or with user authentication provided through:
   * [Two-Way SSL with Client Certificates](https://nifi.apache.org/docs/nifi-registry-docs/html/administration-guide.html#security-configuration)
   * [Lightweight Directory Access Protocol (LDAP)](https://nifi.apache.org/docs/nifi-registry-docs/html/administration-guide.html#ldap_identity_provider)
   
## Building
The Docker image can be built using the following command:

    # user @ puter in ~/path/to/apache/nifi-registry/nifi-registry-docker/dockerhub    
    $ docker build -t apache/nifi-registry:latest .

This will result in an image tagged apache/nifi-registry:latest

    $ docker images
    > REPOSITORY               TAG           IMAGE ID            CREATED                  SIZE
    > apache/nifi-registry     latest        751428cbf631        A long, long time ago    342MB
    
**Note**: The default version of NiFi Registry specified by the Dockerfile is typically that of one that is unreleased if working from source.
To build an image for a prior released version, one can override the `NIFI_REGISTRY_VERSION` build-arg with the following command:
    
    $ docker build --build-arg NIFI_REGISTRY_VERSION={Desired NiFi Registry Version} -t apache/nifi-registry:latest .

There is, however, no guarantee that older versions will work as properties have changed and evolved with subsequent releases.
The configuration scripts are suitable for at least 0.1.0+.

## Running a container

### Unsecured
The minimum to run a NiFi Registry instance is as follows:

    docker run --name nifi-registry \
      -p 18080:18080 \
      -d \
      apache/nifi-registry:latest
      
This will provide a running instance, exposing the instance UI to the host system on at port 18080,
viewable at `http://localhost:18080/nifi-registry`.

You can also pass in environment variables to change the NiFi Registry communication ports and hostname using the Docker '-e' switch as follows:

    docker run --name nifi-registry \
      -p 19090:19090 \
      -d \
      -e NIFI_REGISTRY_WEB_HTTP_PORT='19090' \
      apache/nifi-registry:latest

For a list of the environment variables recognised in this build, look into the .sh/secure.sh and .sh/start.sh scripts
        
### Secured with Two-Way TLS
In this configuration, the user will need to provide certificates and the associated configuration information.
Of particular note, is the `AUTH` environment variable which is set to `tls`.  Additionally, the user must provide an
the DN as provided by an accessing client certificate in the `INITIAL_ADMIN_IDENTITY` environment variable.
This value will be used to seed the instance with an initial user with administrative privileges.
Finally, this command makes use of a volume to provide certificates on the host system to the container instance.

    docker run --name nifi-registry \
      -v /path/to/tls/certs/localhost:/opt/certs \
      -p 18443:18443 \
      -e AUTH=tls \
      -e KEYSTORE_PATH=/opt/certs/keystore.jks \
      -e KEYSTORE_TYPE=JKS \
      -e KEYSTORE_PASSWORD=QKZv1hSWAFQYZ+WU1jjF5ank+l4igeOfQRp+OSbkkrs \
      -e TRUSTSTORE_PATH=/opt/certs/truststore.jks \
      -e TRUSTSTORE_PASSWORD=rHkWR1gDNW3R9hgbeRsT3OM3Ue0zwGtQqcFKJD2EXWE \
      -e TRUSTSTORE_TYPE=JKS \
      -e INITIAL_ADMIN_IDENTITY='CN=AdminUser, OU=nifi' \
      -d \
      apache/nifi-registry:latest

### Secured with LDAP
In this configuration, the user will need to provide certificates and the associated configuration information.  Optionally,
if the LDAP provider of interest is operating in LDAPS or START_TLS modes, certificates will additionally be needed.
Of particular note, is the `AUTH` environment variable which is set to `ldap`.  Additionally, the user must provide a
DN as provided by the configured LDAP server in the `INITIAL_ADMIN_IDENTITY` environment variable. This value will be 
used to seed the instance with an initial user with administrative privileges.  Finally, this command makes use of a 
volume to provide certificates on the host system to the container instance.

For a minimal, connection to an LDAP server using SIMPLE authentication:

    docker run --name nifi-registry \
      -v /path/to/tls/certs/localhost:/opt/certs \
      -p 18443:18443 \
      -e AUTH=ldap \
      -e KEYSTORE_PATH=/opt/certs/keystore.jks \
      -e KEYSTORE_TYPE=JKS \
      -e KEYSTORE_PASSWORD=QKZv1hSWAFQYZ+WU1jjF5ank+l4igeOfQRp+OSbkkrs \
      -e TRUSTSTORE_PATH=/opt/certs/truststore.jks \
      -e TRUSTSTORE_PASSWORD=rHkWR1gDNW3R9hgbeRsT3OM3Ue0zwGtQqcFKJD2EXWE \
      -e TRUSTSTORE_TYPE=JKS \
      -e INITIAL_ADMIN_IDENTITY='cn=nifi-admin,dc=example,dc=org' \
      -e LDAP_AUTHENTICATION_STRATEGY='SIMPLE' \
      -e LDAP_MANAGER_DN='cn=ldap-admin,dc=example,dc=org' \
      -e LDAP_MANAGER_PASSWORD='password' \
      -e LDAP_USER_SEARCH_BASE='dc=example,dc=org' \
      -e LDAP_USER_SEARCH_FILTER='cn={0}' \
      -e LDAP_IDENTITY_STRATEGY='USE_DN' \
      -e LDAP_URL='ldap://ldap:389' \
      -d \
      apache/nifi-registry:latest

The following, optional environment variables may be added to the above command when connecting to a secure LDAP server configured with START_TLS or LDAPS

    -e LDAP_TLS_KEYSTORE: ''
    -e LDAP_TLS_KEYSTORE_PASSWORD: ''
    -e LDAP_TLS_KEYSTORE_TYPE: ''
    -e LDAP_TLS_TRUSTSTORE: ''
    -e LDAP_TLS_TRUSTSTORE_PASSWORD: ''
    -e LDAP_TLS_TRUSTSTORE_TYPE: ''

### Additional Configuration Options

#### Database Configuration

The following, optional environment variables can be used to configure the database.

| nifi-registry.properties entry         | Variable                   |
|----------------------------------------|----------------------------|
| nifi.registry.db.url                   | NIFI_REGISTRY_DB_URL       |
| nifi.registry.db.driver.class          | NIFI_REGISTRY_DB_CLASS     |
| nifi.registry.db.driver.directory      | NIFI_REGISTRY_DB_DIR       |
| nifi.registry.db.username              | NIFI_REGISTRY_DB_USER      |
| nifi.registry.db.password              | NIFI_REGISTRY_DB_PASS      |
| nifi.registry.db.maxConnections        | NIFI_REGISTRY_DB_MAX_CONNS |
| nifi.registry.db.sql.debug             | NIFI_REGISTRY_DB_DEBUG_SQL |

#### Flow Persistence Configuration

The following, optional environment variables may be added to configure flow persistence provider.

| Environment Variable           | Configuration Property               |
|--------------------------------|--------------------------------------|
| NIFI_REGISTRY_FLOW_STORAGE_DIR | Flow Storage Directory               |
| NIFI_REGISTRY_FLOW_PROVIDER    | (Class tag); valid values: git, file |
| NIFI_REGISTRY_GIT_REMOTE       | Remote to Push                       |
| NIFI_REGISTRY_GIT_USER         | Remote Access User                   |
| NIFI_REGISTRY_GIT_PASSWORD     | Remote Access Password               |
| NIFI_REGISTRY_GIT_REPO         | Remote Clone Repository              |

#### Extension Bundle Persistence Configuration

The following, optional environment variables may be added to configure extension bundle persistence provider.

| Environment Variable                  | Configuration Property              |
|---------------------------------------|-------------------------------------|
| NIFI_REGISTRY_BUNDLE_STORAGE_DIR      | Extension Bundle Storage Directory  |
| NIFI_REGISTRY_BUNDLE_PROVIDER         | (Class tag); valid values: file, s3 |
| NIFI_REGISTRY_S3_REGION               | Region                              |
| NIFI_REGISTRY_S3_BUCKET_NAME          | Bucket Name                         |
| NIFI_REGISTRY_S3_KEY_PREFIX           | Key Prefix                          |
| NIFI_REGISTRY_S3_CREDENTIALS_PROVIDER | Credentials Provider                |
| NIFI_REGISTRY_S3_ACCESS_KEY           | Access Key                          |
| NIFI_REGISTRY_S3_SECRET_ACCESS_KEY    | Secret Access Key                   |
| NIFI_REGISTRY_S3_ENDPOINT_URL         | Endpoint URL                        |

