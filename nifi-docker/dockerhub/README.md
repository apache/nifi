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

## Latest changes

### 2.0.0

- Changed base image to bellsoft/liberica-openjdk-debian:21 as NiFi 2.0.0 requires Java 21

### 1.19.0

- Changed base image to eclipse-temurin:11-jre as openjdk:8-jre is no longer maintained
- This also change the image to use Java 11 instead of Java 8
- As a benefit, the NiFi image now supports arm64 platforms in addition to amd64/x86_64

### 1.14.0

- Updated default container configuration to use HTTPS with Single User Authentication

### 1.12.0
- The NiFi Toolkit has been added to the image under the path `/opt/nifi/nifi-toolkit-current` also set as the environment variable `NIFI_TOOLKIT_HOME`
- The installation directory and related environment variables are changed to be version-agnostic to `/opt/nifi/nifi-current`:
```
docker run --rm --entrypoint /bin/bash apache/nifi:1.12.0 -c 'env | grep NIFI'
NIFI_HOME=/opt/nifi/nifi-current
NIFI_LOG_DIR=/opt/nifi/nifi-current/logs
NIFI_TOOLKIT_HOME=/opt/nifi/nifi-toolkit-current
NIFI_PID_DIR=/opt/nifi/nifi-current/run
NIFI_BASE_DIR=/opt/nifi
```
- A symlink refer to the new path for backward compatibility:
```
docker run --rm --entrypoint /bin/bash apache/nifi:1.12.0 -c 'readlink /opt/nifi/nifi-1.12.0'                                   /opt/nifi/nifi-current
```

# Docker Image Quickstart

## Capabilities
This image currently supports running in standalone mode either unsecured or with user authentication provided through:
  * [Single User Authentication](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#single_user_identity_provider)    
  * [Mutual TLS with Client Certificates](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#security-configuration)
  * [Lightweight Directory Access Protocol (LDAP)](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#ldap_login_identity_provider)

This image also contains the NiFi Toolkit (as of version 1.8.0) preconfigured to use either in secure and unsecure mode.

## Building
The Docker image can be built using the following command:

    docker build -t apache/nifi:latest .

This build will result in an image tagged apache/nifi:latest

    # user @ puter in ~/Development/code/apache/nifi/nifi-docker/dockerhub
    $ docker images
    REPOSITORY               TAG                 IMAGE ID            CREATED                 SIZE
    apache/nifi              latest              f0f564eed149        A long, long time ago   1.62GB

**Note**: The default version of NiFi specified by the Dockerfile is typically that of one that is unreleased if working from source.
To build an image for a prior released version, one can override the `NIFI_VERSION` build-arg with the following command:

    docker build --build-arg=NIFI_VERSION={Desired NiFi Version} -t apache/nifi:latest .

There is, however, no guarantee that older versions will work as properties have changed and evolved with subsequent releases.
The configuration scripts are suitable for at least 1.4.0+.

## Running a container

### Standalone Instance secured with HTTPS and Single User Authentication
The minimum to run a NiFi instance is as follows:

    docker run --name nifi \
      -p 8443:8443 \
      -d \
      apache/nifi:latest

This will provide a running instance, exposing the instance UI to the host system on at port 8443,
viewable at `https://localhost:8443/nifi`.
The default configuration generates a random username and password on startup. NiFi writes the generated credentials to the application log.

The following command can be used to find the generated credentials on operating systems with grep installed:

    docker logs nifi | grep Generated

NiFi logs the generated credentials as follows:

    Generated Username [USERNAME]
    Generated Password [PASSWORD]

Environment variables can be used to set the NiFi communication ports and hostname using the Docker '-e' switch as follows:

    docker run --name nifi \
      -p 9443:9443 \
      -d \
      -e NIFI_WEB_HTTPS_PORT='9443' \
      apache/nifi:latest

Single User Authentication credentials can be specified using environment variables as follows:

    docker run --name nifi \
      -p 8443:8443 \
      -d \
      -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
      -e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB \
      apache/nifi:latest

Please note that the password must be 12 characters minimum, otherwise NiFi will generate a random username and password.
See `secure.sh` and `start.sh` scripts for supported environment variables.

### Standalone Instance secured with HTTPS and Mutual TLS Authentication
In this configuration, the user will need to provide certificates and associated configuration information.
Of particular note, is the `AUTH` environment variable which is set to `tls`.  Additionally, the user must provide an
the DN as provided by an accessing client certificate in the `INITIAL_ADMIN_IDENTITY` environment variable.
This value will be used to seed the instance with an initial user with administrative privileges.
Finally, this command makes use of a volume to provide certificates on the host system to the container instance.

    docker run --name nifi \
      -v /User/dreynolds/certs/localhost:/opt/certs \
      -p 8443:8443 \
      -e AUTH=tls \
      -e KEYSTORE_PATH=/opt/certs/keystore.jks \
      -e KEYSTORE_TYPE=JKS \
      -e KEYSTORE_PASSWORD=QKZv1hSWAFQYZ+WU1jjF5ank+l4igeOfQRp+OSbkkrs \
      -e TRUSTSTORE_PATH=/opt/certs/truststore.jks \
      -e TRUSTSTORE_PASSWORD=rHkWR1gDNW3R9hgbeRsT3OM3Ue0zwGtQqcFKJD2EXWE \
      -e TRUSTSTORE_TYPE=JKS \
      -e INITIAL_ADMIN_IDENTITY='CN=Random User, O=Apache, OU=NiFi, C=US' \
      -d \
      apache/nifi:latest

### Standalone Instance secured with HTTPS and LDAP Authentication
In this configuration, the user will need to provide certificates and associated configuration information.  Optionally,
if the LDAP provider of interest is operating in LDAPS or START_TLS modes, certificates will additionally be needed.
Of particular note, is the `AUTH` environment variable which is set to `ldap`.  Additionally, the user must provide a
DN as provided by the configured LDAP server in the `INITIAL_ADMIN_IDENTITY` environment variable. This value will be
used to seed the instance with an initial user with administrative privileges. Finally, this command makes use of a
volume to provide certificates on the host system to the container instance.

#### For a minimal, connection to an LDAP server using SIMPLE authentication:

    docker run --name nifi \
      -v /User/dreynolds/certs/localhost:/opt/certs \
      -p 8443:8443 \
      -e AUTH=ldap \
      -e KEYSTORE_PATH=/opt/certs/keystore.jks \
      -e KEYSTORE_TYPE=JKS \
      -e KEYSTORE_PASSWORD=QKZv1hSWAFQYZ+WU1jjF5ank+l4igeOfQRp+OSbkkrs \
      -e TRUSTSTORE_PATH=/opt/certs/truststore.jks \
      -e TRUSTSTORE_PASSWORD=rHkWR1gDNW3R9hgbeRsT3OM3Ue0zwGtQqcFKJD2EXWE \
      -e TRUSTSTORE_TYPE=JKS \
      -e INITIAL_ADMIN_IDENTITY='cn=admin,dc=example,dc=org' \
      -e LDAP_AUTHENTICATION_STRATEGY='SIMPLE' \
      -e LDAP_MANAGER_DN='cn=admin,dc=example,dc=org' \
      -e LDAP_MANAGER_PASSWORD='password' \
      -e LDAP_USER_SEARCH_BASE='dc=example,dc=org' \
      -e LDAP_USER_SEARCH_FILTER='cn={0}' \
      -e LDAP_IDENTITY_STRATEGY='USE_DN' \
      -e LDAP_URL='ldap://ldap:389' \
      -d \
      apache/nifi:latest

#### The following, optional environment variables may be added to the above command when connecting to a secure  LDAP server configured with START_TLS or LDAPS

    -e LDAP_TLS_KEYSTORE: ''
    -e LDAP_TLS_KEYSTORE_PASSWORD: ''
    -e LDAP_TLS_KEYSTORE_TYPE: ''
    -e LDAP_TLS_TRUSTSTORE: ''
    -e LDAP_TLS_TRUSTSTORE_PASSWORD: ''
    -e LDAP_TLS_TRUSTSTORE_TYPE: ''

### Standalone Instance secured with HTTPS and OpenID Authentication
In this configuration, the user will need to provide certificates and associated configuration information. 
Of particular note, is the `AUTH` environment variable which is set to `oidc`. Additionally, the user must provide a
in the `INITIAL_ADMIN_IDENTITY` environment variable. This value will be used to seed the instance with an initial 
user with administrative privileges. Alternatively, the `INITIAL_ADMIN_GROUP` environment variable can be specified 
to grant access to a group of users instead.

### For a minimal, connection to an OpenID server

    docker run --name nifi \
      -v $(pwd)/certs/localhost:/opt/certs \
      -p 8443:8443 \
      -e AUTH=oidc \
      -e KEYSTORE_PATH=/opt/certs/keystore.jks \
      -e KEYSTORE_TYPE=JKS \
      -e KEYSTORE_PASSWORD=QKZv1hSWAFQYZ+WU1jjF5ank+l4igeOfQRp+OSbkkrs \
      -e TRUSTSTORE_PATH=/opt/certs/truststore.jks \
      -e TRUSTSTORE_PASSWORD=rHkWR1gDNW3R9hgbeRsT3OM3Ue0zwGtQqcFKJD2EXWE \
      -e TRUSTSTORE_TYPE=JKS \
      -e INITIAL_ADMIN_IDENTITY='test' \
      -e INITIAL_ADMIN_GROUP='myGroup' \
      -e NIFI_SECURITY_USER_OIDC_DISCOVERY_URL=http://OPENID_SERVER_URL/auth/realms/OPENID_REALM/.well-known/openid-configuration \
      -e NIFI_SECURITY_USER_OIDC_CONNECT_TIMEOUT=10000 \
      -e NIFI_SECURITY_USER_OIDC_READ_TIMEOUT=10000 \
      -e NIFI_SECURITY_USER_OIDC_CLIENT_ID=nifi \
      -e NIFI_SECURITY_USER_OIDC_CLIENT_SECRET=tU47ugXO308WZqf5TtylyoMX3xH6W0kN \
      -e NIFI_SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM=RS256 \
      -e NIFI_SECURITY_USER_OIDC_ADDITIONAL_SCOPES=email \
      -e NIFI_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER=preferred_username \
      -e NIFI_SECURITY_USER_OIDC_CLAIM_GROUPS=admin \
      -e NIFI_SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER=email \
      -e NIFI_SECURITY_USER_OIDC_TRUSTSTORE_STRATEGY=PKIX \
      -e NIFI_SECURITY_USER_OIDC_TOKEN_REFRESH_WINDOW='60 secs' \
      -d \
      apache/nifi:latest

- Make sure you've created realm, client and user in OpenID Server before with the same user name defined in `INITIAL_ADMIN_IDENTITY` environment variable
- You can read more information about these Nifi security OIDC configurations in this following link: [https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#openid_connect](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#openid_connect)

#### Clustering can be enabled by using the following properties to Docker environment variable mappings.

##### nifi.properties

| Property                                  | Environment Variable                   |
|-------------------------------------------|----------------------------------------|
| nifi.cluster.is.node                      | NIFI_CLUSTER_IS_NODE                   |
| nifi.cluster.node.address                 | NIFI_CLUSTER_ADDRESS                   |
| nifi.cluster.node.protocol.port           | NIFI_CLUSTER_NODE_PROTOCOL_PORT        |
| nifi.cluster.node.protocol.max.threads    | NIFI_CLUSTER_NODE_PROTOCOL_MAX_THREADS |
| nifi.zookeeper.connect.string             | NIFI_ZK_CONNECT_STRING                 |
| nifi.zookeeper.root.node                  | NIFI_ZK_ROOT_NODE                      |
| nifi.cluster.flow.election.max.wait.time  | NIFI_ELECTION_MAX_WAIT                 |
| nifi.cluster.flow.election.max.candidates | NIFI_ELECTION_MAX_CANDIDATES           |

##### state-management.xml

| Property Name  | Environment Variable   |
|----------------|------------------------|
| Connect String | NIFI_ZK_CONNECT_STRING |
| Root Node      | NIFI_ZK_ROOT_NODE      |


### Using the Toolkit

Start the container:

    docker run -d --name nifi apache/nifi

After NiFi has been started, it is possible to run toolkit commands against the running instance:

    docker exec -ti nifi nifi-toolkit-current/bin/cli.sh nifi current-user
    anonymous

## Configuration Information
The following ports are specified by default in Docker for NiFi operation within the container and
can be published to the host.

| Function                 | Property                      | Port  |
|--------------------------|-------------------------------|-------|
| HTTPS Port               | nifi.web.https.port           | 8443  |
| Remote Input Socket Port | nifi.remote.input.socket.port | 10000 |
| JVM Debugger             | java.arg.debug                | 8000  |

The JVM Memory initial and maximum heap size can be set using the `NIFI_JVM_HEAP_INIT` and `NIFI_JVM_HEAP_MAX` environment variables. These use values acceptable to the JVM `Xmx` and `Xms` parameters such as `1g` or `512m`.

The JVM Debugger can be enabled by setting the environment variable NIFI_JVM_DEBUGGER to any value.

=======
**NOTE**: If NiFi is proxied at context paths other than the root path of the proxy, the paths need to be set in the
_nifi.web.proxy.context.path_ property, which can be assigned via the environment variable _NIFI\_WEB\_PROXY\_CONTEXT\_PATH_.

**NOTE**: If mapping the HTTPS port specifying trusted hosts should be provided for the property _nifi.web.proxy.host_.  This property can be specified to running instances
via specifying an environment variable at container instantiation of _NIFI\_WEB\_PROXY\_HOST_.
