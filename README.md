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

# Apache NiFi

<img src="https://nifi.apache.org/images/apache-nifi-logo.svg" width="300" alt="Apache NiFi"/>

### Status

[![ci-workflow](https://github.com/apache/nifi/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi/actions/workflows/ci-workflow.yml)
[![system-tests](https://github.com/apache/nifi/workflows/system-tests/badge.svg)](https://github.com/apache/nifi/actions/workflows/system-tests.yml)
[![integration-tests](https://github.com/apache/nifi/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/apache/nifi/actions/workflows/integration-tests.yml)
[![docker-tests](https://github.com/apache/nifi/actions/workflows/docker-tests.yml/badge.svg)](https://github.com/apache/nifi/actions/workflows/docker-tests.yml)
[![code-compliance](https://github.com/apache/nifi/actions/workflows/code-compliance.yml/badge.svg)](https://github.com/apache/nifi/actions/workflows/code-compliance.yml)
[![code-coverage](https://github.com/apache/nifi/actions/workflows/code-coverage.yml/badge.svg)](https://github.com/apache/nifi/actions/workflows/code-coverage.yml)
[![codecov](https://codecov.io/gh/apache/nifi/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/nifi)

### Resources

[![NiFi API](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-api.svg?label=nifi-api&logo=apachenifi&logoColor=ffffff&color=728e9b)](https://central.sonatype.com/artifact/org.apache.nifi/nifi-api)
[![NiFi NAR Maven Plugin](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-nar-maven-plugin.svg?label=nifi-nar-maven-plugin&logo=apachenifi&logoColor=ffffff&color=728e9b)](https://central.sonatype.com/artifact/org.apache.nifi/nifi-nar-maven-plugin)
[![NiFi Framework](https://img.shields.io/maven-central/v/org.apache.nifi/nifi.svg?label=nifi-framework&logo=apachenifi&logoColor=ffffff&color=728e9b)](https://nifi.apache.org/download/)
[![NiFI Docker Pulls](https://img.shields.io/docker/pulls/apache/nifi.svg?logo=docker&logoColor=ffffff)](https://hub.docker.com/r/apache/nifi/)
[![License](https://img.shields.io/github/license/apache/nifi)](https://github.com/apache/nifi/blob/main/LICENSE)
[![NiFi API Javadoc](https://javadoc.io/badge2/org.apache.nifi/nifi-api/javadoc.svg)](https://javadoc.io/doc/org.apache.nifi/nifi-api)

### Contacts

[![Track Issues](https://img.shields.io/badge/track-Issues-728e9b.svg?logo=jirasoftware)](https://issues.apache.org/jira/browse/NIFI)
[![Chat on Slack](https://img.shields.io/badge/chat-Slack-728e9b.svg?logo=slack)](https://s.apache.org/nifi-community-slack)
[![Contact Developers](https://img.shields.io/badge/contact-Developers-728e9b.svg?logo=apache)](https://lists.apache.org/list.html?dev@nifi.apache.org)
[![Contact Users](https://img.shields.io/badge/contact-Users-728e9b.svg?logo=apache)](https://lists.apache.org/list.html?users@nifi.apache.org)

### Community

[![Join Slack Community](https://img.shields.io/badge/join-Slack-728e9b.svg?logo=slack)](https://join.slack.com/t/apachenifi/shared_invite/zt-11njbtkdx-ZRU8FKYSWoEHRJetidy0zA)
[![Follow on LinkedIn](https://img.shields.io/badge/follow-Apache%20NiFi-728e9b.svg?logo=linkedin)](https://www.linkedin.com/company/apache-nifi/)
[![Follow on X](https://img.shields.io/badge/follow-apachenifi-728e9b.svg?logo=x)](https://x.com/apachenifi)

## Features

[Apache NiFi](https://nifi.apache.org/) is an easy to use, powerful, and reliable system to process and distribute data.

NiFi automates cybersecurity, observability, event streams, and generative AI data pipelines and distribution
for thousands of companies worldwide across every industry.

- Browser User Interface
  - Seamless experience for design, control, and monitoring
  - Runtime management and versioned pipelines
  - Secure by default with HTTPS
- Scalable Processing
  - Configurable prioritization for throughput and latency
  - Guaranteed delivery with retry and backoff strategies
  - Horizontal scaling with clustering
- Provenance Tracking 
  - Searchable history with configurable attributes
  - Graph data lineage from source to destination
  - Metadata and content for each processing decision
- Extensible Design
  - Plugin interface for Processors and Controller Services
  - Support for Processors in native Python
  - REST API for orchestration and monitoring
- Secure Configuration
  - Single sign-on with OpenID Connect or SAML 2
  - Flexible authorization policies for role-based access
  - Encrypted communication with TLS and SFTP

## Requirements

NiFi supports modern operating systems and requires recent language versions for developing and running the application.

### Platform Requirements

- Java 21

### Optional Dependencies

- Python 3.10 or higher

## Projects

The source repository includes several component projects.

Please review individual project documentation for additional details.

- [Apache NiFi](https://nifi.apache.org/documentation/)
- [Apache NiFi Registry](https://github.com/apache/nifi/blob/main/nifi-registry/nifi-registry-assembly/README.md)
- [Apache NiFi MiNiFi](https://github.com/apache/nifi/blob/main/minifi/minifi-assembly/README.md)

## Getting Started

Project guides provide extensive documentation for installing and extending the application.

- [Getting Started](https://nifi.apache.org/documentation/nifi-latest/html/getting-started.html)
- [User Guide](https://nifi.apache.org/documentation/nifi-latest/html/user-guide.html)
- [Administrator Guide](https://nifi.apache.org/documentation/nifi-latest/html/administration-guide.html)
- [Developer Guide](https://nifi.apache.org/documentation/nifi-latest/html/developer-guide.html)

## Developing

NiFi uses the [Maven Wrapper](https://maven.apache.org/wrapper/) for project development. The Maven Wrapper provides
shell scripts that download and cache a selected version of [Apache Maven](https://maven.apache.org/) for running build
commands.

Developing on Microsoft Windows requires using `mvnw.cmd` instead of `mvnw` to run Maven commands.

### Building

Run the following command to build project modules using parallel execution:

```shell
./mvnw install -T1C
```

Run the following command to build project modules using parallel execution with static analysis to confirm compliance
with code and licensing requirements:

```shell
./mvnw install -T1C -P contrib-check
```

Run the following command to build the application binaries without building other optional modules:

```shell
./mvnw install -T1C -am -pl :nifi-assembly
```

### Binaries

The `nifi-assembly` module contains the binary distribution.

```shell
ls nifi-assembly/target/nifi-*-bin.zip
```

The `nifi-assembly` module includes the binary distribution in a directory for local development and testing.

```shell
cd nifi-assembly/target/nifi-*-bin/nifi-*/
```

## Running

NiFi provides shell scripts for starting and stopping the system.

Running on Microsoft Windows requires using `nifi.cmd` instead of `nifi.sh` for system commands.

### Starting

Run the following command to start NiFi from the distribution directory:

```shell
./bin/nifi.sh start
```

### Accessing

The default configuration generates a random username and password on startup. NiFi writes the generated credentials
to the application log located in `logs/nifi-app.log` under the NiFi installation directory.

The following command can be used to find the generated credentials on operating systems with `grep` installed:

```shell
grep Generated logs/nifi-app*log
```

NiFi logs the generated credentials as follows:

```shell
Generated Username [USERNAME]
Generated Password [PASSWORD]
```

The `USERNAME` will be a random UUID composed of 36 characters. The `PASSWORD` will be a random string.

The username and password can be replaced with custom credentials using the following command:

```shell
./bin/nifi.sh set-single-user-credentials <username> <password>
```

NiFi defaults to running on the `localhost` address with HTTPS on port `8443` at the following URL:

```
https://localhost:8443/nifi
```

Browsers will display a warning message indicating a potential security risk due to the self-signed certificate
generated during initialization. Production deployments should provision a certificate from a trusted certificate
authority and update the NiFi keystore and truststore configuration.

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Export Control

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See https://www.wassenaar.org for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache NiFi uses the following libraries and frameworks for encrypted
communication and storage of sensitive information:

- [Apache MINA SSHD](https://mina.apache.org/sshd-project/)
- [Bouncy Castle](https://www.bouncycastle.org)
- [Jagged](https://github.com/exceptionfactory/jagged)
- [Java Cryptography Architecture](https://docs.oracle.com/en/java/javase/21/security/java-cryptography-architecture-jca-reference-guide.html)
- [SSHJ](https://github.com/hierynomus/sshj)
