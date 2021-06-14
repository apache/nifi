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
[<img src="https://nifi.apache.org/assets/images/apache-nifi-logo.svg" width="300" height="126" alt="Apache NiFi"/>][nifi]

[![ci-workflow](https://github.com/apache/nifi/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi/actions)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi.svg)](https://hub.docker.com/r/apache/nifi/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-utils.svg)](https://nifi.apache.org/download.html)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://s.apache.org/nifi-community-slack)

[Apache NiFi](https://nifi.apache.org/) is an easy to use, powerful, and
reliable system to process and distribute data.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [MiNiFi subproject](#minifi-subproject)
- [Registry subproject](#registry-subproject)
- [Getting Help](#getting-help)
- [Documentation](#documentation)
- [License](#license)
- [Export Control](#export-control)

## Features

Apache NiFi was made for dataflow. It supports highly configurable directed graphs of data routing, transformation, and system mediation logic. Some of its key features include:

- Web-based user interface
  - Seamless experience for design, control, and monitoring
  - Multi-tenant user experience
- Highly configurable
  - Loss tolerant vs guaranteed delivery
  - Low latency vs high throughput
  - Dynamic prioritization
  - Flows can be modified at runtime
  - Back pressure
  - Scales up to leverage full machine capability
  - Scales out with zero-leader clustering model
- Data Provenance
  - Track dataflow from beginning to end
- Designed for extension
  - Build your own processors and more
  - Enables rapid development and effective testing
- Secure
  - SSL, SSH, HTTPS, encrypted content, etc...
  - Pluggable fine-grained role-based authentication/authorization
  - Multiple teams can manage and share specific portions of the flow

## Requirements
* JDK 1.8 (*ongoing work to enable NiFi to run on Java 9/10/11; see [NIFI-5174](https://issues.apache.org/jira/browse/NIFI-5174)*)
* Apache Maven 3.1.1 or newer
* Git Client (used during build process by 'bower' plugin)

## Getting Started

Read through the [quickstart guide for development](https://nifi.apache.org/quickstart.html).
It will include information on getting a local copy of the source, give pointers on issue
tracking, and provide some warnings about common problems with development environments.

For a more comprehensive guide to development and information about contributing to the project
read through the [NiFi Developer's Guide](https://nifi.apache.org/developer-guide.html).

### Building

Run `mvn clean install` or for parallel build execute `mvn -T 2.0C clean install`.

The parallel build should take around fifteen minutes on modern hardware.

    laptop:nifi myuser$ mvn -T 2.0C clean install
    [INFO] Scanning for projects...
    [INFO] Inspecting build with total of 115 modules...
        ...tens of thousands of lines elided...
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 09:24 min (Wall Clock)
    [INFO] Finished at: 2015-04-30T00:30:36-05:00
    [INFO] Final Memory: 173M/1359M
    [INFO] ------------------------------------------------------------------------

Run `mvn clean install -DskipTests` to skip unit test execution.

### Deploying

Change directories to `nifi-assembly`. The `target` directory contains binary archives.

    laptop:nifi myuser$ cd nifi-assembly
    laptop:nifi-assembly myuser$ ls -lhd target/nifi*
    drwxr-xr-x  3 myuser  mygroup   102B Apr 30 00:29 target/nifi-1.0.0-SNAPSHOT-bin
    -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.tar.gz
    -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.zip

Copy the `nifi-VERSION-bin.tar.gz` or `nifi-VERSION-bin.zip` to a separate deployment directory.
Extracting the distribution will create a new directory named for the version.

    laptop:nifi-assembly myuser$ mkdir ~/example-nifi-deploy
    laptop:nifi-assembly myuser$ tar xzf target/nifi-*-bin.tar.gz -C ~/example-nifi-deploy
    laptop:nifi-assembly myuser$ ls -lh ~/example-nifi-deploy/
    total 0
    drwxr-xr-x  10 myuser  mygroup   340B Apr 30 01:06 nifi-1.0.0-SNAPSHOT

### Starting

Change directories to the deployment location and run the following command to start NiFi.

    laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
    laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start
  
Running `bin/nifi.sh start` starts NiFi in the background and exits. Use `--wait-for-init` with an optional timeout in
seconds to wait for a complete startup before exiting.

    laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start --wait-for-init 120
  
### Authenticating

The default configuration generates a random username and password on startup. NiFi writes the generated credentials
to the application log located in `logs/nifi-app.log` under the NiFi installation directory.

The following command can be used to find the generated credentials on operating systems with `grep` installed:

    laptop:nifi-1.0.0-SNAPSHOT myuser$ grep Generated logs/nifi-app*log

NiFi logs the generated credentials as follows:

    Generated Username [USERNAME]
    Generated Password [PASSWORD]

The `USERNAME` will be a random UUID composed of 36 characters. The `PASSWORD` will be a random string composed of
32 characters. The generated credentials will be stored in `conf/login-identity-providers.xml` with the password stored
using bcrypt hashing. Record these credentials in a secure location for access to NiFi.

The random username and password can be replaced with custom credentials using the following command:

    ./bin/nifi.sh set-single-user-credentials <username> <password>
  
### Running

Open the following link in a web browser to access NiFi: https://localhost:8443/nifi

The web browser will display a warning message indicating a potential security risk due to the self-signed
certificate NiFi generated during initialization. Accepting the potential security risk and continuing to load
the interface is an option for initial development installations. Production deployments should provision a certificate
from a trusted certificate authority and update the NiFi keystore and truststore configuration.

Accessing NiFi after accepting the self-signed certificate will display the login screen.
![NiFi Login Screen](nifi-docs/src/main/asciidoc/images/nifi-login.png?raw=true)

Using the generated credentials, enter the generated username in the `User` field
and the generated password in the `Password` field, then select `LOG IN` to access the system.
![NiFi Flow Authenticated Screen](nifi-docs/src/main/asciidoc/images/nifi-flow-authenticated.png?raw=true)

### Configuring

The [NiFi User Guide](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html) describes how to build a data flow.

### Stopping

Run the following command to stop NiFi:

    laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
    laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh stop

## MiNiFi subproject

MiNiFi is a child project effort of Apache NiFi. It is a complementary data collection approach that supplements the core tenets of [NiFi](https://nifi.apache.org/) in dataflow management, focusing on the collection of data at the source of its creation.

Specific goals for MiNiFi are comprised of:
- small and lightweight footprint
- central management of agents
- generation of data provenance
- integration with NiFi for follow-on dataflow management and full chain of custody of information

Perspectives of the role of MiNiFi should be from the perspective of the agent acting immediately at, or directly adjacent to, source sensors, systems, or servers.

To run:
- Change directory to 'minifi-assembly'. In the target directory, there should be a build of minifi.

        $ cd minifi-assembly
        $ ls -lhd target/minifi*
        drwxr-xr-x  3 user  staff   102B Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-1.14.0-SNAPSHOT-bin.zip

- For testing ongoing development you could use the already unpacked build present in the directory
  named "minifi-*version*-bin", where *version* is the current project version. To deploy in another
  location make use of either the tarball or zipfile and unpack them wherever you like. The
  distribution will be within a common parent directory named for the version.

        $ mkdir ~/example-minifi-deploy
        $ tar xzf target/minifi-*-bin.tar.gz -C ~/example-minifi-deploy
        $ ls -lh ~/example-minifi-deploy/
        total 0
        drwxr-xr-x  10 user  staff   340B Jul 6 01:06 minifi-1.14.0-SNAPSHOT

To run MiNiFi:
- Change directory to the location where you installed MiNiFi and run it.

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh start

- View the logs located in the logs folder
  $ tail -F ~/example-minifi-deploy/logs/minifi-app.log

- For help building your first data flow and sending data to a NiFi instance see the System Admin Guide located in the docs folder or making use of the minifi-toolkit, which aids in adapting NiFi templates to MiNiFi YAML configuration file format.

- If you are testing ongoing development, you will likely want to stop your instance.

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh stop

### Docker Build

To build:
- Run a full NiFi build (see above for instructions). Then from the minifi/ subdirectory, execute `mvn -P docker clean install`.  This will run the full build, create a docker image based on it, and run docker-compose integration tests.  After it completes successfully, you should have an apacheminifi:${minifi.version} image that can be started with the following command (replacing ${minifi.version} with the current maven version of your branch):
```
docker run -d -v YOUR_CONFIG.YML:/opt/minifi/minifi-${minifi.version}/conf/config.yml apacheminifi:${minifi.version}
```

## Registry subproject

Registry—a subproject of Apache NiFi—is a complementary application that provides a central location for storage and management of shared resources across one or more instances of NiFi and/or MiNiFi.

### Getting Registry Started

1) Build NiFi (see [Getting Started for NiFi](#getting-started) )
    
or
    
Build only the Registry subproject:

    cd nifi/nifi-registry
    mvn clean install

    
If you wish to enable style and license checks, specify the contrib-check profile:

    mvn clean install -Pcontrib-check


2) Start Registry

    cd nifi-registry/nifi-registry-assembly/target/nifi-registry-<VERSION>-bin/nifi-registry-<VERSION>/
    ./bin/nifi-registry.sh start

Note that the application web server can take a while to load before it is accessible.   

3) Accessing the application web UI
 
With the default settings, the application UI will be available at [http://localhost:18080/nifi-registry](http://localhost:18080/nifi-registry) 
   
4) Accessing the application REST API

If you wish to test against the application REST API, you can access the REST API directly. With the default settings, the base URL of the REST API will be at `http://localhost:18080/nifi-registry-api`. A UI for testing the REST API will be available at [http://localhost:18080/nifi-registry-api/swagger/ui.html](http://localhost:18080/nifi-registry-api/swagger/ui.html) 

5) Accessing the application logs

Logs will be available in `logs/nifi-registry-app.log`

### Database Testing

In order to ensure that NiFi Registry works correctly against different relational databases, 
the existing integration tests can be run against different databases by leveraging the [Testcontainers framework](https://www.testcontainers.org/).

Spring profiles are used to control the DataSource factory that will be made available to the Spring application context. 
DataSource factories are provided that use the Testcontainers framework to start a Docker container for a given database and create a corresponding DataSource. 
If no profile is specified then an H2 DataSource will be used by default and no Docker containers are required.

Assuming Docker is running on the system where the build is running, then the following commands can be run:

| Target Database | Build Command | 
| --------------- | ------------- |
| All supported   | `mvn verify -Ptest-all-dbs` |
| H2 (default)    | `mvn verify` |
| PostgreSQL 9.x  | `mvn verify -Dspring.profiles.active=postgres` | 
| PostgreSQL 10.x | `mvn verify -Dspring.profiles.active=postgres-10` | 
| MySQL 5.6       | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-56` |
| MySQL 5.7       | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-57` |
| MySQL 8         | `mvn verify -Pcontrib-check -Dspring.profiles.active=mysql-8`  |
      
 When one of the Testcontainer profiles is activated, the test output should show logs that indicate a container has been started, such as the following:
 
    2019-05-15 16:14:45.078  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Creating container for image: mysql:5.7
    2019-05-15 16:14:45.145  INFO 66091 --- [           main] o.t.utility.RegistryAuthLocator          : Credentials not found for host (index.docker.io) when using credential helper/store (docker-credential-osxkeychain)
    2019-05-15 16:14:45.646  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Starting container with ID: ca85c8c5a1990d2a898fad04c5897ddcdb3a9405e695cc11259f50f2ebe67c5f
    2019-05-15 16:14:46.437  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Container mysql:5.7 is starting: ca85c8c5a1990d2a898fad04c5897ddcdb3a9405e695cc11259f50f2ebe67c5f
    2019-05-15 16:14:46.479  INFO 66091 --- [           main] 🐳 [mysql:5.7]                           : Waiting for database connection to become available at jdbc:mysql://localhost:33051/test?useSSL=false&allowPublicKeyRetrieval=true using query 'SELECT 1'

The Flyway connection should also indicate the given database:

    2019-05-15 16:15:02.114  INFO 66091 --- [           main] o.a.n.r.db.CustomFlywayConfiguration     : Determined database type is MYSQL
    2019-05-15 16:15:02.115  INFO 66091 --- [           main] o.a.n.r.db.CustomFlywayConfiguration     : Setting migration locations to [classpath:db/migration/common, classpath:db/migration/mysql]
    2019-05-15 16:15:02.373  INFO 66091 --- [           main] o.a.n.r.d.CustomFlywayMigrationStrategy  : First time initializing database...
    2019-05-15 16:15:02.380  INFO 66091 --- [           main] o.f.c.internal.license.VersionPrinter    : Flyway Community Edition 5.2.1 by Boxfuse
    2019-05-15 16:15:02.403  INFO 66091 --- [           main] o.f.c.internal.database.DatabaseFactory  : Database: jdbc:mysql://localhost:33051/test (MySQL 5.7)

For a full list of the available DataSource factories, consult the `nifi-registry-test` module.

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.apache.org
([archive](https://lists.apache.org/list.html?dev@nifi.apache.org)). For more interactive discussions, community members can often be found in the following locations:

- Apache NiFi Slack Workspace: https://apachenifi.slack.com/

  New users can join the workspace using the following [invite link](https://s.apache.org/nifi-community-slack).
  
- IRC: #nifi on [irc.freenode.net](https://webchat.freenode.net/?channels=#nifi)

To submit a feature request or bug report, please file a Jira at [https://issues.apache.org/jira/projects/NIFI/issues](https://issues.apache.org/jira/projects/NIFI/issues). If this is a **security vulnerability report**, please email [security@nifi.apache.org](mailto:security@nifi.apache.org) directly and review the [Apache NiFi Security Vulnerability Disclosure](https://nifi.apache.org/security.html) and [Apache Software Foundation Security](https://www.apache.org/security/committers.html) processes first. 

## Documentation

See https://nifi.apache.org/ for the latest NiFi documentation.

See https://nifi.apache.org/minifi and https://cwiki.apache.org/confluence/display/MINIFI for the latest MiNiFi-specific documentation.

See https://nifi.apache.org/registry for the latest Registry-specific documentation.

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
software, to see if this is permitted. See <https://www.wassenaar.org/> for more
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

Apache NiFi uses BouncyCastle, JCraft Inc., and the built-in
Java cryptography libraries for SSL, SSH, and the protection
of sensitive configuration parameters. See

- https://bouncycastle.org/about.html
- http://www.jcraft.com/c-info.html
- https://www.oracle.com/corporate/security-practices/corporate/governance/global-trade-compliance.html

for more details on each of these libraries cryptography features.

[nifi]: https://nifi.apache.org/
[logo]: https://nifi.apache.org/assets/images/apache-nifi-logo.svg
