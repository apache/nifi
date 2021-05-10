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
[<img src="https://nifi.apache.org/assets/images/registry-logo.png" width="360" height="126" alt="Apache NiFi Registry" />][nifi-registry]

[![ci-workflow](https://github.com/apache/nifi-registry/workflows/ci-workflow/badge.svg)](https://github.com/apache/nifi-registry/actions)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi-registry.svg)](https://hub.docker.com/r/apache/nifi-registry/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi.registry/nifi-registry-framework.svg)](https://nifi.apache.org/registry)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://s.apache.org/nifi-community-slack)

Registry—a subproject of Apache NiFi—is a complementary application that provides a central location for storage and management of shared resources across one or more instances of NiFi and/or MiNiFi.

## Table of Contents

- [Getting Started](#getting-started)
- [License](#license)

## Getting Started

### Requirements
    
* Java 1.8 (above 1.8.0_45)
* Maven 3.1.0, or newer
* Recent git client

1) Clone the repo

        git clone https://git-wip-us.apache.org/repos/asf/nifi-registry.git
        git checkout main

2) Build the project

        cd nifi-registry
        mvn clean install

    If you wish to enable style and license checks, specify the contrib-check profile:
    
        mvn clean install -Pcontrib-check

3) Start the application

        cd nifi-registry-assembly/target/nifi-registry-<VERSION>-bin/nifi-registry-<VERSION>/
        ./bin/nifi-registry.sh start
   
   Note that the application web server can take a while to load before it is accessible.   

4) Accessing the application web UI
 
    With the default settings, the application UI will be available at [http://localhost:18080/nifi-registry](http://localhost:18080/nifi-registry) 
   
5) Accessing the application REST API

    If you wish to test against the application REST API, you can access the REST API directly. With the default settings, the base URL of the REST API will be at `http://localhost:18080/nifi-registry-api`. A UI for testing the REST API will be available at [http://localhost:18080/nifi-registry-api/swagger/ui.html](http://localhost:18080/nifi-registry-api/swagger/ui.html) 

6) Accessing the application logs

    Logs will be available in `logs/nifi-registry-app.log`

## Database Testing

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

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[nifi-registry]: https://nifi.apache.org/registry
