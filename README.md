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

[![Build Status](https://travis-ci.org/apache/nifi.svg?branch=master)](https://travis-ci.org/apache/nifi)
[![Docker pulls](https://img.shields.io/docker/pulls/apache/nifi.svg)](https://hub.docker.com/r/apache/nifi/)
[![Version](https://img.shields.io/maven-central/v/org.apache.nifi/nifi-utils.svg)](https://nifi.apache.org/download.html)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://join.slack.com/t/apachenifi/shared_invite/enQtNDI2NDMyMTY3MTA5LWJmZDI3MmM1ZmYyODQwZDYwM2MyMDY5ZjkyMDkxY2JmOGMyNmEzYTE0MTRkZTYwYzZlYTJkY2JhZTYyMzcyZGI)

[Apache NiFi](https://nifi.apache.org/) is an easy to use, powerful, and
reliable system to process and distribute data.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
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
  - Scales out with zero-master clustering model
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
* JDK 1.8 or newer
* Apache Maven 3.1.0 or newer
* Git Client (used during build process by 'bower' plugin)

## Getting Started

- Read through the [quickstart guide for development](http://nifi.apache.org/quickstart.html).
  It will include information on getting a local copy of the source, give pointers on issue
  tracking, and provide some warnings about common problems with development environments.
- For a more comprehensive guide to development and information about contributing to the project
  read through the [NiFi Developer's Guide](http://nifi.apache.org/developer-guide.html).

To build:
- Execute `mvn clean install` or for parallel build execute `mvn -T 2.0C clean install`. On a
  modest development laptop that is a couple of years old, the latter build takes a bit under ten
  minutes. After a large amount of output you should eventually see a success message.

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

To deploy:
- Change directory to 'nifi-assembly'. In the target directory, there should be a build of nifi.

        laptop:nifi myuser$ cd nifi-assembly
        laptop:nifi-assembly myuser$ ls -lhd target/nifi*
        drwxr-xr-x  3 myuser  mygroup   102B Apr 30 00:29 target/nifi-1.0.0-SNAPSHOT-bin
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 myuser  mygroup   144M Apr 30 00:30 target/nifi-1.0.0-SNAPSHOT-bin.zip

- For testing ongoing development you could use the already unpacked build present in the directory
  named "nifi-*version*-bin", where *version* is the current project version. To deploy in another
  location make use of either the tarball or zipfile and unpack them wherever you like. The
  distribution will be within a common parent directory named for the version.

        laptop:nifi-assembly myuser$ mkdir ~/example-nifi-deploy
        laptop:nifi-assembly myuser$ tar xzf target/nifi-*-bin.tar.gz -C ~/example-nifi-deploy
        laptop:nifi-assembly myuser$ ls -lh ~/example-nifi-deploy/
        total 0
        drwxr-xr-x  10 myuser  mygroup   340B Apr 30 01:06 nifi-1.0.0-SNAPSHOT

To run NiFi:
- Change directory to the location where you installed NiFi and run it.

        laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh start

- Direct your browser to http://localhost:8080/nifi/ and you should see a screen like this screenshot:
  ![image of a NiFi dataflow canvas](nifi-docs/src/main/asciidoc/images/nifi_first_launch_screenshot.png?raw=true)

- For help building your first data flow see the [NiFi User Guide](http://nifi.apache.org/docs/nifi-docs/html/user-guide.html)

- If you are testing ongoing development, you will likely want to stop your instance.

        laptop:~ myuser$ cd ~/example-nifi-deploy/nifi-*
        laptop:nifi-1.0.0-SNAPSHOT myuser$ ./bin/nifi.sh stop

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/nifi-dev)).
For more interactive conversations and chat, we're also often available in IRC: #nifi on
[irc.freenode.net](http://webchat.freenode.net/?channels=#nifi) and in #NiFi on [ASF HipChat](https://www.hipchat.com/gzh2m5YML). 

## Documentation

See http://nifi.apache.org/ for the latest documentation.

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

## Export Control

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
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
http://bouncycastle.org/about.html
http://www.jcraft.com/c-info.html
http://www.oracle.com/us/products/export/export-regulations-345813.html
for more details on each of these libraries cryptography features.

[nifi]: https://nifi.apache.org/
[logo]: https://nifi.apache.org/assets/images/apache-nifi-logo.svg
