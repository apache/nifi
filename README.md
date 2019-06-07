<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      https://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache NiFi -  MiNiFi [![Build Status](https://travis-ci.org/apache/nifi-minifi.svg?branch=master)](https://travis-ci.org/apache/nifi-minifi)

MiNiFi is a child project effort of Apache NiFi

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Docker Build](#docker-build)
- [Getting Help](#getting-help)
- [Documentation](#documentation)
- [License](#license)
- [Export Control](#export-control)

## Features

Apache NiFi - MiNiFi is a complementary data collection approach that supplements the core tenets of [NiFi](https://nifi.apache.org/) in dataflow management, focusing on the collection of data at the source of its creation.

Specific goals for MiNiFi are comprised of:
- small and lightweight footprint
- central management of agents
- generation of data provenance
- integration with NiFi for follow-on dataflow management and full chain of custody of information

Perspectives of the role of MiNiFi should be from the perspective of the agent acting immediately at, or directly adjacent to, source sensors, systems, or servers.

## Requirements
* JDK 1.8 or higher
* Apache Maven 3.1.0 or higher

## Getting Started

To build:
- Execute `mvn clean install` or for parallel build execute `mvn -T 2.0C clean install`. On a
  modest development laptop that is a couple of years old, the latter build takes a bit under two
  minutes. After a large amount of output you should eventually see a success message.

        $ mvn -T 2.0C clean install
        [INFO] Scanning for projects...
        [INFO] Inspecting build with total of 26 modules...
            ...tens of thousands of lines elided...
        [INFO] ------------------------------------------------------------------------
        [INFO] BUILD SUCCESS
        [INFO] ------------------------------------------------------------------------
        [INFO] Total time: 47.373 s (Wall Clock)
        [INFO] Finished at: 2016-07-06T13:07:28-04:00
        [INFO] Final Memory: 36M/1414M
        [INFO] ------------------------------------------------------------------------

To run:
- Change directory to 'minifi-assembly'. In the target directory, there should be a build of minifi.

        $ cd minifi-assembly
        $ ls -lhd target/minifi*
        drwxr-xr-x  3 user  staff   102B Jul  6 13:07 minifi-0.0.1-SNAPSHOT-bin
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-0.0.1-SNAPSHOT-bin.tar.gz
        -rw-r--r--  1 user  staff    39M Jul  6 13:07 minifi-0.0.1-SNAPSHOT-bin.zip

- For testing ongoing development you could use the already unpacked build present in the directory
  named "minifi-*version*-bin", where *version* is the current project version. To deploy in another
  location make use of either the tarball or zipfile and unpack them wherever you like. The
  distribution will be within a common parent directory named for the version.

        $ mkdir ~/example-minifi-deploy
        $ tar xzf target/minifi-*-bin.tar.gz -C ~/example-minifi-deploy
        $ ls -lh ~/example-minifi-deploy/
        total 0
        drwxr-xr-x  10 user  staff   340B Jul 6 01:06 minifi-0.0.1-SNAPSHOT

To run MiNiFi:
- Change directory to the location where you installed NiFi and run it.

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh start

- View the logs located in the logs folder
        $ tail -F ~/example-minifi-deploy/logs/minifi-app.log

- For help building your first data flow and sending data to a NiFi instance see the System Admin Guide located in the docs folder or making use of the minifi-toolkit, which aids in adapting NiFi templates to MiNiFi YAML configuration file format.

- If you are testing ongoing development, you will likely want to stop your instance.

        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh stop

## Docker Build

To build:
- Execute mvn -P docker clean install.  This will run the full build, create a docker image based on it, and run docker-compose integration tests.  After it completes successfully, you should have an apacheminifi:${minifi.version} image that can be started with the following command (replacing ${minifi.version} with the current maven version of your branch):
```
docker run -d -v YOUR_CONFIG.YML:/opt/minifi/minifi-${minifi.version}/conf/config.yml apacheminifi:${minifi.version}
```

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.apache.org
([archive](https://mail-archives.apache.org/mod_mbox/nifi-dev)).
For more interactive conversations and chat, we're also often available in IRC: #nifi on
[irc.freenode.net](https://webchat.freenode.net/?channels=#nifi) and in #NiFi on [ASF HipChat](https://www.hipchat.com/gzh2m5YML). 

## Documentation

See https://nifi.apache.org/minifi and https://cwiki.apache.org/confluence/display/MINIFI for the latest documentation.

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

Apache NiFi - MiNiFi uses BouncyCastle, JCraft Inc., and the built-in
java cryptography libraries for SSL, SSH, and the protection
of sensitive configuration parameters. See
https://bouncycastle.org/about.html
https://jcraft.com/c-info.html
https://www.oracle.com/us/products/export/export-regulations-345813.html
for more details on each of these libraries cryptography features.
