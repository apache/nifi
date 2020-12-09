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
# Apache NiFi - MiNiFi

MiNiFi is a child project effort of Apache NiFi

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
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
* JRE 1.8

## Getting Started

To run MiNiFi:
- Change directory to the location where you installed MiNiFi and run it.
  - Linux / OS X
        $ cd ~/example-minifi-deploy/minifi-*
        $ ./bin/minifi.sh start

  - Windows
      execute bin/run-minifi.bat

- View the logs located in the logs folder
        $ tail -F ~/example-minifi-deploy/logs/minifi-app.log

- For help building your first data flow and sending data to a NiFi instance see the System Admin Guide located in the docs folder or making use of the minifi-toolkit, which aids in adapting NiFi templates to MiNiFi YAML configuration file format.

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.apache.org
([archive](https://mail-archives.apache.org/mod_mbox/nifi-dev)).
We're also often available in IRC: #nifi on
[irc.freenode.net](https://webchat.freenode.net/?channels=#nifi).

## Documentation

See https://nifi.apache.org/minifi and https://cwiki.apache.org/confluence/display/NIFI/MiNiFi for the latest documentation.

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)

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
