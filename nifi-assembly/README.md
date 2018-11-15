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

Apache NiFi is an easy to use, powerful, and reliable system to process and distribute data.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Getting Help](#getting-help)
- [Requirements](#requirements)
- [License](#license)
- [Export Control] (#export-control)

## Features

Apache NiFi was made for dataflow. It supports highly configurable directed graphs of data routing, transformation, and system mediation logic. Some of its key features include:

- Web-based user interface
  - Seamless experience for design, control, and monitoring
- Highly configurable
  - Loss tolerant vs guaranteed delivery
  - Low latency vs high throughput
  - Dynamic prioritization
  - Flows can be modified at runtime
  - Back pressure
- Data Provenance
  - Track dataflow from beginning to end
- Designed for extension
  - Build your own processors and more
  - Enables rapid development and effective testing
- Secure
  - SSL, SSH, HTTPS, encrypted content, etc...
  - Pluggable role-based authentication/authorization

## Getting Started

To start NiFi:
- [linux/osx] execute bin/nifi.sh start
- [windows] execute bin/run-nifi.bat
- Direct your browser to http://localhost:8080/nifi/

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/nifi-dev)).
We're also often available in IRC: #nifi on
[irc.freenode.net](http://webchat.freenode.net/?channels=#nifi).

## Requirements
* JDK 1.8 or higher

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

Apache NiFi uses BouncyCastle, Jasypt, JCraft Inc., and the built-in 
java cryptography libraries for SSL, SSH, and the protection
of sensitive configuration parameters. See 
http://bouncycastle.org/about.html
http://www.jasypt.org/faq.html
http://jcraft.com/c-info.html
http://www.oracle.com/us/products/export/export-regulations-345813.html
for more details on each of these libraries cryptography features.
