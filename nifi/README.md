# Apache NiFi

Apache NiFi is a dataflow system based on the concepts of flow-based programming. It is currently apart of the Apache Incubator.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Getting Help](#getting-help)
- [Requirements](#requirements)
- [License](#license)
- [Disclaimer](#disclaimer)
- [Export Control] (#export-control)

## Features

Apache NiFi supports powerful and scalable directed graphs of data routing, transformation, and system mediation logic. Some of the high-level capabilities and objectives of Apache NiFi include:

- Web-based user interface for seamless experience between design, control, feedback, and monitoring of data flows
- Highly configurable along several dimensions of quality of service such as loss tolerant versus guaranteed delivery, low latency versus high throughput, and priority based queuing
- Fine-grained data provenance for all data received, forked, joined, cloned, modified, sent, and ultimately dropped as data reaches its configured end-state
- Component-based extension model along well defined interfaces enabling rapid development and effective testing 

## Getting Started

To build:
- Execute 'mvn clean install' or for parallel build execute 'mvn -T 2.0C clean install'

To start NiFi:
- Change directory to 'assembly'.  In the target directory there should be a build of nifi.
- Unpack the build wherever you like or use the already unpacked build.  '<install_location>/bin/nifi.sh start'
- Direct your browser to http://localhost:8080/nifi/

## Getting Help
If you have questions, you can reach out to our mailing list: dev@nifi.incubator.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/incubator-nifi-dev)).
We're also often available in IRC: #nifi on
[irc.freenode.net](http://webchat.freenode.net/?channels=#nifi).

## Requirements
* JDK 1.7 or higher

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

## Disclaimer

Apache NiFi is an effort undergoing incubation at the Apache Software
Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.

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
