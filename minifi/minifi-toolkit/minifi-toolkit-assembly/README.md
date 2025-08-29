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
# Apache NiFi - MiNiFi - Toolkit

MiNiFi is a child project effort of Apache NiFi.  The MiNiFi toolkit aids in creating MiNiFi configuration files from exported NiFi templates.

## Table of Contents

- [Requirements](#requirements)
- [MiNiFi Toolkit Converter](#minifi-toolkit-converter)
- [Encrypting Sensitive Properties in bootstrap.conf](#encrypt-sensitive-properties-in-bootstrapconf)
- [Getting Help](#getting-help)
- [Documentation](#documentation)
- [License](#license)
- [Export Control](#export-control)

## Requirements
* JRE 21

The latest version of the MiNiFi Toolkit can be found at https://nifi.apache.org/download/#minifi under the `MiNiFi Toolkit Binaries` section.

# <a id="minifi-toolkit-converter" href="#minifi-toolkit-converter">MiNiFi Toolkit Converter</a>

After downloading the binary and extracting it, to run the MiNiFi Toolkit Converter:
- Change directory to the location where you installed MiNiFi Toolkit and run it and view usage information
  - Linux / OS X
        $ ./config.sh

  - Windows
      execute bin/config.bat

- Usage Information

      Usage:

      java org.apache.nifi.minifi.toolkit.configuration.ConfigMain <command> options

      Valid commands include:
     - transform-nifi: Transform NiFi2 flow JSON format into MiNifi flow JSON format
     - transform-yml: Transforms legacy MiNiFi flow config YAML into MiNiFi flow config JSON

## Example
- You have an older version of MiNiFi located in <legacy_minifi_directory>.
- You would like upgrade to the latest version of MiNiFi. You downloaded and extracted the latest MiNiFi into <latest_minifi_directory>.
- Run the following command to migrate the flow and the bootstrap config
```
./config.sh transform-yml <legacy_minifi_directory>/conf/config.yml <legacy_minifi_directory>/conf/bootstrap.conf <latest_minifi_directory>/conf/flow.json.raw <latest_minifi_directory>/conf/bootstrap.conf
```
- Run the following command to transform NiFi flow JSON format into MiNifi flow JSON format
```
./config.sh transform-nifi <downloaded_nifi2_flow_json_file> <minifi_flow_json_file> 
```

## Note
It's not guaranteed in all circumstances that the migration will result in a correct flow. For example if a processor's configuration has changed between version, the conversion tool won't be aware of this, and will use the deprecated property names. You will need to fix such issues manually.

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
