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

The latest version of the MiNiFi Toolkit can be found at https://nifi.apache.org/minifi/download.html under the `MiNiFi Toolkit Binaries` section.

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
      transform-yml: Transforms legacy MiNiFi flow config YAML into MiNiFi flow config JSON

## Example
- You have an older version of MiNiFi located in <legacy_minifi_directory>.
- You would like upgrade to the latest version of MiNiFi. You downloaded and extracted the latest MiNiFi into <latest_minifi_directory>.
- Run the following command to migrate the flow and the bootstrap config
```
./config.sh transform-yml <legacy_minifi_directory>/conf/config.yml <legacy_minifi_directory>/conf/bootstrap.conf <latest_minifi_directory>/conf/flow.json.raw <latest_minifi_directory>/conf/bootstrap.conf
```

## Note
It's not guaranteed in all circumstances that the migration will result in a correct flow. For example if a processor's configuration has changed between version, the conversion tool won't be aware of this, and will use the deprecated property names. You will need to fix such issues manually.

# <a id="encrypt-sensitive-properties-in-bootstrapconf" href="#encrypt-sensitive-properties-in-bootstrapconf">Encrypting Sensitive Properties in bootstrap.conf</a>

## MiNiFi Encrypt-Config Tool
The encrypt-config command line tool (invoked in minifi-toolkit as ./bin/encrypt-config.sh or bin\encrypt-config.bat) reads from a bootstrap.conf file with plaintext sensitive configuration values and encrypts each value using a random encryption key. It replaces the plain values with the protected value in the same file, or writes to a new bootstrap.conf file if specified. Additionally it can be used to encrypt the unencrypted sensitive properties (if any) in the flow.json.raw. For using this functionality `nifi.minifi.sensitive.props.key` and `nifi.minifi.sensitive.props.algorithm` has to be provided in bootstrap.conf. 

The supported encryption algorithm utilized is AES/GCM 256-bit.

### Usage
To show help:

```
./bin/encrypt-config.sh -h
```

The following are the available options:
* -b, --bootstrapConf <bootstrapConfPath> Path to file containing Bootstrap Configuration [bootstrap.conf]
* -B, --outputBootstrapConf <outputBootstrapConf> Path to output file for Bootstrap Configuration [bootstrap.conf] with root key configured
* -x, --encryptRawFlowJsonOnly Process Raw Flow Configuration [flow.json.raw] sensitive property values without modifying other configuration files
* -f, --rawFlowJson <flowConfigurationPath> Path to file containing Raw Flow Configuration [flow.json.raw] that will be updated unless the output argument is provided
* -g, --outputRawFlowJson <outputFlowConfigurationPath> ath to output file for Raw Flow Configuration [flow.json.raw] with property protection applied
* -h, --help          Show help message and exit.

### Example 1
As an example of how the tool works with the following existing values in the bootstrap.conf file:
```
nifi.sensitive.props.key=thisIsABadSensitiveKeyPassword
nifi.sensitive.props.algorithm=NIFI_PBKDF2_AES_GCM_256
nifi.sensitive.props.additional.keys=

nifi.security.keystore=/path/to/keystore.jks
nifi.security.keystoreType=JKS
nifi.security.keystorePasswd=thisIsABadKeystorePassword
nifi.security.keyPasswd=thisIsABadKeyPassword
nifi.security.truststore=
nifi.security.truststoreType=
nifi.security.truststorePasswd=
c2.security.truststore.location=
c2.security.truststore.password=thisIsABadTruststorePassword
c2.security.truststore.type=JKS
c2.security.keystore.location=
c2.security.keystore.password=thisIsABadKeystorePassword
c2.security.keystore.type=JKS
```
Enter the following arguments when using the tool:
```
encrypt-config.sh \
-b bootstrap.conf \
```
As a result, the bootstrap.conf file is overwritten with protected properties and sibling encryption identifiers (aes/gcm/256, the currently supported algorithm):
```
nifi.sensitive.props.key=4OjkrFywZb7BlGz4||Tm9pg0jV4TltvVKeiMlm9zBsqmtmYUA2QkzcLKQpspyggtQuhNAkAla5s2695A==
nifi.sensitive.props.key.protected=aes/gcm/256
nifi.sensitive.props.algorithm=NIFI_PBKDF2_AES_GCM_256
nifi.sensitive.props.additional.keys=

nifi.security.keystore=/path/to/keystore.jks
nifi.security.keystoreType=JKS
nifi.security.keystorePasswd=iXDmDCadoNJ3VotZ||WvOGbrii4Gk0vr3b6mDstZg+NE0BPZUPk6LVqQlf2Sx3G5XFbUbUYAUz
nifi.security.keystorePasswd.protected=aes/gcm/256
nifi.security.keyPasswd=199uUUgpPqB4Fuoo||KckbW7iu+HZf1r4KSMQAFn8NLJK+CnUuayqPsTsdM0Wxou1BHg==
nifi.security.keyPasswd.protected=aes/gcm/256
nifi.security.truststore=
nifi.security.truststoreType=
nifi.security.truststorePasswd=
c2.security.truststore.location=
c2.security.truststore.password=0pHpp+l/WHsDM/sm||fXBvDAQ1BXvNQ8b4EHKa1GspsLx+UD+2EDhph0HbsdmgpVhEv4qj0q5TDo0=
c2.security.truststore.password.protected=aes/gcm/256
c2.security.truststore.type=JKS
c2.security.keystore.location=
c2.security.keystore.password=j+80L7++RNDf9INQ||RX/QkdVFwRos6Y4XJ8YSUWoI3W5Wx50dyw7HrAA84719SvfxA9eUSDEA
c2.security.keystore.password.protected=aes/gcm/256
c2.security.keystore.type=JKS
```

Additionally, the bootstrap.conf file is updated with the encryption key as follows:
```
minifi.bootstrap.sensitive.key=c92623e798be949379d0d18f432a57f1b74732141be321cb4af9ed94aa0ae8ac
```

Sensitive configuration values are encrypted by the tool by default, however you can encrypt any additional properties, if desired. To encrypt additional properties, specify them as comma-separated values in the minifi.sensitive.props.additional.keys property.

If the bootstrap.conf file already has valid protected values, those property values are not modified by the tool.

### Example 2
An example to encrypt non encrypted sensitive properties in flow.json.raw
```
nifi.sensitive.props.key=sensitivePropsKey
nifi.sensitive.props.algorithm=NIFI_PBKDF2_AES_GCM_256
```
Enter the following arguments when using the tool:
```
encrypt-config.sh -x -f flow.json.raw
```
As a result, the flow.json.raw file is overwritten with encrypted sensitive properties
The algorithm uses the property descriptors in the flow.json.raw to determine if a property is sensitive or not. If that information is missing, no properties will be encrypted even if it is defined sensitive in the agent manifest.

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
