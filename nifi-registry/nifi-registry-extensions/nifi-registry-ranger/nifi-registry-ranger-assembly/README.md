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
# NiFi Registry Ranger extension

This extension provides `org.apache.nifi.registry.ranger.RangerAuthorizer` class for NiFi Registry to authorize user requests by access policies defined at [Apache Ranger](https://ranger.apache.org/).

## Prerequisites

* Apache Ranger 1.2.0 or later is needed.

## How to install

### Enable Ranger extension at NiFi Registry build

In order to enable Ranger extension when you build NiFi Registry, specify `include-ranger` profile with a maven install command:

```
cd nifi-registry
mvn clean install -Pinclude-ranger
```

Then the extension will be installed at `${NIFI_REG_HOME}/ext/ranger` directory.

### Add Ranger extension to existing NiFi Registry

Alternatively, you can add Ranger extension to an existing NiFi Registry.
To do so, build the extension with the following command:

```
cd nifi-registry
mvn clean install -f nifi-registry-extensions/nifi-registry-ranger
```

The extension zip will be created as `nifi-registry-extensions/nifi-registry-ranger-extension/target/nifi-registry-ranger-extension-xxx-bin.zip`.

Unzip the file into arbitrary directory so that NiFi Registry can use, such as `${NIFI_REG_HOME}/ext/ranger`.
For example:

```
mkdir -p ${NIFI_REG_HOME}/ext/ranger
unzip -d ${NIFI_REG_HOME}/ext/ranger nifi-registry-extensions/nifi-registry-ranger-extension/target/nifi-registry-ranger-extension-xxx-bin.zip
```

## NiFi Registry Configuration

In order to use this extension, following NiFi Registry files need to be configured.

### nifi-registry.properties

```
# Specify Ranger extension dir
nifi.registry.extension.dir.ranger=./ext/ranger/lib
# Specify Ranger authorizer identifier, which is defined at authorizers.xml
nifi.registry.security.authorizer=ranger-authorizer
```

### authorizers.xml

Add following `authorizer` element:
```
    <authorizer>
        <identifier>ranger-authorizer</identifier>
        <class>org.apache.nifi.registry.ranger.RangerAuthorizer</class>
        <property name="Ranger Service Type">nifi-registry</property>

        <property name="User Group Provider">file-user-group-provider</property>

        <!-- Specify Ranger service name to use -->
        <property name="Ranger Application Id">nifi-registry-service-name</property>

        <!--
            Specify configuration file paths for Ranger plugin.
            See the XML files bundled with this extension for further details.
         -->
        <property name="Ranger Security Config Path">./ext/ranger/conf/ranger-nifi-registry-security.xml</property>
        <property name="Ranger Audit Config Path">./ext/ranger/conf/ranger-nifi-registry-audit.xml</property>

        <!--
            Specify user identity that is used by Ranger to access NiFi Registry.
            This property is used by NiFi Registry for Ranger to get available NiFi Registry policy resource identifiers.
            The configured user can access NiFi Registry /policies/resources REST endpoint regardless of configured access policies.
            Ranger uses available policies for user input suggestion at Ranger policy editor UI.
        -->
        <property name="Ranger Admin Identity">ranger@NIFI</property>

        <!--
            Specify if target Ranger is Kerberized.
            If set to true, NiFi Registry will use the principal and keytab defined at nifi-registry.properties:
            - nifi.registry.kerberos.service.principal
            - nifi.registry.kerberos.service.keytab.location

            The specified credential is used to access Ranger API, and to write audit logs into HDFS (if enabled).

            At Ranger side, the configured user needs to be added to 'policy.download.auth.users' property, see Ranger configuration section below.

            Also, ranger-nifi-registry-security.xml needs additional "xasecure.add-hadoop-authorization = true" configuration.
        -->
        <property name="Ranger Kerberos Enabled">false</property>

    </authorizer>
```

## Ranger Configuration

At Ranger side, add a NiFi Registry service. NiFi Registry service has following configuration properties:

- NiFi Registry URL: Specify corresponding NiFi Registry URL that will be managed by this Ranger service. E.g. `https://nifi-registry.example.com:18443/nifi-registry-api/policies/resources`
- Authentication Type: Should be `SSL`. Ranger authenticates itself to NiFi Registry by X.509 client certificate in the configured Keystore.
- Keystore: Specify a Keystore filepath to use for X.509 client certificate.
- Keystore Type: Specify the type of Keystore. E.g. `JKS`
- Keystore Password: Specify the password of Keystore.
- Truststore: Specify a Truststore filepath to verify NiFi Registry server certificate.
- Truststore Type: Specify the type of Truststore. E.g. `JKS`
- Truststore Password: Specify the password of Truststore.
- Add New Configurations:
  - policy.download.auth.users: Required if Ranger is Kerberized.
    Specify the NiFi Registry user to download policies,
    which is configured by 'nifi.registry.kerberos.service.principal' at nifi-registry.properties,
    when NiFi Registry Ranger authorizer is configured as 'Ranger Kerberos Enabled' to true.
