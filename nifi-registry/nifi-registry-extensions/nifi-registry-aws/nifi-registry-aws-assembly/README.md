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
# NiFi Registry AWS extensions

This modules provides AWS related extensions for NiFi Registry.

## Prerequisites

* AWS account credentials and an S3 bucket.

## How to install

### Enable AWS extensions at NiFi Registry build

The AWS extensions will be automatically included when you build NiFi Registry and will be installed at the `${NIFI_REG_HOME}/ext/aws` directory.

If you wish to build NiFi Registry without including the AWS extensions, specify the `skipAws` system property:
```
cd nifi-registry
mvn clean install -DskipAws
```

### Add AWS extensions to existing NiFi Registry

To add AWS extensions to an existing NiFi Registry, build the extension with the following command:

```
cd nifi-registry
mvn clean install -f nifi-registry-extensions/nifi-registry-aws
```

The extension zip will be created as `nifi-registry-extensions/nifi-registry-aws/nifi-registry-aws-assembly/target/nifi-registry-aws-assembly-xxx-bin.zip`.

Unzip the file into arbitrary directory so that NiFi Registry can use, such as `${NIFI_REG_HOME}/ext/aws`.
For example:

```
mkdir -p ${NIFI_REG_HOME}/ext/aws
unzip -d ${NIFI_REG_HOME}/ext/aws nifi-registry-extensions/nifi-registry-aws/nifi-registry-aws-assembly/target/nifi-registry-aws-assembly-xxx-bin.zip
```

## NiFi Registry Configuration

In order to use this extension, the following NiFi Registry files need to be configured.

### nifi-registry.properties

The extension dir property will be automatically configured when building with the `include-aws` profile (i.e. when not specifying -DskipAws).

To manually specify the property when adding the AWS extensions to an existing NiFi registry, configure the following property:
```
# Specify AWS extension dir
nifi.registry.extension.dir.aws=./ext/aws/lib
```

### providers.xml

Uncomment the extensionBundlePersistenceProvider for S3:
```
<!--
<extensionBundlePersistenceProvider>
    <class>org.apache.nifi.registry.aws.S3BundlePersistenceProvider</class>
    <property name="Region">us-east-1</property>
    <property name="Bucket Name">my-bundles</property>
    <property name="Key Prefix"></property>
    <property name="Credentials Provider">DEFAULT_CHAIN</property>
    <property name="Access Key"></property>
    <property name="Secret Access Key"></property>
    <property name="Endpoint URL"></property>
</extensionBundlePersistenceProvider>
-->
```

NOTE: Remember to remove, or comment out, the FileSystemBundlePersistenceProvider since there can only be one defined.

