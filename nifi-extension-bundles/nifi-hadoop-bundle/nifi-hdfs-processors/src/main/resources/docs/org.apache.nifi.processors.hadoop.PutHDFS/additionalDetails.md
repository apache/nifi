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

# PutHDFS

## SSL Configuration:

Hadoop provides the ability to configure keystore and/or truststore properties. If you want to use SSL-secured file
system like swebhdfs, you can use the Hadoop configurations instead of using SSL Context Service.

1. create 'ssl-client.xml' to configure the truststores.

ssl-client.xml Properties:

| Property                              | Default Value | Explanation                                 |
|---------------------------------------|---------------|---------------------------------------------|
| ssl.client.truststore.type            | jks           | Truststore file type                        |
| ssl.client.truststore.location        | NONE          | Truststore file location                    |
| ssl.client.truststore.password        | NONE          | Truststore file password                    |
| ssl.client.truststore.reload.interval | 10000         | Truststore reload interval, in milliseconds |

ssl-client.xml Example:

```xml
<configuration>
    <property>
        <name>ssl.client.truststore.type</name>
        <value>jks</value>
    </property>
    <property>
        <name>ssl.client.truststore.location</name>
        <value>/path/to/truststore.jks</value>
    </property>
    <property>
        <name>ssl.client.truststore.password</name>
        <value>clientfoo</value>
    </property>
    <property>
        <name>ssl.client.truststore.reload.interval</name>
        <value>10000</value>
    </property>
</configuration>
```

6. put 'ssl-client.xml' to the location looked up in the classpath, like under NiFi conriguration directory.
7. set the name of 'ssl-client.xml' to _hadoop.ssl.client.conf_ in the 'core-site.xml' which HDFS processors use.

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>swebhdfs://{namenode.hostname:port}</value>
    </property>
    <property>
        <name>hadoop.ssl.client.conf</name>
        <value>ssl-client.xml</value>
    </property>
</configuration>
```
