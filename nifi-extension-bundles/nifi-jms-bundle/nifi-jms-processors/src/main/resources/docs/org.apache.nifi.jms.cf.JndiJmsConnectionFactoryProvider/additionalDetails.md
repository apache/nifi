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

# JMSConnectionFactoryProvider

## Capabilities

This Controller Service allows users to reference a JMS Connection Factory that has already been established and made
available via Java Naming and Directory Interface (JNDI) Server. Please see documentation from your JMS Vendor in order
to understand the appropriate values to configure for this service.

A Connection Factory in Java is typically obtained via JNDI in code like below. The comments have been added in to
explain how this maps to the Controller Service's configuration.

```java
Hashtable env = new Hashtable();
env.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_INITIAL_CONTEXT_FACTORY); // Value for this comes from the "JNDI Initial Context Factory Class" property.
env.put(Context.PROVIDER_URL, JNDI_PROVIDER_URL); // Value for this comes from the "JNDI Provider URL" property.
env.put("My-Environment-Variable","Environment-Variable-Value"); // This is accomplished by added a user-defined property with name "My-Environment-Variable" and value "Environment-Variable-Value"

Context initialContext = new InitialContext(env);
ConnectionFactory connectionFactory = initialContext.lookup(JNDI_CONNECTION_FACTORY_NAME); // Value for this comes from the "JNDI Name of the Connection Factory" property
```

It is also important to note that, in order for this to work, the class named by the "JNDI Initial Context Factory
Class" must be available on the classpath. The JMS provider specific client classes (like the class of the Connection
Factory object to be retrieved from JNDI) must also be available on the classpath. In NiFi, this is accomplished by
setting the "JNDI / JMS Client Libraries" property to point to one or more .jar files or directories (comma-separated
values).

When the Controller Service is disabled and then re-enabled, it will perform the JNDI lookup again. Once the Connection
Factory has been obtained, though, it will not perform another JNDI lookup until the service is disabled.

## Example Configuration

As an example, the following configuration may be used to connect to Active MQ's JMS Broker, using the Connection
Factory provided via their embedded JNDI server:

| Property Name                       | Property Value                                         |
|-------------------------------------|--------------------------------------------------------|
| JNDI Initial Context Factory Class  | org.apache.activemq.jndi.ActiveMQInitialContextFactory |
| JNDI Provider URL                   | tcp://jms-broker:61616                                 |
| JNDI Name of the Connection Factory | ConnectionFactory                                      |
| JNDI / JMS Client Libraries         | /opt/apache-activemq-5.15.2/lib/                       |

The above example assumes that there exists a host that is accessible with hostname "jms-broker" and that is running
Apache ActiveMQ on port 61616 and also that the jar(s) containing the
org.apache.activemq.jndi.ActiveMQInitialContextFactory class and the other JMS client classes can be found within the
/opt/apache-activemq-5.15.2/lib/ directory.

## Property Validation

The following component properties include additional validation to restrict allowed values:

* JNDI Provider URL

### JNDI Provider URL Validation

The default validation for `JNDI Provider URL` allows the following URL schemes:

* file
* jgroups
* ssl
* t3
* t3s
* tcp
* udp
* vm

The following Java System property can be configured to override the default allowed URL schemes:

* `org.apache.nifi.jms.cf.jndi.provider.url.schemes.allowed`

The System property must contain a space-separated list of URL schemes. This property can be configured in the
application `bootstrap.conf` as follows:

* `java.arg.jndiJmsUrlSchemesAllowed=-Dorg.apache.nifi.jms.cf.jndi.provider.url.schemes.allowed=ssl tcp`