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

## Description

This controller service serves as a general factory service to serving vendor specific instances of the
_javax.jms.ConnectionFactory_. It does so by allowing user to configure vendor specific properties as well as point to
the location of the vendor provided JMS client libraries so the correct implementation of the
_javax.jms.ConnectionFactory_ can be found, loaded, instantiated and served to the dependent processors (see PublishJMS,
ConsumeJMS).

All JMS vendors and _ConnectionFactory_ implementations are supported as long as the configuration values can be set
through _set_ methods (detailed explanation in the last paragraph). However, some helpful accommodation are done for the
following JMS vendors:

* Apache ActiveMQ
* IBM MQ
* TIBCO EMS
* Qpid JMS (AMQP 1.0)

This controller service exposes only a single mandatory static configuration property that are required across all
implementations. The rest of the configuration properties are either optional or vendor specific.

The mandatory configuration property is:

* **JMS Connection Factory Implementation** - The fully qualified name of the JMS _ConnectionFactory_ implementation
  class. For example:
    * Apache
      ActiveMQ - [org.apache.activemq.ActiveMQConnectionFactory](http://activemq.apache.org/maven/5.15.9/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html)
    * IBM
      MQ - [com.ibm.mq.jms.MQQueueConnectionFactory](https://www-01.ibm.com/support/knowledgecenter/SSFKSJ_8.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/jms/MQQueueConnectionFactory.html)
    * TIBCO
      EMS - [com.tibco.tibjms.TibjmsQueueConnectionFactory](https://docs.tibco.com/pub/enterprise_message_service/8.1.0/doc/html/tib_ems_api_reference/api/javadoc/com/tibco/tibjms/TibjmsQueueConnectionFactory.html)
    * Qpid JMS (AMQP
      1.0) - [org.apache.qpid.jms.JmsConnectionFactory](https://github.com/apache/qpid-jms/blob/1.1.0/qpid-jms-client/src/main/java/org/apache/qpid/jms/JmsConnectionFactory.java)

The following static configuration properties are optional but required in many cases:

* **JMS Client Libraries** - Path to the directory with additional resources (eg. JARs, configuration files, etc.) to be
  added to the classpath (defined as a comma separated list of values). Such resources typically represent target JMS
  client libraries for the _ConnectionFactory_ implementation.
* **JMS Broker URI** - URI pointing to the network location of the JMS Message broker. For example:
    * Apache ActiveMQ - _tcp://myhost:1234_ for single broker and _failover:(tcp://myhost01:1234,tcp://myhost02:1234)_
      for multiple brokers.
    * IBM MQ - _myhost(1234)_ for single broker. _myhost01(1234),myhost02(1234)_ for multiple brokers.
    * TIBCO EMS - _tcp://myhost:1234_ for single broker and _tcp://myhost01:7222,tcp://myhost02:7222_ for multiple
      brokers.
    * Qpid JMS (AMQP 1.0) - _amqp\[s\]://myhost:1234_ for single broker and _failover:(amqp\[s\]://myhost01:
      1234,amqp\[s\]://myhost02:1234)_ for multiple brokers.

The rest of the vendor specific configuration are set through dynamic properties utilizing
the [Java Beans](http://docstore.mik.ua/orelly/java-ent/jnut/ch06_02.htm) convention where a property name is derived
from the _set_ method of the vendor specific _ConnectionFactory_'s implementation. For example,
_com.ibm.mq.jms.MQConnectionFactory.setChannel(String)_ would imply 'channel' property and
_com.ibm.mq.jms.MQConnectionFactory.setTransportType(int)_ would imply 'transportType' property. For the list of
available properties please consult vendor provided documentation. Following is examples of such vendor provided
documentation:

* [Apache ActiveMQ](http://activemq.apache.org/maven/5.15.9/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html)
* [IBM MQ](https://www.ibm.com/support/knowledgecenter/SSFKSJ_8.0.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/jms/MQConnectionFactory.html)
* [TIBCO EMS](https://docs.tibco.com/pub/enterprise_message_service/8.1.0/doc/html/tib_ems_api_reference/api/javadoc/com/tibco/tibjms/TibjmsConnectionFactory.html)
* [Qpid JMS (AMQP 1.0)](https://docs.tibco.com/pub/enterprise_message_service/8.1.0/doc/html/tib_ems_api_reference/api/javadoc/com/tibco/tibjms/TibjmsConnectionFactory.html)

Besides the dynamic properties and _set_ methods described in the previous section, some providers also support
additional configuration via the Broker URI (as query parameters added to the URI):

* [Qpid JMS (AMQP 1.0)](https://qpid.apache.org/releases/qpid-jms-1.1.0/docs/index.html#connection-uri)

## Sample controller service configuration for IBM MQ

| Property                              | Value                                   | Static/Dynamic | Comments                                                                                                                                                                                                                                             |
|---------------------------------------|-----------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JMS Connection Factory Implementation | com.ibm.mq.jms.MQQueueConnectionFactory | Static         | Vendor provided implementation of QueueConnectionFactory                                                                                                                                                                                             |
| JMS Client Libraries                  | /opt/mqm/java/lib                       | Static         | Default installation path of client JAR files on Linux systems                                                                                                                                                                                       |
| JMS Broker URI                        | mqhost01(1414),mqhost02(1414)           | Static         | [Connection Name List syntax](https://www.ibm.com/support/knowledgecenter/ro/SSAW57_9.0.0/com.ibm.websphere.nd.multiplatform.doc/ae/ucli_pqcfm.html#MQTopicConnectionFactory_enterporthostname). Colon separated host/port pair(s) is also supported |
| JMS SSL Context Service               |                                         | Static         | Only required if using SSL/TLS                                                                                                                                                                                                                       |
| channel                               | TO.BAR                                  | Dynamic        | Required when using the client transport mode                                                                                                                                                                                                        |
| queueManager                          | PQM1                                    | Dynamic        | Name of queue manager. Always required.                                                                                                                                                                                                              |
| transportType                         | 1                                       | Dynamic        | Constant integer value corresponding to the client transport mode. Default value is ["Bindings, then client"](https://www.ibm.com/support/knowledgecenter/en/SSEQTP_9.0.0/com.ibm.websphere.base.doc/ae/umj_pjcfm.html)                              |