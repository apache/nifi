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

# ConsumeJMS

## Summary

This processor consumes messages from JMS compliant messaging system and converts them to a FlowFile to be routed to the
next component in the flow.

This processor does two things. It constructs FlowFile by extracting information from the consumed JMS message including
body, standard JMS Headers and Properties. The message body is written to a FlowFile while standard JMS Headers and
Properties are set as FlowFile attributes.

## Configuration Details

At the time of writing this document it only defines the essential configuration properties which are suitable for most
cases. Other properties will be defined later as this component progresses. Configuring ConsumeJMS:

1. **User Name** - \[OPTIONAL\] User Name used for authentication and authorization when this processor obtains
   _javax.jms.Connection_ from the pre-configured _javax.jms.ConnectionFactory_ (see below).
2. **Password** - \[OPTIONAL\] Password used in conjunction with **User Name**.
3. **Destination Name** - \[REQUIRED\] the name of the _javax.jms.Destination_. Usually provided by administrator (
   e.g., 'topic://myTopic').
4. **Destination Type** - \[REQUIRED\] the type of the _javax.jms.Destination_. Could be one of 'QUEUE' or 'TOPIC'
   Usually provided by the administrator. Defaults to 'QUEUE'.

### Connection Factory Configuration

There are multiple ways to configure the Connection Factory for the processor:

* **Connection Factory Service** property - link to a pre-configured controller service (
  _JndiJmsConnectionFactoryProvider_ or _JMSConnectionFactoryProvider_)
* **JNDI &ast;** properties - processor level configuration, the properties are the same as the properties of
  _JndiJmsConnectionFactoryProvider_ controller service, the dynamic properties can also be used in this case
* **JMS &ast;** properties - processor level configuration, the properties are the same as the properties of
  _JMSConnectionFactoryProvider_ controller service, the dynamic properties can also be used in this case

The preferred way is to use the Connection Factory Service property and a pre-configured controller service. It is also
the most convenient method, because it is enough to configure the controller service once, and then it can be used in
multiple processors.

However, some JMS client libraries may not work with the controller services due to incompatible Java ClassLoader
handling between the 3rd party JMS client library and NiFi. Should you encounter _java.lang.ClassCastException_ errors
when using the controller services, please try to configure the Connection Factory via the 'JNDI \*' or the 'JMS \*' and
the dynamic properties of the processor. For more details on these properties, see the documentation of the
corresponding controller service (_JndiJmsConnectionFactoryProvider_ for 'JNDI \*' and _JMSConnectionFactoryProvider_
for 'JMS \*').