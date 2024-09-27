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

# PublishAMQP

## Summary

This processor publishes the contents of the incoming FlowFile to an AMQP-based messaging system. At the time of writing
this document the supported AMQP protocol version is v0.9.1.

The component is based
on [RabbitMQ Client API](https://www.rabbitmq.com/api-guide.html) [The following guide and tutorial](https://www.rabbitmq.com/getstarted.html)
may also help you to brush up on some of the AMQP basics.

This processor does two things. It constructs AMQP Message by extracting FlowFile contents (both body and attributes).
Once message is constructed it is sent to an AMQP Exchange. AMQP Properties will be extracted from the FlowFile and
converted to _com.rabbitmq.client.AMQP.BasicProperties_ to be sent along with the message. Upon success the incoming
FlowFile is transferred to _success_ Relationship and upon failure FlowFile is penalized and transferred to the
_failure_ Relationship.

## Where did my message go?

In a typical AMQP exchange model, the message that is sent to an AMQP Exchange will be routed based on the _Routing Key_
to its final destination in the _Queue_. It's called **Binding**. If due to some misconfiguration the binding between
the _Exchange, Routing Key and the Queue_ is not set up, the message will have no final destination and will return (
i.e., the data will not make it to the queue). If that happens you will see a log in both app-log and bulletin stating
to that effect. Fixing the binding (normally done by AMQP administrator) will resolve the issue.

## AMQP Properties

Attributes extracted from the FlowFile are considered candidates for AMQP properties if their names are prefixed with 
_amqp\$_ (e.g., amqp\$contentType=text/xml). To enrich message with additional AMQP properties you may use *
*UpdateAttribute** processor between the source processor and PublishAMQP processor. The following is the list of
available standard AMQP properties: _("amqp\$contentType", "amqp\$contentEncoding", "
amqp\$headers" (if 'Headers Source' is set to 'Attribute "amqp\$headers" Value') , "amqp\$deliveryMode", "amqp\$priority", 
"amqp\$correlationId", "amqp\$replyTo", "amqp\$expiration", "amqp\$messageId", "amqp\$timestamp", "amqp\$type", 
"amqp\$userId", "amqp\$appId", "amqp\$clusterId")_

### AMQP Message Headers Source

The headers attached to AMQP message by the processor depends on the "Headers Source" property value.

1. **Attribute "amqp\$headers" Value** - The processor will read single attribute "amqp\$headers" and split it based on "
   Header Separator" and then read headers in _key=value_ format.
2. **Attributes Matching Regex** - The processor will pick flow file attributes by matching the regex provided in "
   Attributes To Headers Regular Expression". The name of the attribute is used as key of header

## Configuration Details

At the time of writing this document it only defines the essential configuration properties which are suitable for most
cases. Other properties will be defined later as this component progresses. Configuring PublishAMQP:

1. **Exchange Name** - \[OPTIONAL\] the name of AMQP exchange the messages will be sent to. Usually provided by the
   administrator (e.g., 'amq.direct') It is an optional property. If kept empty the messages will be sent to a default
   AMQP exchange. More on AMQP Exchanges could be found [here](https://www.rabbitmq.com/tutorials/amqp-concepts.html).
2. **Routing Key** - \[REQUIRED\] the name of the routing key that will be used by AMQP to route messages from the
   exchange to destination queue(s). Usually provided by administrator (e.g., 'myKey') In the event when messages are
   sent to a default exchange this property corresponds to a destination queue name, otherwise a binding from the
   Exchange to a Queue via Routing Key must be set (usually by the AMQP administrator). More on AMQP Exchanges and
   Bindings could be found [here](https://www.rabbitmq.com/tutorials/amqp-concepts.html).
3. **Host Name** - \[REQUIRED\] the name of the host where AMQP broker is running. Usually provided by administrator (
   e.g., 'myhost.com'). Defaults to 'localhost'.
4. **Port** - \[REQUIRED\] the port number where AMQP broker is running. Usually provided by the administrator (e.g., '
   2453'). Defaults to '5672'.
5. **User Name** - \[REQUIRED\] user name to connect to AMQP broker. Usually provided by the administrator (e.g., 'me').
   Defaults to 'guest'.
6. **Password** - \[REQUIRED\] password to use with user name to connect to AMQP broker. Usually provided by the
   administrator. Defaults to 'guest'.
7. **Use Certificate Authentication** - \[OPTIONAL\] Use the SSL certificate common name for authentication rather than
   user name/password. This can only be used in conjunction with SSL. Defaults to 'false'.
8. **Virtual Host** - \[OPTIONAL\] Virtual Host name which segregates AMQP system for enhanced security. Please refer
   to [this blog](http://blog.dtzq.com/2012/06/rabbitmq-users-and-virtual-hosts.html) for more details on Virtual Host.