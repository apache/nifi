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

# ConsumeAMQP

## Summary

This processor consumes messages from AMQP messaging queue and converts them to a FlowFile to be routed to the next
component in the flow. At the time of writing this document the supported AMQP protocol version is v0.9.1.

The component is based
on [RabbitMQ Client API](https://www.rabbitmq.com/api-guide.html) [The following guide and tutorial](https://www.rabbitmq.com/getstarted.html)
may also help you to brush up on some of the AMQP basics.

This processor does two things. It constructs FlwFile by extracting information from the consumed AMQP message (both
body and attributes). Once message is consumed a FlowFile is constructed. The message body is written to a FlowFile and
its _com.rabbitmq.client.AMQP.BasicProperties_ are transfered into the FlowFile as attributes. AMQP attribute names are
prefixed with _amqp$_ prefix.

## AMQP Properties

The following is the list of available standard AMQP properties which may come with the message: _("amqp\$contentType", 
"amqp\$contentEncoding", "amqp\$headers", "amqp\$deliveryMode", "amqp\$priority", "amqp\$correlationId", 
"amqp\$replyTo", "amqp\$expiration", "amqp\$messageId", "amqp\$timestamp", "amqp\$type", "amqp\$userId", 
"amqp\$appId", "amqp\$clusterId", "amqp\$routingKey")_

## Configuration Details

At the time of writing this document it only defines the essential configuration properties which are suitable for most
cases. Other properties will be defined later as this component progresses. Configuring PublishAMQP:

1. **Queue** - \[REQUIRED\] the name of AMQP queue the messages will be retrieved from. Usually provided by
   administrator (e.g., 'amq.direct')
2. **Host Name** - \[REQUIRED\] the name of the host where AMQP broker is running. Usually provided by administrator (
   e.g., 'myhost.com'). Defaults to 'localhost'.
3. **Port** - \[REQUIRED\] the port number where AMQP broker is running. Usually provided by the administrator (e.g., '
   2453'). Defaults to '5672'.
4. **User Name** - \[REQUIRED\] user name to connect to AMQP broker. Usually provided by the administrator (e.g., 'me').
   Defaults to 'guest'.
5. **Password** - \[REQUIRED\] password to use with user name to connect to AMQP broker. Usually provided by the
   administrator. Defaults to 'guest'.
6. **Use Certificate Authentication** - \[OPTIONAL\] Use the SSL certificate common name for authentication rather than
   user name/password. This can only be used in conjunction with SSL. Defaults to 'false'.
7. **Virtual Host** - \[OPTIONAL\] Virtual Host name which segregates AMQP system for enhanced security. Please refer
   to [this blog](http://blog.dtzq.com/2012/06/rabbitmq-users-and-virtual-hosts.html) for more details on Virtual Host.