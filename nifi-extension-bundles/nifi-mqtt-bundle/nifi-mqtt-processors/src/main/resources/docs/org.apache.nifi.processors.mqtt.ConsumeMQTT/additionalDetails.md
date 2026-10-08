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

# ConsumeMQTT

The MQTT messages are always being sent to subscribers on a topic regardless of how frequently the processor is
scheduled to run. If the 'Run Schedule' is significantly behind the rate at which the messages are arriving to this
processor, then a back-up can occur in the internal queue of this processor. Each time the processor is scheduled, the
messages in the internal queue will be written to FlowFiles. In case the internal queue is full, the MQTT client will
try for up to 1 second to add the message into the internal queue. If the internal queue is still full after this time,
an exception saying that 'The subscriber queue is full' would be thrown, the message would be dropped and the client
would be disconnected. In case the QoS property is set to 0, the message would be lost. In case the QoS property is set
to 1 or 2, the message will be received after the client reconnects.

## Multiple Topic Filters

The 'Topic Filter' property accepts a comma-separated list of topic filters. When more than one filter is provided, all
of them are subscribed to with a single SUBSCRIBE request over the same broker connection, instead of requiring one
instance of this processor (and one broker connection) per topic. This is especially useful when the topics to consume
are flat or externally dictated, or when broker ACLs only authorize specific, explicit topics, making wildcard filters
unusable.

Each topic filter in the list is subscribed to at the same Quality of Service, configured by the 'Quality of Service'
property. If 'Group ID' is set, the shared subscription prefix (`$share/<Group ID>/`) is applied to every topic filter
individually, so each remains its own subscription eligible for load-balancing across the group.

A value that contains no comma is a single topic filter and is used exactly as configured, so existing configurations
are unaffected. Within a comma-separated list, whitespace around each topic filter is trimmed, empty entries are
ignored, and duplicate topic filters are rejected during validation. Because MQTT topic filters may legally contain a
comma, a topic filter that itself contains a comma will be misinterpreted as multiple filters; this is expected to be a
rare edge case.

If the broker rejects a subscription for one or more of the requested topic filters (for example due to an ACL denial),
the processor fails to initialize its client and yields, logging the offending topic filter(s), rather than silently
continuing without them.
