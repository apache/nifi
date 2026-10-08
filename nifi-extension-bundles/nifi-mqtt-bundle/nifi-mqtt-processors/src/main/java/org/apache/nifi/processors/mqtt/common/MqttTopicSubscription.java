/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.mqtt.common;

/**
 * Represents a single (Topic Filter, QoS) pair of a SUBSCRIBE request. A SUBSCRIBE packet can carry a list of these,
 * allowing a client to subscribe to multiple Topic Filters with a single request.
 *
 * @param topicFilter the topic filter to subscribe to, which can include wildcards.
 * @param qos the maximum quality of service at which to subscribe. Messages published at a lower quality of
 *            service will be received at the published QoS. Messages published at a higher quality of service
 *            will be received using the QoS specified on the subscribe.
 */
public record MqttTopicSubscription(String topicFilter, int qos) {
}
