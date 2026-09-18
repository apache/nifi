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
package org.apache.nifi.kafka.service.api.consumer.share;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Subscription context for a Kafka share-group consumer.
 * Share groups (KIP-932) accept only a topic-name list; pattern subscription is not part of the
 * 4.1 share-consumer API.
 */
public class ShareGroupContext {

    private final String groupId;
    private final Collection<String> topics;
    private final ShareAcknowledgementMode acknowledgementMode;

    public ShareGroupContext(final String groupId, final Collection<String> topics, final ShareAcknowledgementMode acknowledgementMode) {
        this.groupId = Objects.requireNonNull(groupId, "Group ID required");
        this.topics = List.copyOf(Objects.requireNonNull(topics, "Topics required"));
        this.acknowledgementMode = Objects.requireNonNull(acknowledgementMode, "Acknowledgement Mode required");
    }

    public String getGroupId() {
        return groupId;
    }

    public Collection<String> getTopics() {
        return topics;
    }

    public ShareAcknowledgementMode getAcknowledgementMode() {
        return acknowledgementMode;
    }

    @Override
    public String toString() {
        return "ShareGroupContext[groupId=%s, topics=%s, acknowledgementMode=%s]".formatted(groupId, topics, acknowledgementMode);
    }
}
