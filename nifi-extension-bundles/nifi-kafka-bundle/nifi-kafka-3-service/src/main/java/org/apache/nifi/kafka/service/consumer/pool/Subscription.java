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
package org.apache.nifi.kafka.service.consumer.pool;

import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Subscription for pooled Kafka Consumers
 */
public class Subscription {

    private final String groupId;
    private final Collection<String> topics;
    private final Pattern topicPattern;
    private final AutoOffsetReset autoOffsetReset;

    public Subscription(final String groupId, final Collection<String> topics, final AutoOffsetReset autoOffsetReset) {
        this.groupId = Objects.requireNonNull(groupId, "Group ID required");
        this.topics = Collections.unmodifiableCollection(Objects.requireNonNull(topics, "Topics required"));
        this.topicPattern = null;
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset, "Auto Offset Reset required");
    }

    public Subscription(final String groupId, final Pattern topicPattern, final AutoOffsetReset autoOffsetReset) {
        this.groupId = Objects.requireNonNull(groupId, "Group ID required");
        this.topics = Collections.emptyList();
        this.topicPattern = Objects.requireNonNull(topicPattern, "Topic Pattern required");
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset, "Auto Offset Reset required");
    }

    public String getGroupId() {
        return groupId;
    }

    public Collection<String> getTopics() {
        return topics;
    }

    public Optional<Pattern> getTopicPattern() {
        return Optional.ofNullable(topicPattern);
    }

    public AutoOffsetReset getAutoOffsetReset() {
        return autoOffsetReset;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }

        if (object == this) {
            return true;
        }

        if (object instanceof final Subscription subscription) {
            return groupId.equals(subscription.groupId)
                   && isTopicSubscriptionMatched(subscription)
                   && autoOffsetReset == subscription.autoOffsetReset;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return groupId.hashCode();
    }

    @Override
    public String toString() {
        return String.format("Subscription Group ID [%s] Topics %s Topic Pattern [%s]", groupId, topics, topicPattern);
    }

    private boolean isTopicSubscriptionMatched(final Subscription subscription) {
        if (topics.size() == subscription.topics.size() && topics.containsAll(subscription.topics)) {
            final String regexLeft = (topicPattern == null ? null : topicPattern.pattern());
            final String regexRight = (subscription.topicPattern == null ? null : subscription.topicPattern.pattern());
            return Objects.equals(regexLeft, regexRight);
        }

        return false;
    }
}
