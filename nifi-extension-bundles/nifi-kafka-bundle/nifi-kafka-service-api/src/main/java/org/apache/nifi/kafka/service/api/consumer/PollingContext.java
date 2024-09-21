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
package org.apache.nifi.kafka.service.api.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Polling Context for consuming records from Kafka Topics
 */
public class PollingContext {

    private final String groupId;
    private final Collection<String> topics;
    private final Pattern topicPattern;
    private final AutoOffsetReset autoOffsetReset;
    private final Duration maxUncommittedTime;

    public PollingContext(final String groupId, final Collection<String> topics,
                          final AutoOffsetReset autoOffsetReset, final Duration maxUncommittedTime) {
        this.groupId = Objects.requireNonNull(groupId, "Group ID required");
        this.topics = Collections.unmodifiableCollection(Objects.requireNonNull(topics, "Topics required"));
        this.topicPattern = null;
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset, "Auto Offset Reset required");
        this.maxUncommittedTime = maxUncommittedTime;
    }

    public PollingContext(final String groupId, final Pattern topicPattern,
                          final AutoOffsetReset autoOffsetReset, final Duration maxUncommittedTime) {
        this.groupId = Objects.requireNonNull(groupId, "Group ID required");
        this.topics = Collections.emptyList();
        this.topicPattern = Objects.requireNonNull(topicPattern, "Topic Patten required");
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset, "Auto Offset Reset required");
        this.maxUncommittedTime = maxUncommittedTime;
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

    public Duration getMaxUncommittedTime() {
        return maxUncommittedTime;
    }

    @Override
    public String toString() {
        return String.format("Group ID [%s] Topics %s Topic Pattern [%s] Auto Offset Reset [%s] Max Uncommitted Time [%s]",
                groupId, topics, topicPattern, autoOffsetReset, maxUncommittedTime);
    }
}
