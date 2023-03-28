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

import org.apache.nifi.kafka.service.api.common.OffsetSummary;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class PollingSummary extends PollingContext {
    private final Map<TopicPartitionSummary, OffsetSummary> offsets;

    public PollingSummary(
            final String groupId,
            final Collection<String> topics,
            final AutoOffsetReset autoOffsetReset,
            final Duration maxUncommittedTime,
            final Map<TopicPartitionSummary, OffsetSummary> offsets
    ) {
        super(groupId, topics, autoOffsetReset, maxUncommittedTime);
        this.offsets = Objects.requireNonNull(offsets, "Offsets required");
    }

    public PollingSummary(
            final String groupId,
            final Pattern topicPattern,
            final AutoOffsetReset autoOffsetReset,
            final Duration maxUncommittedTime,
            final Map<TopicPartitionSummary, OffsetSummary> offsets
    ) {
        super(groupId, topicPattern, autoOffsetReset, maxUncommittedTime);
        this.offsets = Objects.requireNonNull(offsets, "Offsets required");
    }

    public Map<TopicPartitionSummary, OffsetSummary> getOffsets() {
        return offsets;
    }
}
