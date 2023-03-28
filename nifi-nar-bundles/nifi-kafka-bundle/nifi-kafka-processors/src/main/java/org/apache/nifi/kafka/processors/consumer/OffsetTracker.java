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
package org.apache.nifi.kafka.processors.consumer;

import org.apache.nifi.kafka.service.api.common.OffsetSummary;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.consumer.PollingSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.util.LinkedHashMap;
import java.util.Map;

public class OffsetTracker {
    private final Map<TopicPartitionSummary, OffsetSummary> offsets;

    public OffsetTracker() {
        offsets = new LinkedHashMap<>();
    }

    public void update(final ByteRecord consumerRecord) {
        final TopicPartitionSummary topicPartitionSummary = new TopicPartitionSummary(consumerRecord.getTopic(), consumerRecord.getPartition());
        final long offset = consumerRecord.getOffset();
        final OffsetSummary offsetSummary = offsets.computeIfAbsent(topicPartitionSummary, (summary) -> new OffsetSummary(offset));
        offsetSummary.setOffset(offset);
    }

    public PollingSummary getPollingSummary(final PollingContext pollingContext) {
        final PollingSummary pollingSummary;
        if (pollingContext.getTopicPattern().isPresent()) {
            pollingSummary = new PollingSummary(pollingContext.getGroupId(), pollingContext.getTopicPattern().get(),
                    pollingContext.getAutoOffsetReset(), pollingContext.getMaxUncommittedTime(), offsets);
        } else {
            pollingSummary = new PollingSummary(pollingContext.getGroupId(), pollingContext.getTopics(),
                    pollingContext.getAutoOffsetReset(), pollingContext.getMaxUncommittedTime(), offsets);
        }
        return pollingSummary;
    }
}
