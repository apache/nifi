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

package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.FlowFileFilters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_CONSUMER_GROUP_ID;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_LEADER_EPOCH;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_MAX_OFFSET;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_OFFSET;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_PARTITION;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_TOPIC;

public class PublishKafkaUtil {

    /**
     * Polls for a batch of FlowFiles that should be published to Kafka within a single transaction
     * @param session the process session to poll from
     * @return the FlowFiles that should be sent as a single transaction
     */
    public static List<FlowFile> pollFlowFiles(final ProcessSession session) {
        final List<FlowFile> initialFlowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 500));
        if (initialFlowFiles.isEmpty()) {
            return initialFlowFiles;
        }

        // Check if any of the FlowFiles indicate that the consumer offsets have yet to be committed.
        boolean offsetsCommitted = true;
        for (final FlowFile flowFile : initialFlowFiles) {
            if ("false".equals(flowFile.getAttribute(KAFKA_CONSUMER_OFFSETS_COMMITTED))) {
                offsetsCommitted = false;
                break;
            }
        }

        if (offsetsCommitted) {
            return initialFlowFiles;
        }

        // If we need to commit consumer offsets, it is important that we retrieve all FlowFiles that may be available. Otherwise, we could
        // have a situation in which there are 2 FlowFiles for Topic MyTopic and Partition 1. The first FlowFile may have an offset of 100,000
        // while the second has an offset of 98,000. If we gather only the first, we could commit 100,000 offset before processing offset 98,000.
        // To avoid that, we consume all FlowFiles in the queue. It's important also that all FlowFiles that have been consumed from Kafka are made
        // available in the queue. This can be done by using a ProcessGroup with Batch Output, as described in the additionalDetails of the Kafka Processors.
        return pollAllFlowFiles(session, initialFlowFiles);
    }

    private static List<FlowFile> pollAllFlowFiles(final ProcessSession session, final List<FlowFile> initialFlowFiles) {
        final List<FlowFile> polled = new ArrayList<>(initialFlowFiles);
        while (true) {
            final List<FlowFile> flowFiles = session.get(10_000);
            if (flowFiles.isEmpty()) {
                break;
            }

            polled.addAll(flowFiles);
        }

        return polled;
    }

    /**
     * Adds the appropriate Kafka Consumer offsets to the active transaction of the given publisher lease
     * @param lease the lease that has an open transaction
     * @param flowFile the FlowFile whose offsets should be acknowledged
     * @param logger the processor's logger
     */
    public static void addConsumerOffsets(final PublisherLease lease, final FlowFile flowFile, final ComponentLog logger) {
        final String topic = flowFile.getAttribute(KAFKA_TOPIC);
        final Long partition = getNumericAttribute(flowFile, KAFKA_PARTITION, logger);
        Long maxOffset = getNumericAttribute(flowFile, KAFKA_MAX_OFFSET, logger);
        if (maxOffset == null) {
            maxOffset = getNumericAttribute(flowFile, KAFKA_OFFSET, logger);
        }

        final Long epoch = getNumericAttribute(flowFile, KAFKA_LEADER_EPOCH, logger);
        final String consumerGroupId = flowFile.getAttribute(KAFKA_CONSUMER_GROUP_ID);

        if (topic == null || partition == null || maxOffset == null || consumerGroupId == null) {
            logger.warn("Cannot commit consumer offsets because at least one of the following FlowFile attributes is missing from {}: {}", flowFile,
                Arrays.asList(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_MAX_OFFSET + " (or " + KAFKA_OFFSET + ")", KAFKA_LEADER_EPOCH, KAFKA_CONSUMER_GROUP_ID));
            return;
        }

        lease.ackConsumerOffsets(topic, partition.intValue(), maxOffset, epoch == null ? null : epoch.intValue(), consumerGroupId);
    }

    private static Long getNumericAttribute(final FlowFile flowFile, final String attributeName, final ComponentLog logger) {
        final String attributeValue = flowFile.getAttribute(attributeName);
        if (attributeValue == null) {
            return null;
        }

        try {
            return Long.parseLong(attributeValue);
        } catch (final NumberFormatException nfe) {
            logger.warn("Expected a numeric value for attribute '{}' but found non-numeric value for {}", attributeName, flowFile);
            return null;
        }
    }
}
