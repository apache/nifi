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
package org.apache.nifi.kafka.processors.consumer.bundle;

import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.header.RecordHeader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BundleKey {
    final TopicPartitionSummary topicPartition;
    final long timestamp;
    final List<RecordHeader> headers;
    final List<RecordHeader> headersFiltered;
    final Map<String, String> attributes;
    final byte[] messageKey;

    public BundleKey(final TopicPartitionSummary topicPartition,
                     final long timestamp,
                     final List<RecordHeader> headers,
                     final List<RecordHeader> headersFiltered,
                     final Map<String, String> attributes,
                     final byte[] messageKey) {
        this.topicPartition = Objects.requireNonNull(topicPartition);
        this.timestamp = timestamp;
        this.headers = Objects.requireNonNull(headers);
        this.headersFiltered = Objects.requireNonNull(headersFiltered);
        this.attributes = attributes;
        this.messageKey = messageKey;
    }

    public TopicPartitionSummary getTopicPartition() {
        return topicPartition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<RecordHeader> getHeaders() {
        return headers;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public byte[] getMessageKey() {
        return messageKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final BundleKey bundleKey = (BundleKey) o;
        return Objects.equals(topicPartition, bundleKey.topicPartition)
                && Objects.equals(headersFiltered, bundleKey.headersFiltered)
                && Arrays.equals(messageKey, bundleKey.messageKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, headersFiltered, Arrays.hashCode(messageKey));
    }
}
