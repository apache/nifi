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
package org.apache.nifi.kafka.service.api.record;

import org.apache.nifi.kafka.service.api.header.RecordHeader;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Byte Record translation of Kafka Record with byte arrays for key and value properties
 */
public class ByteRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final List<RecordHeader> headers;
    private final byte[] key;
    private final byte[] value;
    private final long bundledCount;

    public ByteRecord(
            final String topic,
            final int partition,
            final long offset,
            final long timestamp,
            final List<RecordHeader> headers,
            final byte[] key,
            final byte[] value,
            final long bundledCount
    ) {
        this.topic = Objects.requireNonNull(topic, "Topic required");
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = Objects.requireNonNull(headers, "Headers required");
        this.key = key;
        this.value = Objects.requireNonNull(value, "Value required");
        this.bundledCount = bundledCount;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Optional<byte[]> getKey() {
        return Optional.ofNullable(key);
    }

    public byte[] getValue() {
        return value;
    }

    public List<RecordHeader> getHeaders() {
        return headers;
    }

    public long getBundledCount() {
        return bundledCount;
    }
}
