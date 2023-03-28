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

/**
 * The NiFi representation of a <code>org.apache.kafka.clients.producer.ProducerRecord&lt;byte[], byte[]&gt;</code>,
 * intended to abstract Kafka library internals from NiFi Kafka processor implementations.
 */
public class KafkaRecord {
    private final String topic;
    private final Integer partition;
    private final List<RecordHeader> headers;
    private final byte[] key;
    private final byte[] value;
    private final Long timestamp;

    public KafkaRecord(
            final String topic,
            final Integer partition,
            final Long timestamp,
            final byte[] key,
            final byte[] value,
            final List<RecordHeader> headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public List<RecordHeader> getHeaders() {
        return headers;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
