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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Holder of context information used by {@link KafkaPublisher} required to
 * publish messages to Kafka.
 */
class PublishingContext {

    private final InputStream contentStream;

    private final String topic;

    private final int lastAckedMessageIndex;

    private final int maxRequestSize;

    private byte[] keyBytes;

    private byte[] delimiterBytes;

    PublishingContext(InputStream contentStream, String topic) {
        this(contentStream, topic, -1);
    }

    PublishingContext(InputStream contentStream, String topic, int lastAckedMessageIndex) {
        this(contentStream, topic, lastAckedMessageIndex, 1048576);
    }

    PublishingContext(InputStream contentStream, String topic, int lastAckedMessageIndex, int maxRequestSize) {
        this.validateInput(contentStream, topic, lastAckedMessageIndex);
        this.contentStream = contentStream;
        this.topic = topic;
        this.lastAckedMessageIndex = lastAckedMessageIndex;
        this.maxRequestSize = maxRequestSize;
    }

    @Override
    public String toString() {
        return "topic: '" + this.topic + "'; delimiter: '" + new String(this.delimiterBytes, StandardCharsets.UTF_8) + "'";
    }

    int getLastAckedMessageIndex() {
        return this.lastAckedMessageIndex;
    }

    int getMaxRequestSize() {
        return this.maxRequestSize;
    }

    byte[] getKeyBytes() {
        return this.keyBytes;
    }

    byte[] getDelimiterBytes() {
        return this.delimiterBytes;
    }

    InputStream getContentStream() {
        return this.contentStream;
    }

    String getTopic() {
        return this.topic;
    }

    void setKeyBytes(byte[] keyBytes) {
        if (this.keyBytes == null) {
            if (keyBytes != null) {
                this.assertBytesValid(keyBytes);
                this.keyBytes = keyBytes;
            }
        } else {
            throw new IllegalArgumentException("'keyBytes' can only be set once per instance");
        }
    }

    void setDelimiterBytes(byte[] delimiterBytes) {
        if (this.delimiterBytes == null) {
            if (delimiterBytes != null) {
                this.assertBytesValid(delimiterBytes);
                this.delimiterBytes = delimiterBytes;
            }
        } else {
            throw new IllegalArgumentException("'delimiterBytes' can only be set once per instance");
        }
    }

    private void assertBytesValid(byte[] bytes) {
        if (bytes != null) {
            if (bytes.length == 0) {
                throw new IllegalArgumentException("'bytes' must not be empty");
            }
        }
    }

    private void validateInput(InputStream contentStream, String topic, int lastAckedMessageIndex) {
        if (contentStream == null) {
            throw new IllegalArgumentException("'contentStream' must not be null");
        } else if (topic == null || topic.trim().length() == 0) {
            throw new IllegalArgumentException("'topic' must not be null or empty");
        } else if (lastAckedMessageIndex < -1) {
            throw new IllegalArgumentException("'lastAckedMessageIndex' must be >= -1");
        }
    }
}
