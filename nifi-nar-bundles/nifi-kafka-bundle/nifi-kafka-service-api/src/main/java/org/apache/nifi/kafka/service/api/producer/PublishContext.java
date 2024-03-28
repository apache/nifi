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
package org.apache.nifi.kafka.service.api.producer;

import org.apache.nifi.flowfile.FlowFile;

/**
 * Specification of parameters used by a Kafka processor to publish records to Kafka in the context of an enabled
 * {@link org.apache.nifi.kafka.service.api.KafkaConnectionService}.
 */
public class PublishContext {
    private final String topic;
    private final Integer partition;
    private final Long timestamp;
    private final FlowFile flowFile;

    private Exception exception;

    public PublishContext(final String topic, final Integer partition, final Long timestamp, final FlowFile flowFile) {
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.flowFile = flowFile;
        this.exception = null;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(final Exception e) {
        exception = e;
    }
}
