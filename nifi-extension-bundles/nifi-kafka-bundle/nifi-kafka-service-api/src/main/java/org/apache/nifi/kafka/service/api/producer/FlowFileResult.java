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

import java.util.List;
import java.util.Map;

/**
 * Container for results of processing / sending one FlowFile to Kafka.
 */
public class FlowFileResult {
    private final FlowFile flowFile;  // source data from NiFi
    private final long sentCount;  // count of converted Kafka records (from NiFi)
    private final Map<String, Long> sentPerTopic;
    private final List<ProducerRecordMetadata> metadatas;  // success results (from Kafka callback)
    private final List<Exception> exceptions;  // failure results (from Kafka callback)

    public FlowFileResult(final FlowFile flowFile, final long sentCount, final Map<String, Long> sentPerTopic,
                          final List<ProducerRecordMetadata> metadatas, final List<Exception> exceptions) {
        this.flowFile = flowFile;
        this.sentCount = sentCount;
        this.sentPerTopic = sentPerTopic;
        this.metadatas = metadatas;
        this.exceptions = exceptions;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public long getSentCount() {
        return sentCount;
    }

    public Map<String, Long> getSentPerTopic() {
        return sentPerTopic;
    }

    public List<ProducerRecordMetadata> getMetadatas() {
        return metadatas;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }
}
