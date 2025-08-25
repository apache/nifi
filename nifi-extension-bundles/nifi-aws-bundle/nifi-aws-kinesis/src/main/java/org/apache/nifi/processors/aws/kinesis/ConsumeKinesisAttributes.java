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
package org.apache.nifi.processors.aws.kinesis;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.HashMap;
import java.util.Map;

final class ConsumeKinesisAttributes {

    private static final String PREFIX = "aws.kinesis.";

    // AWS Kinesis attributes.
    static final String STREAM_NAME = PREFIX + "stream.name";
    static final String SHARD_ID = PREFIX + "shard.id";
    static final String SEQUENCE_NUMBER = PREFIX + "sequence.number";
    static final String SUB_SEQUENCE_NUMBER = PREFIX + "subsequence.number";

    static final String PARTITION_KEY = PREFIX + "partition.key";
    static final String APPROXIMATE_ARRIVAL_TIMESTAMP = PREFIX + "approximate.arrival.timestamp.ms";

    // Record attributes.
    static final String MIME_TYPE = "mime.type";
    static final String RECORD_COUNT = "record.count";
    static final String RECORD_ERROR_MESSAGE = "record.error.message";

    static Map<String, String> fromKinesisRecord(
            final String streamName,
            final String shardId,
            final KinesisClientRecord record) {
        final Map<String, String> attributes = new HashMap<>(6, 1.0f);

        attributes.put(STREAM_NAME, streamName);
        attributes.put(SHARD_ID, shardId);
        attributes.put(SEQUENCE_NUMBER, record.sequenceNumber());
        attributes.put(SUB_SEQUENCE_NUMBER, String.valueOf(record.subSequenceNumber()));
        attributes.put(PARTITION_KEY, record.partitionKey());

        if (record.approximateArrivalTimestamp() != null) {
           attributes.put(APPROXIMATE_ARRIVAL_TIMESTAMP, String.valueOf(record.approximateArrivalTimestamp().toEpochMilli()));
        }

        return attributes;
    }

    private ConsumeKinesisAttributes() {
    }
}
