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
package org.apache.nifi.processors.aws.kinesis.converter;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class KinesisRecordMetadata {

    static final String METADATA = "kinesisMetadata";
    static final String APPROX_ARRIVAL_TIMESTAMP = "approximateArrival";

    private static final String STREAM = "stream";
    private static final String SHARD_ID = "shardId";
    private static final String SEQUENCE_NUMBER = "sequenceNumber";
    private static final String SUB_SEQUENCE_NUMBER = "subSequenceNumber";
    private static final String SHARDED_SEQUENCE_NUMBER = "shardedSequenceNumber";
    private static final String PARTITION_KEY = "partitionKey";

    private static final RecordField FIELD_STREAM = new RecordField(STREAM, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SHARD_ID = new RecordField(SHARD_ID, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SEQUENCE_NUMBER = new RecordField(SEQUENCE_NUMBER, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SUB_SEQUENCE_NUMBER = new RecordField(SUB_SEQUENCE_NUMBER, RecordFieldType.LONG.getDataType());
    private static final RecordField FIELD_SHARDED_SEQUENCE_NUMBER = new RecordField(SHARDED_SEQUENCE_NUMBER, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION_KEY = new RecordField(PARTITION_KEY, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_APPROX_ARRIVAL_TIMESTAMP = new RecordField(APPROX_ARRIVAL_TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());

    private static final RecordSchema SCHEMA_METADATA = new SimpleRecordSchema(List.of(
            FIELD_STREAM,
            FIELD_SHARD_ID,
            FIELD_SEQUENCE_NUMBER,
            FIELD_SUB_SEQUENCE_NUMBER,
            FIELD_SHARDED_SEQUENCE_NUMBER,
            FIELD_PARTITION_KEY,
            FIELD_APPROX_ARRIVAL_TIMESTAMP));

    static final RecordField FIELD_METADATA = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(SCHEMA_METADATA));

    static Record composeMetadataObject(final KinesisClientRecord kinesisRecord, final String streamName, final String shardId) {
        final Map<String, Object> metadata = new HashMap<>(7, 1.0f);

        metadata.put(STREAM, streamName);
        metadata.put(SHARD_ID, shardId);
        metadata.put(SEQUENCE_NUMBER, kinesisRecord.sequenceNumber());
        metadata.put(SUB_SEQUENCE_NUMBER, kinesisRecord.subSequenceNumber());
        metadata.put(SHARDED_SEQUENCE_NUMBER, "%s%020d".formatted(kinesisRecord.sequenceNumber(), kinesisRecord.subSequenceNumber()));
        metadata.put(PARTITION_KEY, kinesisRecord.partitionKey());

        if (kinesisRecord.approximateArrivalTimestamp() != null) {
            metadata.put(APPROX_ARRIVAL_TIMESTAMP, kinesisRecord.approximateArrivalTimestamp().toEpochMilli());
        }

        return new MapRecord(SCHEMA_METADATA, metadata);
    }

    private KinesisRecordMetadata() {
    }
}
