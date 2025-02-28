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
package org.apache.nifi.processors.aws.kinesis.stream.record.converter;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.time.Instant;
import java.util.*;

public class RecordConverterWrapper implements RecordConverter {

    private static final String VALUE = "value";
    private static final String METADATA = "metadata";

    private static final String STREAM = "kinesis.stream";
    private static final String SHARD_ID = "aws.kinesis.shard.id";
    private static final String SEQUENCE_NUMBER = "aws.kinesis.sequence.number";
    private static final String PARTITION_KEY = "aws.kinesis.partition.key";
    private static final String APPROX_ARRIVAL_TIMESTAMP = "aws.kinesis.approximate.arrival.timestamp";

    private static final RecordField FIELD_STREAM = new RecordField(STREAM, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SHARD_ID = new RecordField(SHARD_ID, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SEQUENCE_NUMBER = new RecordField(SEQUENCE_NUMBER, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION_KEY = new RecordField(PARTITION_KEY, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_APPROX_ARRIVAL_TIMESTAMP = new RecordField(APPROX_ARRIVAL_TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());
    private static final RecordSchema SCHEMA_METADATA = new SimpleRecordSchema(Arrays.asList(
            FIELD_STREAM, FIELD_SHARD_ID, FIELD_SEQUENCE_NUMBER, FIELD_PARTITION_KEY, FIELD_APPROX_ARRIVAL_TIMESTAMP));

    public static final RecordField FIELD_METADATA = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(SCHEMA_METADATA));


    @Override
    public Record convert(final Record record, final KinesisClientRecord kinesisRecord, final String streamName, final String shardId) {
        final Tuple<String, Object> metadata = toWrapperRecordMetadata(kinesisRecord, streamName, shardId);
        return new MapRecord(convertToWriteSchema(record.getSchema()), Map.of(metadata.getKey(), metadata.getValue(), VALUE, record));
    }

    private Tuple<String, Object> toWrapperRecordMetadata(final KinesisClientRecord consumerRecord, final String streamName, final String shardId) {
        final Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(STREAM, streamName);
        metadata.put(SHARD_ID, shardId);
        metadata.put(SEQUENCE_NUMBER, consumerRecord.sequenceNumber());
        metadata.put(PARTITION_KEY, consumerRecord.partitionKey());
        final Instant approxArrivalTimestamp = consumerRecord.approximateArrivalTimestamp();
        metadata.put(APPROX_ARRIVAL_TIMESTAMP, approxArrivalTimestamp == null ? null : approxArrivalTimestamp.toEpochMilli());
        final Record record = new MapRecord(SCHEMA_METADATA, metadata);
        return new Tuple<>(METADATA, record);
    }

    private RecordSchema convertToWriteSchema(final RecordSchema readerSchema) {
        final RecordField recordField = new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(readerSchema));
        return new SimpleRecordSchema(List.of(FIELD_METADATA, recordField));
    }
}
