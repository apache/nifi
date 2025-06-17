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

import jakarta.annotation.Nullable;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InjectMetadataRecordConverterTest {

    private static final String KINESIS_METADATA = "kinesisMetadata";

    private static final String TEST_STREAM_NAME = "test-stream";
    private static final String TEST_SHARD_ID = "shardId-000000000001";
    private static final String TEST_SEQUENCE_NUMBER = "49590338271490256608559692538361571095921575989136588801";
    private static final long TEST_SUB_SEQUENCE_NUMBER = 2;
    private static final String TEST_PARTITION_KEY = "test-partition-key";
    private static final Instant TEST_ARRIVAL_TIMESTAMP = Instant.ofEpochMilli(1640995200000L);

    private static final String EXPECTED_SHARDED_SEQUENCE_NUMBER = "4959033827149025660855969253836157109592157598913658880100000000000000000002";

    private static final RecordSchema INPUT_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("age", RecordFieldType.INT.getDataType())
    ));
    private static final Record INPUT_RECORD = new MapRecord(INPUT_SCHEMA, Map.of(
            "name", "John Doe",
            "age", 30
    ));

    private static final RecordSchema SCHEMA_METADATA = new SimpleRecordSchema(List.of(
            new RecordField("stream", RecordFieldType.STRING.getDataType()),
            new RecordField("shardId", RecordFieldType.STRING.getDataType()),
            new RecordField("sequenceNumber", RecordFieldType.STRING.getDataType()),
            new RecordField("subSequenceNumber", RecordFieldType.LONG.getDataType()),
            new RecordField("shardedSequenceNumber", RecordFieldType.STRING.getDataType()),
            new RecordField("partitionKey", RecordFieldType.STRING.getDataType()),
            new RecordField("approximateArrival", RecordFieldType.TIMESTAMP.getDataType())
    ));
    private static final RecordSchema EXPECTED_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("age", RecordFieldType.INT.getDataType()),
            new RecordField(KINESIS_METADATA, RecordFieldType.RECORD.getRecordDataType(SCHEMA_METADATA))
    ));

    private static final InjectMetadataRecordConverter CONVERTER = new InjectMetadataRecordConverter();

    @Test
    void testConvertWithApproximateArrivalTimestamp() {
        final KinesisClientRecord kinesisRecord = createTestKinesisRecord(TEST_ARRIVAL_TIMESTAMP);

        final Record record = CONVERTER.convert(INPUT_RECORD, kinesisRecord, TEST_STREAM_NAME, TEST_SHARD_ID);

        assertEquals(EXPECTED_SCHEMA, record.getSchema());

        final Map<String, Object> recordValues = new HashMap<>(record.toMap());
        recordValues.remove(KINESIS_METADATA);
        assertEquals(INPUT_RECORD.toMap(), recordValues);

        final Record metadata = record.getAsRecord(KINESIS_METADATA, SCHEMA_METADATA);
        final boolean expectTimestamp = true;
        verifyMetadata(metadata, expectTimestamp);
    }

    @Test
    void testConvertWithoutApproximateArrivalTimestamp() {
        final KinesisClientRecord kinesisRecord = createTestKinesisRecord(null);

        final Record record = CONVERTER.convert(INPUT_RECORD, kinesisRecord, TEST_STREAM_NAME, TEST_SHARD_ID);

        assertEquals(EXPECTED_SCHEMA, record.getSchema());

        final Map<String, Object> recordValues = new HashMap<>(record.toMap());
        recordValues.remove(KINESIS_METADATA);
        assertEquals(INPUT_RECORD.toMap(), recordValues);

        final Record metadata = record.getAsRecord(KINESIS_METADATA, SCHEMA_METADATA);
        final boolean expectTimestamp = false;
        verifyMetadata(metadata, expectTimestamp);
    }

    private KinesisClientRecord createTestKinesisRecord(final @Nullable Instant arrivalTimestamp) {
        return KinesisClientRecord.builder()
                .data(ByteBuffer.allocate(0))
                .sequenceNumber(TEST_SEQUENCE_NUMBER)
                .subSequenceNumber(TEST_SUB_SEQUENCE_NUMBER)
                .partitionKey(TEST_PARTITION_KEY)
                .approximateArrivalTimestamp(arrivalTimestamp)
                .build();
    }

    private static void verifyMetadata(final Record metadata, final boolean expectTimestamp) {
        assertEquals(TEST_STREAM_NAME, metadata.getValue("stream"));
        assertEquals(TEST_SHARD_ID, metadata.getValue("shardId"));
        assertEquals(TEST_SEQUENCE_NUMBER, metadata.getValue("sequenceNumber"));
        assertEquals(TEST_SUB_SEQUENCE_NUMBER, metadata.getValue("subSequenceNumber"));
        assertEquals(EXPECTED_SHARDED_SEQUENCE_NUMBER, metadata.getValue("shardedSequenceNumber"));
        assertEquals(TEST_PARTITION_KEY, metadata.getValue("partitionKey"));

        if (expectTimestamp) {
            assertEquals(TEST_ARRIVAL_TIMESTAMP.toEpochMilli(), metadata.getValue("approximateArrival"));
        } else {
            assertNull(metadata.getValue("approximateArrival"));
        }
    }
}
