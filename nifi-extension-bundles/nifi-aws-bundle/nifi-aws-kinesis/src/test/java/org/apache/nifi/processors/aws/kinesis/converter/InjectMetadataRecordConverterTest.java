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
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.INPUT_RECORD;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.KINESIS_METADATA;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.SCHEMA_METADATA;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.TEST_ARRIVAL_TIMESTAMP;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.TEST_SHARD_ID;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.TEST_STREAM_NAME;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.createTestKinesisRecord;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverterTestUtil.verifyMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InjectMetadataRecordConverterTest {

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
}
