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

import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordMetadata.FIELD_METADATA;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordMetadata.METADATA;
import static org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordMetadata.composeMetadataObject;

public final class WrapperRecordConverter implements KinesisRecordConverter {

    private static final String VALUE = "value";

    @Override
    public Record convert(final Record record, final KinesisClientRecord kinesisRecord, final String streamName, final String shardId) {
        final Record metadata = composeMetadataObject(kinesisRecord, streamName, shardId);

        final RecordSchema convertedSchema = new SimpleRecordSchema(List.of(
                FIELD_METADATA,
                new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(record.getSchema())))
        );
        final Map<String, Object> convertedRecord = Map.of(
                METADATA, metadata,
                VALUE, record
        );

        return new MapRecord(convertedSchema, convertedRecord);
    }
}
