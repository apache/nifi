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
package org.apache.nifi.processors.salesforce.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RecordExtender {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final SimpleRecordSchema ATTRIBUTES_RECORD_SCHEMA = new SimpleRecordSchema(Arrays.asList(
            new RecordField("type", RecordFieldType.STRING.getDataType()),
            new RecordField("referenceId", RecordFieldType.STRING.getDataType())
    ));

    private final RecordSchema extendedSchema;

    public RecordExtender(final RecordSchema originalSchema) {
        final List<RecordField> recordFields = new ArrayList<>(originalSchema.getFields());
        recordFields.add(new RecordField("attributes", RecordFieldType.RECORD.getRecordDataType(
                ATTRIBUTES_RECORD_SCHEMA
        )));

        extendedSchema = new SimpleRecordSchema(recordFields);
    }

    public ObjectNode getWrappedRecordsJson(final ByteArrayOutputStream out) throws IOException {
        final ObjectNode root = MAPPER.createObjectNode();
        final JsonNode jsonNode = MAPPER.readTree(out.toByteArray());
        root.set("records", jsonNode);
        return root;
    }

    public MapRecord getExtendedRecord(final String objectType, final int count, final Record record) {

        final Set<String> rawFieldNames = record.getRawFieldNames().stream()
                .filter(fieldName -> record.getValue(fieldName) != null)
                .collect(Collectors.toSet());
        final Map<String, Object> objectMap = rawFieldNames.stream()
                .collect(Collectors.toMap(Function.identity(), record::getValue));

        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put("type", objectType);
        attributesMap.put("referenceId", count);

        final MapRecord attributesRecord = new MapRecord(ATTRIBUTES_RECORD_SCHEMA, attributesMap);

        objectMap.put("attributes", attributesRecord);

        return new MapRecord(extendedSchema, objectMap);
    }

    public RecordSchema getExtendedSchema() {
        return extendedSchema;
    }
}
