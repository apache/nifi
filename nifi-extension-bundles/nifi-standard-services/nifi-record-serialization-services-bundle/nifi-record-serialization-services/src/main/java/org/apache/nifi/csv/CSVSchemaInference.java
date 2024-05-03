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
package org.apache.nifi.csv;

import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.SchemaInferenceUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CSVSchemaInference implements SchemaInferenceEngine<CSVRecordAndFieldNames> {

    private final TimeValueInference timeValueInference;

    public CSVSchemaInference(final TimeValueInference timeValueInference) {
        this.timeValueInference = timeValueInference;
    }

    @Override
    public RecordSchema inferSchema(final RecordSource<CSVRecordAndFieldNames> recordSource) throws IOException {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        while (true) {
            final CSVRecordAndFieldNames recordAndFieldNames = recordSource.next();
            if (recordAndFieldNames == null) {
                // If there are no records, assume the datatypes of all fields are strings
                if (typeMap.isEmpty()) {
                    if (recordSource instanceof CSVRecordSource) {
                        CSVRecordSource csvRecordSource = (CSVRecordSource) recordSource;
                        for (String fieldName : csvRecordSource.getFieldNames()) {
                            typeMap.put(fieldName, new FieldTypeInference());
                        }
                    }
                }
                break;
            }

            inferSchema(recordAndFieldNames, typeMap);
        }
        return createSchema(typeMap);
    }

    private void inferSchema(final CSVRecordAndFieldNames recordAndFieldNames, final Map<String, FieldTypeInference> typeMap) {
        final CSVRecord csvRecord = recordAndFieldNames.getRecord();
        for (final String fieldName : recordAndFieldNames.getFieldNames()) {
            final String value = csvRecord.get(fieldName);
            if (value == null) {
                return;
            }

            final FieldTypeInference typeInference = typeMap.computeIfAbsent(fieldName, key -> new FieldTypeInference());
            final String trimmed = trim(value);
            final DataType dataType = SchemaInferenceUtil.getDataType(trimmed, timeValueInference);
            typeInference.addPossibleDataType(dataType);
        }
    }

    private String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    private RecordSchema createSchema(final Map<String, FieldTypeInference> inferences) {
        final List<RecordField> recordFields = new ArrayList<>(inferences.size());
        inferences.forEach((fieldName, type) -> recordFields.add(new RecordField(fieldName, type.toDataType(), true)));
        return new SimpleRecordSchema(recordFields);
    }
}
