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
package org.apache.nifi.excel;

import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExcelSchemaInference implements SchemaInferenceEngine<Row> {
    private final CellFieldTypeReader cellFieldTypeReader;

    public ExcelSchemaInference(TimeValueInference timeValueInference) {
        this.cellFieldTypeReader = new StandardCellFieldTypeReader(timeValueInference);
    }

    @Override
    public RecordSchema inferSchema(RecordSource<Row> recordSource) throws IOException {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        Row row;
        while ((row = recordSource.next()) != null) {
            inferSchema(row, typeMap);
        }
        return createSchema(typeMap);
    }

    private void inferSchema(final Row row, final Map<String, FieldTypeInference> typeMap) {
        if (ExcelUtils.hasCells(row)) {
            IntStream.range(0, row.getLastCellNum())
                    .forEach(index -> {
                        final Cell cell = row.getCell(index);
                        final String fieldName = ExcelUtils.FIELD_NAME_PREFIX + index;
                        cellFieldTypeReader.inferCellFieldType(cell, fieldName, typeMap);
                    });
        }
    }

    private RecordSchema createSchema(final Map<String, FieldTypeInference> inferences) {
        final List<RecordField> recordFields = inferences.entrySet().stream()
                .map(entry -> new RecordField(entry.getKey(), entry.getValue().toDataType(), true))
                .collect(Collectors.toList());
        return new SimpleRecordSchema(recordFields);
    }
}