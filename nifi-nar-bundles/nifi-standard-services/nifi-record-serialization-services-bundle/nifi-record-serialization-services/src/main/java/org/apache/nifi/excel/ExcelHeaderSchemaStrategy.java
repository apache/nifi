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

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.SchemaInferenceUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExcelHeaderSchemaStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    private final PropertyContext context;

    private final DataFormatter dataFormatter;

    public ExcelHeaderSchemaStrategy(PropertyContext context) {
        this.context = context;
        this.dataFormatter = new DataFormatter();
    }

    public ExcelHeaderSchemaStrategy(PropertyContext context, Locale locale) {
        this.context = context;
        this.dataFormatter = new DataFormatter(locale);
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        if (this.context == null) {
            throw new SchemaNotFoundException("Schema Access Strategy intended only for validation purposes and cannot obtain schema");
        }

        String errMsg = "Failed to read Header line from Excel worksheet";
        RecordSource<Row> recordSource;
        try {
            recordSource = new ExcelRecordSource(contentStream, context, variables);
        } catch (Exception e) {
            throw new SchemaNotFoundException(errMsg, e);
        }

        Row headerRow = recordSource.next();
        if (!ExcelUtils.hasCells(headerRow)) {
            throw new SchemaNotFoundException("The chosen header line in the Excel worksheet had no cells");
        }

        try {
            String dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
            String timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
            String timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
            final TimeValueInference timeValueInference = new TimeValueInference(dateFormat, timeFormat, timestampFormat);
            final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
            IntStream.range(0, headerRow.getLastCellNum())
                    .forEach(index -> {
                        final Cell cell = headerRow.getCell(index);
                        final String fieldName = Integer.toString(index);
                        final FieldTypeInference typeInference = typeMap.computeIfAbsent(fieldName, key -> new FieldTypeInference());
                        final String formattedCellValue = dataFormatter.formatCellValue(cell);
                        final DataType dataType = SchemaInferenceUtil.getDataType(formattedCellValue, timeValueInference);
                        typeInference.addPossibleDataType(dataType);
                    });

            final List<RecordField> fields = typeMap.entrySet().stream()
                    .map(entry -> new RecordField(entry.getKey(), entry.getValue().toDataType(), true))
                    .collect(Collectors.toList());

            return new SimpleRecordSchema(fields);
        } catch (Exception e) {
            throw new SchemaNotFoundException(errMsg, e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
