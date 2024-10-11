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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExcelHeaderSchemaStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);
    static final int NUM_ROWS_TO_DETERMINE_TYPES = 10; // NOTE: This number is arbitrary.
    static final AllowableValue USE_STARTING_ROW = new AllowableValue("Use Starting Row", "Use Starting Row",
            "The configured first row of the Excel file is a header line that contains the names of the columns. The schema will be derived by using the "
                    + "column names in the header and the following " + NUM_ROWS_TO_DETERMINE_TYPES + " rows to determine the type(s) of each column");

    private final PropertyContext context;
    private final ComponentLog logger;
    private final CellFieldTypeReader cellFieldTypeReader;
    private final DataFormatter dataFormatter;

    public ExcelHeaderSchemaStrategy(PropertyContext context, ComponentLog logger, TimeValueInference timeValueInference) {
        this.context = context;
        this.logger = logger;
        this.cellFieldTypeReader = new StandardCellFieldTypeReader(timeValueInference);
        this.dataFormatter = new DataFormatter();
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException {
        if (this.context == null) {
            throw new SchemaNotFoundException("Schema Access Strategy intended only for validation purposes and cannot obtain schema");
        }

        final String requiredSheetsDelimited = context.getProperty(ExcelReader.REQUIRED_SHEETS).evaluateAttributeExpressions(variables).getValue();
        final List<String> requiredSheets = ExcelReader.getRequiredSheets(requiredSheetsDelimited);
        final Integer rawFirstRow = context.getProperty(ExcelReader.STARTING_ROW).evaluateAttributeExpressions(variables).asInteger();
        final int firstRow = rawFirstRow == null ? NumberUtils.toInt(ExcelReader.STARTING_ROW.getDefaultValue()) : rawFirstRow;
        final int zeroBasedFirstRow = ExcelReader.getZeroBasedIndex(firstRow);
        final String password = context.getProperty(ExcelReader.PASSWORD).getValue();
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withRequiredSheets(requiredSheets)
                .withFirstRow(zeroBasedFirstRow)
                .withPassword(password)
                .build();

        final RowIterator rowIterator = new RowIterator(contentStream, configuration, logger);
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        List<String> fieldNames = null;
        int index = 0;

        while (rowIterator.hasNext()) {
           Row row = rowIterator.next();
           if (index == 0) {
               fieldNames = getFieldNames(firstRow, row);
           } else if (index <= NUM_ROWS_TO_DETERMINE_TYPES) {
               inferSchema(row, fieldNames, typeMap);
           } else {
               break;
           }

           index++;
        }

        if (typeMap.isEmpty()) {
            final String message = String.format("Failed to infer schema from empty first %d rows", NUM_ROWS_TO_DETERMINE_TYPES);
            throw new SchemaNotFoundException(message);
        }
        return createSchema(typeMap);
    }

    private List<String> getFieldNames(int firstRowIndex, Row row) throws SchemaNotFoundException {
        if (!ExcelUtils.hasCells(row)) {
            throw new SchemaNotFoundException(String.format("Field names could not be determined from configured header row %s, as this row has no cells with data", firstRowIndex));
        }

        final List<String> fieldNames = new ArrayList<>();
        for (int index = 0; index < row.getLastCellNum(); index++) {
            final Cell cell = row.getCell(index);
            final String fieldName = dataFormatter.formatCellValue(cell);

            // NOTE: This accounts for column(s) which may be empty in the configured starting row.
            if (fieldName == null || fieldName.isEmpty()) {
                fieldNames.add(ExcelUtils.FIELD_NAME_PREFIX + index);
            } else {
                fieldNames.add(fieldName);
            }
        }

        return fieldNames;
    }

    private void inferSchema(final Row row, final List<String> fieldNames, final Map<String, FieldTypeInference> typeMap) throws SchemaNotFoundException {
        // NOTE: This allows rows to be blank when inferring the schema
        if (ExcelUtils.hasCells(row)) {
            if (row.getLastCellNum() > fieldNames.size()) {
                throw new SchemaNotFoundException(String.format("Row %s has %s cells, more than the expected %s number of field names", row.getRowNum(), row.getLastCellNum(), fieldNames.size()));
            }

            IntStream.range(0, row.getLastCellNum())
                    .forEach(index -> {
                        final Cell cell = row.getCell(index);
                        final String fieldName = fieldNames.get(index);
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

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
