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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExcelStartingRowSchemaInference implements SchemaInferenceEngine<Row> {

    static final AllowableValue USE_STARTING_ROW = new AllowableValue("Use Starting Row", "Use Starting Row",
            "The configured first row of the Excel file is a header line that contains the names of the columns. The schema will be derived by using the "
                    + "column names in the header of the first sheet and dependent on the strategy chosen either the subsequent "
                    + RowEvaluationStrategy.NUM_ROWS_TO_DETERMINE_TYPES + " rows or all of the subsequent rows. However the configured header rows of subsequent sheets are skipped. "
                    + "NOTE: If there are duplicate column names then each subsequent duplicate column name is given a one up number. "
                    + "For example, column names \"Name\", \"Name\" will be changed to \"Name\", \"Name_1\".");
    private final RowEvaluationStrategy rowEvaluationStrategy;
    private final int firstRow;
    private final CellFieldTypeReader cellFieldTypeReader;
    private final DataFormatter dataFormatter;

    public ExcelStartingRowSchemaInference(RowEvaluationStrategy rowEvaluationStrategy, int firstRow, TimeValueInference timeValueInference) {
        this.rowEvaluationStrategy = rowEvaluationStrategy;
        this.firstRow = firstRow;
        this.cellFieldTypeReader = new StandardCellFieldTypeReader(timeValueInference);
        this.dataFormatter = new DataFormatter();
    }

    @Override
    public RecordSchema inferSchema(RecordSource<Row> recordSource) throws IOException {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        final int zeroBasedFirstRow = ExcelReader.getZeroBasedIndex(firstRow);
        List<String> fieldNames = null;
        int index = 0;
        Row row;

        while ((row = recordSource.next()) != null) {
            if (index == 0) {
                fieldNames = getFieldNames(firstRow, row);
            } else if (row.getRowNum() == zeroBasedFirstRow) { // skip first row of all sheets
                continue;
            } else {
                if (RowEvaluationStrategy.STANDARD == rowEvaluationStrategy) {
                    if (index <= RowEvaluationStrategy.NUM_ROWS_TO_DETERMINE_TYPES) {
                        inferSchema(row, fieldNames, typeMap);
                    } else {
                        break;
                    }
                } else {
                    inferSchema(row, fieldNames, typeMap);
                }
            }
            index++;
        }
        return createSchema(typeMap);
    }

    private List<String> getFieldNames(int firstRowIndex, Row row) throws IOException {
        if (!ExcelUtils.hasCells(row)) {
            throw new IOException(new SchemaNotFoundException(String.format("Field names could not be determined from configured header row %s, as this row has no cells with data", firstRowIndex)));
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
        final List<String> renamedDuplicateFieldNames = renameDuplicateFieldNames(fieldNames);

        return renamedDuplicateFieldNames;
    }

    private List<String> renameDuplicateFieldNames(final List<String> fieldNames) {
        final Map<String, Integer> fieldNameCounts = new HashMap<>();
        final List<String> renamedDuplicateFieldNames = new ArrayList<>();

        for (String fieldName : fieldNames) {
            if (fieldNameCounts.containsKey(fieldName)) {
                final int count = fieldNameCounts.get(fieldName);
                renamedDuplicateFieldNames.add("%s_%d".formatted(fieldName, count));
                fieldNameCounts.put(fieldName, count + 1);
            } else {
                fieldNameCounts.put(fieldName, 1);
                renamedDuplicateFieldNames.add(fieldName);
            }
        }
        return renamedDuplicateFieldNames;
    }

    private void inferSchema(final Row row, final List<String> fieldNames, final Map<String, FieldTypeInference> typeMap) throws IOException {
        // NOTE: This allows rows to be blank when inferring the schema
        if (ExcelUtils.hasCells(row)) {
            if (row.getLastCellNum() > fieldNames.size()) {
                throw new IOException(new SchemaNotFoundException(String.format("Row %s has %s cells, more than the expected %s number of field names",
                        row.getRowNum(), row.getLastCellNum(), fieldNames.size())));
            }

            IntStream.range(0, row.getLastCellNum())
                    .forEach(index -> {
                        final Cell cell = row.getCell(index);
                        final String fieldName = fieldNames.get(index);
                        cellFieldTypeReader.inferCellFieldType(cell, fieldName, typeMap);
                    });
        }
    }

    private RecordSchema createSchema(final Map<String, FieldTypeInference> inferences) throws IOException {
        if (inferences.isEmpty()) {
            throw new IOException(new SchemaNotFoundException("Failed to infer schema from empty rows"));
        }

        final List<RecordField> recordFields = inferences.entrySet().stream()
                .map(entry -> new RecordField(entry.getKey(), entry.getValue().toDataType(), true))
                .collect(Collectors.toList());
        return new SimpleRecordSchema(recordFields);
    }
}
