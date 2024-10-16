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
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of Cell Field Type Reader uses Cell Type and Cell Value information with inference based on Timestamp formats
 */
class StandardCellFieldTypeReader implements CellFieldTypeReader {
    private final TimeValueInference timeValueInference;

    /**
     * Standard Cell Field Type Reader constructor with Time Value Inference for handling STRING Cell Types that may contain values with Timestamps
     *
     * @param timeValueInference Time Value Inference required for STRING Cell Type evaluation
     */
    StandardCellFieldTypeReader(final TimeValueInference timeValueInference) {
        this.timeValueInference = Objects.requireNonNull(timeValueInference, "Time Value Inference required");
    }

    /**
     * Infer Cell Field Type and append possible Data Types to mapped Field Type Inference information
     *
     * @param cell Spreadsheet Cell can be null
     * @param fieldName Cell field name for tracking in Field Types required
     * @param fieldTypes Map of Field Name to Field Type Inference information required
     */
    @Override
    public void inferCellFieldType(final Cell cell, final String fieldName, final Map<String, FieldTypeInference> fieldTypes) {
        Objects.requireNonNull(fieldName, "Field Name required");
        Objects.requireNonNull(fieldTypes, "Field Types required");

        final FieldTypeInference fieldTypeInference = fieldTypes.computeIfAbsent(fieldName, key -> new FieldTypeInference());
        final DataType dataType = getCellDataType(cell);
        fieldTypeInference.addPossibleDataType(dataType);
    }

    /**
     * Get Record Data Type from Spreadsheet Cell Type and additional resolution of NUMERIC and STRING types
     *
     * @param cell Spreadsheet Cell can be null
     * @return Record Data Type or null
     */
    @Override
    public DataType getCellDataType(final Cell cell) {
        if (cell == null) {
            return null;
        }

        final CellType cellType = cell.getCellType();

        final DataType dataType;

        if (CellType.NUMERIC == cellType) {
            // Date Formatting check limited to NUMERIC Cell Types
            final double numericCellValue = cell.getNumericCellValue();
            if (DateUtil.isCellDateFormatted(cell)) {
                dataType = getDateTimeDataType(numericCellValue);
            } else {
                if (isWholeNumber(numericCellValue)) {
                    dataType = RecordFieldType.LONG.getDataType();
                } else {
                    // Default to DOUBLE for NUMERIC values following cell.getNumericCellValue()
                    dataType = RecordFieldType.DOUBLE.getDataType();
                }
            }
        } else if (CellType.BOOLEAN == cellType) {
            dataType = RecordFieldType.BOOLEAN.getDataType();
        } else if (CellType.STRING == cellType) {
            final String cellValue = cell.getStringCellValue();
            // Attempt Time Value inference for STRING cell values
            final Optional<DataType> timeDataType = timeValueInference.getDataType(cellValue);
            dataType = timeDataType.orElse(RecordFieldType.STRING.getDataType());
        } else if (CellType.FORMULA == cellType) {
            dataType = getFormulaResultDataType(cell);
        } else {
            // Default to null for known and unknown Cell Types
            dataType = null;
        }

        return dataType;
    }

    private DataType getDateTimeDataType(final double numericCellValue) {
        final DataType dataType;

        if (isWholeNumber(numericCellValue)) {
            // Numbers without decimal fractions indicate Dates without Times
            dataType = RecordFieldType.DATE.getDataType();
        } else if (numericCellValue < 1) {
            // Decimal fractions indicate Times without Dates
            dataType = RecordFieldType.TIME.getDataType();
        } else {
            dataType = RecordFieldType.TIMESTAMP.getDataType();
        }

        return dataType;
    }

    private DataType getFormulaResultDataType(final Cell cell) {
        final DataType dataType;

        final CellType formulaResultType = cell.getCachedFormulaResultType();
        if (CellType.BOOLEAN == formulaResultType) {
            dataType = RecordFieldType.BOOLEAN.getDataType();
        } else if (CellType.STRING == formulaResultType) {
            dataType = RecordFieldType.STRING.getDataType();
        } else if (CellType.NUMERIC == formulaResultType) {
            // Date Formatting check limited to NUMERIC Cell Types without Conditional Formatting Evaluator
            if (DateUtil.isCellDateFormatted(cell)) {
                final double numericCellValue = cell.getNumericCellValue();
                dataType = getDateTimeDataType(numericCellValue);
            } else {
                // Default to DOUBLE for NUMERIC values following cell.getNumericCellValue()
                dataType = RecordFieldType.DOUBLE.getDataType();
            }
        } else {
            // Default to null for known and unknown Formula Result Cell Types
            dataType = null;
        }

        return dataType;
    }

    private boolean isWholeNumber(final double numericCellValue) {
        final long roundedCellValue = (long) numericCellValue;
        return roundedCellValue == numericCellValue;
    }
}
