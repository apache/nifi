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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ExcelRecordReader implements RecordReader {
    private final RowIterator rowIterator;
    private final RecordSchema schema;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    public ExcelRecordReader(ExcelRecordReaderConfiguration configuration, InputStream inputStream, ComponentLog logger) throws MalformedRecordException {
        this.schema = configuration.getSchema();

        if (isEmpty(configuration.getDateFormat())) {
            this.dateFormat = null;
        } else {
            this.dateFormat = configuration.getDateFormat();
        }

        if (isEmpty(configuration.getTimeFormat())) {
            this.timeFormat = null;
        } else {
            this.timeFormat = configuration.getTimeFormat();
        }

        if (isEmpty(configuration.getTimestampFormat())) {
            this.timestampFormat = null;
        } else {
            this.timestampFormat = configuration.getTimestampFormat();
        }

        try {
            this.rowIterator = new RowIterator(inputStream, configuration, logger);
        } catch (RuntimeException e) {
            throw new MalformedRecordException("Read initial Record from Excel XLSX failed", e);
        }
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws MalformedRecordException {
        Row currentRow = null;
        try {
            if (rowIterator.hasNext()) {
                currentRow = rowIterator.next();
                Map<String, Object> currentRowValues = getCurrentRowValues(currentRow, coerceTypes, dropUnknownFields);
                return new MapRecord(schema, currentRowValues);
            }
        } catch (Exception e) {
            String exceptionMessage = "Read next Record from Excel XLSX failed";
            if (currentRow != null) {
                exceptionMessage = String.format("%s on row %s in sheet %s",
                        exceptionMessage, currentRow.getRowNum(), currentRow.getSheet().getSheetName());
            }
            throw new MalformedRecordException(exceptionMessage, e);
        }
        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {
        this.rowIterator.close();
    }

    private Map<String, Object> getCurrentRowValues(Row currentRow, boolean coerceTypes, boolean dropUnknownFields) {
        final List<RecordField> recordFields = schema.getFields();
        final Map<String, Object> currentRowValues = new LinkedHashMap<>();

        if (ExcelUtils.hasCells(currentRow)) {
            IntStream.range(0, currentRow.getLastCellNum())
                    .forEach(index -> {
                        Cell cell = currentRow.getCell(index);
                        Object cellValue;
                        if (index >= recordFields.size()) {
                            if (!dropUnknownFields) {
                                cellValue = getCellValue(cell);
                                currentRowValues.put("unknown_field_index_" + index, cellValue);
                            }
                        } else {
                            final RecordField recordField = recordFields.get(index);
                            String fieldName = recordField.getFieldName();
                            DataType dataType = recordField.getDataType();
                            cellValue = getCellValue(cell);
                            final Object value = coerceTypes ? convert(cellValue, dataType, fieldName)
                                    : convertSimpleIfPossible(cellValue, dataType, fieldName);
                            currentRowValues.put(fieldName, value);
                        }
                    });
        }

        return currentRowValues;
    }

    private static Object getCellValue(final Cell cell) {
        final Object cellValue;

        if (cell == null) {
            cellValue = null;
        } else {
            final CellType cellType = cell.getCellType();
            cellValue = switch (cellType) {
                case _NONE, BLANK, ERROR, STRING -> cell.getStringCellValue();
                case NUMERIC -> DateUtil.isCellDateFormatted(cell) ? cell.getDateCellValue() : cell.getNumericCellValue();
                case BOOLEAN -> cell.getBooleanCellValue();
                case FORMULA -> getFormulaCellValue(cell);
            };
        }

        return cellValue;
    }

    private static Object getFormulaCellValue(final Cell cell) {
        final CellType formulaResultType = cell.getCachedFormulaResultType();
        return switch (formulaResultType) {
            case BOOLEAN -> cell.getBooleanCellValue();
            case STRING, ERROR -> cell.getStringCellValue();
            case NUMERIC -> DateUtil.isCellDateFormatted(cell) ? cell.getDateCellValue() : cell.getNumericCellValue();
            default -> null;
        };
    }

    private Object convert(final Object value, final DataType dataType, final String fieldName) {
        if (value == null || dataType == null) {
            return value;
        }

        return DataTypeUtils.convertType(value, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
    }

    private Object convertSimpleIfPossible(final Object value, final DataType dataType, final String fieldName) {
        if (value == null || dataType == null) {
            return value;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(value, dataType)) {
                    return DataTypeUtils.convertType(value, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(value, dateFormat)) {
                    return DataTypeUtils.convertType(value, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(value, timeFormat)) {
                    return DataTypeUtils.convertType(value, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(value, timestampFormat)) {
                    return DataTypeUtils.convertType(value, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
        }

        return value;
    }
}
