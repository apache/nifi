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
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ExcelRecordReader implements RecordReader {
    private final RowIterator rowIterator;
    private ComponentLog logger;
    private final RecordSchema schema;
    private final List<String> desiredSheets;
    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    public ExcelRecordReader(ExcelRecordReaderArgs args) throws MalformedRecordException {
        this.logger = args.getLogger();
        this.schema = args.getSchema();
        this.desiredSheets = new ArrayList<>();
        if (args.getDesiredSheets() != null && args.getDesiredSheets().length() > 0) {
            IntStream.range(0, args.getDesiredSheets().length())
                    .forEach(index -> this.desiredSheets.add(args.getDesiredSheets().get(index)));
        }

        if (isEmpty(args.getDateFormat())) {
            this.dateFormat = null;
            LAZY_DATE_FORMAT = null;
        } else {
            this.dateFormat = args.getDateFormat();
            LAZY_DATE_FORMAT = () -> DataTypeUtils.getDateFormat(dateFormat);
        }

        if (isEmpty(args.getTimeFormat())) {
            this.timeFormat = null;
            LAZY_TIME_FORMAT = null;
        } else {
            this.timeFormat = args.getTimeFormat();
            LAZY_TIME_FORMAT = () -> DataTypeUtils.getDateFormat(timeFormat);
        }

        if (isEmpty(args.getTimestampFormat())) {
            this.timestampFormat = null;
            LAZY_TIMESTAMP_FORMAT = null;
        } else {
            this.timestampFormat = args.getTimestampFormat();
            LAZY_TIMESTAMP_FORMAT = () -> DataTypeUtils.getDateFormat(timestampFormat);
        }

        try {
            this.rowIterator = new RowIterator(args.getInputStream(), desiredSheets, args.getFirstRow(), logger);
        } catch (RuntimeException e) {
            String msg = "Error occurred while processing record file";
            logger.error(msg, e);
            throw new MalformedRecordException(msg, e);
        }
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            if (rowIterator.hasNext()) {
                Row currentRow = rowIterator.next();
                Map<String, Object> currentRowValues = getCurrentRowValues(currentRow, coerceTypes, dropUnknownFields);
                return new MapRecord(schema, currentRowValues);
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }
        return null;
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

    public static Object getCellValue(Cell cell) {
        if (cell != null) {
            switch (cell.getCellType()) {
                case _NONE:
                case BLANK:
                case ERROR:
                case FORMULA:
                case STRING:
                    return cell.getStringCellValue();
                case NUMERIC:
                    return DateUtil.isCellDateFormatted(cell) ? cell.getDateCellValue() : cell.getNumericCellValue();
                case BOOLEAN:
                    return cell.getBooleanCellValue();
            }
        }
        return null;
    }

    private Object convert(final Object value, final DataType dataType, final String fieldName) {
        if (value == null || dataType == null) {
            return value;
        }

        return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
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
                    return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(value, dateFormat)) {
                    return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(value, timeFormat)) {
                    return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(value, timestampFormat)) {
                    return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
        }

        return value;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        this.rowIterator.close();
    }

    void setLogger(ComponentLog logger) {
        this.logger = logger;
        this.rowIterator.setLogger(logger);
    }
}
