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
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardCellFieldTypeReader {

    private static final String TIMESTAMP_FORMATTED = "2020-01-01 12:00:00";

    private static final double NUMERIC_DOUBLE = 123.45;

    private static final Long NUMERIC_LONG = Long.MAX_VALUE;

    private static final Long NUMERIC_TIMESTAMP = 45678L;

    private static final short EXCEL_INTERNAL_DATE_FORMAT = 14;

    private static final String FIELD_NAME = "FirstField";

    @Mock
    private Cell cell;

    @Mock
    private CellStyle cellStyle;

    @Mock
    private TimeValueInference timeValueInference;

    private StandardCellFieldTypeReader reader;

    @BeforeEach
    void setReader() {
        reader = new StandardCellFieldTypeReader(timeValueInference);
    }

    @Test
    void testGetCellDataTypeBlank() {
        when(cell.getCellType()).thenReturn(CellType.BLANK);

        final DataType dataType = reader.getCellDataType(cell);

        assertNull(dataType);
    }

    @Test
    void testGetCellDataTypeError() {
        when(cell.getCellType()).thenReturn(CellType.ERROR);

        final DataType dataType = reader.getCellDataType(cell);

        assertNull(dataType);
    }

    @Test
    void testGetCellDataTypeFormula() {
        when(cell.getCellType()).thenReturn(CellType.FORMULA);

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.STRING.getDataType(), dataType);
    }

    @Test
    void testGetCellDataTypeBoolean() {
        when(cell.getCellType()).thenReturn(CellType.BOOLEAN);

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.BOOLEAN.getDataType(), dataType);
    }

    @Test
    void testGetCellDataTypeString() {
        when(cell.getCellType()).thenReturn(CellType.STRING);

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.STRING.getDataType(), dataType);
    }

    @Test
    void testGetCellDataTypeStringTimestamp() {
        final DataType timestampDataType = RecordFieldType.TIMESTAMP.getDataType();

        when(cell.getCellType()).thenReturn(CellType.STRING);
        when(cell.getStringCellValue()).thenReturn(TIMESTAMP_FORMATTED);
        when(timeValueInference.getDataType(eq(TIMESTAMP_FORMATTED))).thenReturn(Optional.of(timestampDataType));

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(timestampDataType, dataType);
    }

    @Test
    void testGetCellDataTypeNumericDouble() {
        when(cell.getCellType()).thenReturn(CellType.NUMERIC);
        when(cell.getNumericCellValue()).thenReturn(NUMERIC_DOUBLE);

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.DOUBLE.getDataType(), dataType);
    }

    @Test
    void testGetCellDataTypeNumericLong() {
        when(cell.getCellType()).thenReturn(CellType.NUMERIC);
        when(cell.getNumericCellValue()).thenReturn(NUMERIC_LONG.doubleValue());

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.LONG.getDataType(), dataType);
    }

    @Test
    void testGetCellDataTypeNumericTimestamp() {
        when(cell.getCellType()).thenReturn(CellType.NUMERIC);
        when(cell.getNumericCellValue()).thenReturn(NUMERIC_TIMESTAMP.doubleValue());
        when(cell.getCellStyle()).thenReturn(cellStyle);
        // Set Data Format to internal Date Format for Data Type detection in DateUtil.isCellDateFormatted
        when(cellStyle.getDataFormat()).thenReturn(EXCEL_INTERNAL_DATE_FORMAT);

        final DataType dataType = reader.getCellDataType(cell);

        assertEquals(RecordFieldType.TIMESTAMP.getDataType(), dataType);
    }

    @Test
    void testInferCellFieldType() {
        final Map<String, FieldTypeInference> fieldTypes = new HashMap<>();

        reader.inferCellFieldType(cell, FIELD_NAME, fieldTypes);

        final FieldTypeInference fieldTypeInference = fieldTypes.get(FIELD_NAME);
        assertNotNull(fieldTypeInference);
    }
}
