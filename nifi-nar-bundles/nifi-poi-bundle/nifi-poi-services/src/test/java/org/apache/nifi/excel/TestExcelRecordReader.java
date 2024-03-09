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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.openxml4j.exceptions.OpenXML4JRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestExcelRecordReader {

    private static final String DATA_FORMATTING_FILE = "dataformatting.xlsx";
    private static final String MULTI_SHEET_FILE = "twoSheets.xlsx";

    @Mock
    ComponentLog logger;

    @Test
    public void testNonExcelFile() {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(configuration, getInputStream("notExcel.txt"), logger));
        final Throwable cause = mre.getCause();
        assertInstanceOf(OpenXML4JRuntimeException.class, cause);
    }

    @Test
    public void testOlderExcelFormatFile() {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder().build();
        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(configuration, getInputStream("olderFormat.xls"), logger));
        assertTrue(ExceptionUtils.getStackTrace(mre).contains("data appears to be in the OLE2 Format"));
    }

    @Test
    public void testMultipleRecordsSingleSheet() throws MalformedRecordException {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(getDataFormattingSchema())
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(DATA_FORMATTING_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(9, records.size());
    }

    private RecordSchema getDataFormattingSchema() {
        final List<RecordField> fields = Arrays.asList(
                new RecordField("Numbers", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Timestamps", RecordFieldType.DATE.getDataType()),
                new RecordField("Money", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Flags", RecordFieldType.BOOLEAN.getDataType()));

        return new SimpleRecordSchema(fields);
    }

    private InputStream getInputStream(final String excelFile) {
        final String resourcePath = String.format("/excel/%s", excelFile);
        final InputStream resourceStream = getClass().getResourceAsStream(resourcePath);
        if (resourceStream == null) {
            throw new IllegalStateException(String.format("Resource [%s] not found", resourcePath));
        }
        return resourceStream;
    }

    private List<Record> getRecords(ExcelRecordReader recordReader, boolean coerceTypes, boolean dropUnknownFields) throws MalformedRecordException {
        Record record;
        List<Record> records = new ArrayList<>();
        while ((record = recordReader.nextRecord(coerceTypes, dropUnknownFields)) != null) {
            records.add(record);
        }

        return records;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDropUnknownFields(boolean dropUnknownFields) throws MalformedRecordException {
        final List<RecordField> fields = Arrays.asList(
                new RecordField("Numbers", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Timestamps", RecordFieldType.DATE.getDataType()));

        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(new SimpleRecordSchema(fields))
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(DATA_FORMATTING_FILE), logger);
        List<Record> records = getRecords(recordReader, false, dropUnknownFields);

        assertEquals(9, records.size());
        if(dropUnknownFields) {
            records.forEach(record -> assertEquals(fields.size(), record.getRawFieldNames().size()));
        } else {
            records.forEach(record -> {
                int rawNumFields = record.getRawFieldNames().size();
                assertTrue(rawNumFields >= 2 && rawNumFields <= 4);
            });
        }
    }

    @Test
    public void testSkipLines() throws MalformedRecordException {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withFirstRow(5)
                .withSchema(getDataFormattingSchema())
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(DATA_FORMATTING_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(4, records.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void tesCoerceTypes(boolean coerceTypes) throws MalformedRecordException {
        String fieldName = "dates";
        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(new RecordField(fieldName, RecordFieldType.TIMESTAMP.getDataType())));
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withDateFormat("MM/dd/yyyy")
                .withTimeFormat(RecordFieldType.TIME.getDefaultFormat())
                .withTimestampFormat(RecordFieldType.TIMESTAMP.getDefaultFormat())
                .withSchema(schema)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream("dates.xlsx"), logger);
        List<Record> records = getRecords(recordReader, coerceTypes, false);

        assertEquals(6, records.size());
        records.forEach(record -> assertInstanceOf(Timestamp.class, record.getValue(fieldName)));
    }

    @Test
    public void testSelectSpecificSheet() throws MalformedRecordException {
        RecordSchema schema = getSpecificSheetSchema();
        List<String> requiredSheets = Collections.singletonList("TestSheetA");
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withFirstRow(1)
                .withRequiredSheets(requiredSheets)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(3, records.size());
    }

    private RecordSchema getSpecificSheetSchema() {
        return  new SimpleRecordSchema(Arrays.asList(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.STRING.getDataType()),
                new RecordField("third", RecordFieldType.STRING.getDataType())));
    }

    @Test
    public void testSelectSpecificSheetNotFound() {
        RecordSchema schema = getSpecificSheetSchema();
        List<String> requiredSheets = Collections.singletonList("notExistingSheet");
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withFirstRow(1)
                .withRequiredSheets(requiredSheets)
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class,
                () -> new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger));
        assertInstanceOf(ProcessException.class, mre.getCause());
        assertTrue(mre.getCause().getMessage().startsWith("Required Excel Sheets not found"));
    }

    @Test
    public void testSelectAllSheets() throws MalformedRecordException {
        RecordSchema schema = new SimpleRecordSchema(Arrays.asList(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.STRING.getDataType())));
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(7, records.size());
    }
}
