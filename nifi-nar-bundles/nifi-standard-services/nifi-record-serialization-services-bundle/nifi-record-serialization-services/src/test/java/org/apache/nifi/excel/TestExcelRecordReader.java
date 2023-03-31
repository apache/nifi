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
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExcelRecordReader {

    private static final String DATA_FORMATTING_FILE = "dataformatting.xlsx";
    private static final String MULTI_SHEET_FILE = "twoSheets.xlsx";

    @Test
    public void testNonExcelFile() throws IOException {
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream("notExcel.txt"))
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(args));
        assertTrue(ExceptionUtils.getStackTrace(mre).contains("this is not a valid OOXML"));
    }

    @Test
    public void testOlderExcelFormatFile() throws IOException {
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream("olderFormat.xls"))
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(args));
        assertTrue(ExceptionUtils.getStackTrace(mre).contains("data appears to be in the OLE2 Format"));
    }
    @Test
    public void testMultipleRecordsSingleSheet() throws IOException, MalformedRecordException {
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream(DATA_FORMATTING_FILE))
                .withSchema(getDataFormattingSchema())
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, false, true);

        assertEquals(9, records.size());
    }

    private RecordSchema getDataFormattingSchema() {
        final List<RecordField> fields = List.of(
                new RecordField("Numbers", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Timestamps", RecordFieldType.DATE.getDataType()),
                new RecordField("Money", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Flags", RecordFieldType.BOOLEAN.getDataType()));

        return new SimpleRecordSchema(fields);
    }

    private InputStream getInputStream(String excelFile) throws IOException {
        String excelResourcesDir = "src/test/resources/excel";
        Path excelDoc = Paths.get(excelResourcesDir, excelFile);
        return Files.newInputStream(excelDoc);
    }

    private ExcelRecordReader getRecordReader(ExcelRecordReaderArgs args) throws MalformedRecordException {
        ExcelRecordReader recordReader = new ExcelRecordReader(args);
        recordReader.setLogger(new MockComponentLog(null, recordReader));

        return recordReader;
    }

    private List<Record> getRecords(ExcelRecordReader recordReader, boolean coerceTypes, boolean dropUnknownFields, boolean print) throws IOException, MalformedRecordException {
        Record record;
        List<Record> records = new ArrayList<>();
        while ((record = recordReader.nextRecord(coerceTypes, dropUnknownFields)) != null) {
            if (print) {
                System.out.println(record.toMap());
            }
            records.add(record);
        }

        return records;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDropUnknownFields(boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final List<RecordField> fields = List.of(
                new RecordField("Numbers", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Timestamps", RecordFieldType.DATE.getDataType()));

        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream(DATA_FORMATTING_FILE))
                .withSchema(new SimpleRecordSchema(fields))
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, dropUnknownFields, true);

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
    public void testSkipLines() throws IOException, MalformedRecordException {
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withFirstRow(5)
                .withInputStream(getInputStream(DATA_FORMATTING_FILE))
                .withSchema(getDataFormattingSchema())
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, false, false);

        assertEquals(4, records.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void tesCoerceTypes(boolean coerceTypes) throws IOException, MalformedRecordException {
        String fieldName = "dates";
        RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField(fieldName, RecordFieldType.TIMESTAMP.getDataType())));
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withDateFormat("MM/dd/yyyy")
                .withTimeFormat(RecordFieldType.TIME.getDefaultFormat())
                .withTimestampFormat(RecordFieldType.TIMESTAMP.getDefaultFormat())
                .withInputStream(getInputStream("dates.xlsx"))
                .withSchema(schema)
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, coerceTypes, false, false);

        assertEquals(6, records.size());
        records.forEach(record -> assertInstanceOf(Timestamp.class, record.getValue(fieldName)));
    }

    @Test
    public void testSelectSpecificSheet() throws IOException, MalformedRecordException {
        RecordSchema schema = getSpecificSheetSchema();
        AtomicReferenceArray<String> desiredSheets = new AtomicReferenceArray<>(1);
        desiredSheets.set(0, "TestSheetA");
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream(MULTI_SHEET_FILE))
                .withSchema(schema)
                .withFirstRow(1)
                .withDesiredSheets(desiredSheets)
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, false, false);

        assertEquals(3, records.size());
    }

    private RecordSchema getSpecificSheetSchema() {
        return  new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.STRING.getDataType()),
                new RecordField("third", RecordFieldType.STRING.getDataType())));
    }
    @Test
    public void testSelectSpecificSheetNotFound() throws IOException, MalformedRecordException {
        RecordSchema schema = getSpecificSheetSchema();
        AtomicReferenceArray<String> desiredSheets = new AtomicReferenceArray<>(1);
        desiredSheets.set(0, "notExistingSheet");
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream(MULTI_SHEET_FILE))
                .withSchema(schema)
                .withFirstRow(1)
                .withDesiredSheets(desiredSheets)
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, false, false);

        assertEquals(0, records.size());
    }

    @Test
    public void testSelectAllSheets() throws IOException, MalformedRecordException {
        RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.STRING.getDataType())));
        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withInputStream(getInputStream(MULTI_SHEET_FILE))
                .withSchema(schema)
                .withLogger(Mockito.mock(ComponentLog.class))
                .build();

        ExcelRecordReader recordReader = getRecordReader(args);
        List<Record> records = getRecords(recordReader, false, false, true);


        assertEquals(7, records.size());
    }
}
