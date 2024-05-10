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
import com.github.pjfanning.xlsx.exceptions.ReadException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
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
    private static final String PASSWORD = "nifi";
    private static final ByteArrayOutputStream PASSWORD_PROTECTED = new ByteArrayOutputStream();
    private static final Object[][] DATA = {
            {"ID", "Name"},
            {1, "Manny"},
            {2, "Moe"},
            {3, "Jack"},
    };

    @Mock
    ComponentLog logger;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        //Generate an Excel file and populate it with data
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            final XSSFSheet sheet = workbook.createSheet("User Info");
            populateSheet(sheet);
            workbook.write(outputStream);
        }

        //Protect the Excel file with a password
        try (POIFSFileSystem poifsFileSystem = new POIFSFileSystem()) {
            EncryptionInfo encryptionInfo = new EncryptionInfo(EncryptionMode.agile);
            Encryptor encryptor = encryptionInfo.getEncryptor();
            encryptor.confirmPassword(PASSWORD);

            try (OPCPackage opc = OPCPackage.open(new ByteArrayInputStream(outputStream.toByteArray()));
                 OutputStream os = encryptor.getDataStream(poifsFileSystem)) {
                opc.save(os);
            }
            poifsFileSystem.writeFilesystem(PASSWORD_PROTECTED);
        }
    }

    private static void populateSheet(XSSFSheet sheet) {
        //Adding the data to the Excel worksheet
        int rowCount = 0;
        for (Object[] dataRow : DATA) {
            Row row = sheet.createRow(rowCount++);
            int columnCount = 0;

            for (Object field : dataRow) {
                Cell cell = row.createCell(columnCount++);
                if (field instanceof String) {
                    cell.setCellValue((String) field);
                } else if (field instanceof Integer) {
                    cell.setCellValue((Integer) field);
                }
            }
        }
    }

    @Test
    public void testNonExcelFile() {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(configuration, getInputStream("notExcel.txt"), logger));
        final Throwable cause = mre.getCause();
        assertInstanceOf(ReadException.class, cause);
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

    @Test
    void testPasswordProtected() throws Exception {
        RecordSchema schema = getPasswordProtectedSchema();
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withPassword(PASSWORD)
                .withAvoidTempFiles(true)
                .build();

        InputStream inputStream = new ByteArrayInputStream(PASSWORD_PROTECTED.toByteArray());
        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, inputStream, logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(DATA.length, records.size());
    }

    @Test
    void testPasswordProtectedWithoutPassword() {
        RecordSchema schema = getPasswordProtectedSchema();
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .build();

        InputStream inputStream = new ByteArrayInputStream(PASSWORD_PROTECTED.toByteArray());
        assertThrows(Exception.class, () -> new ExcelRecordReader(configuration, inputStream, logger));
    }

    private RecordSchema getPasswordProtectedSchema() {
        return new SimpleRecordSchema(Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())));
    }
}
