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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.nio.file.Files.newDirectoryStream;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    private static final ByteArrayOutputStream PASSWORD_PROTECTED_OLDER_EXCEL = new ByteArrayOutputStream();
    private static final Object[][] DATA = {
            {"ID", "Name"},
            {1, "Manny"},
            {2, "Moe"},
            {3, "Jack"},
    };

    @Mock
    ComponentLog logger;

    /*
     * Cleanup the temporary poifiles directory which is created by org.apache.poi.util.DefaultTempFileCreationStrategy
     * the strategy org.apache.poi.util.TempFile uses which in turn is used by com.github.pjfanning.xlsx.impl.StreamingSheetReader.
     */
    @AfterAll
    public static void cleanUpAfterAll() {
        final Path tempDir = Path.of(System.getProperty("java.io.tmpdir")).resolve("poifiles");
        try (DirectoryStream<Path> directoryStream = newDirectoryStream(tempDir, "tmp-[0-9]*.xlsx")) {
            for (Path tmpFile : directoryStream) {
                Files.deleteIfExists(tmpFile);
            }
        } catch (Exception ignored) {
        }
    }

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        createPasswordProtectedWorkbook();
        createPasswordProtectedOlderExcelWorkbook();
    }

    @Test
    public void testNonExcelFile() {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> new ExcelRecordReader(configuration, getInputStream("notExcel.txt"), logger));
        final Throwable cause = mre.getCause();
        assertInstanceOf(ProcessException.class, cause);
    }

    @Test
    public void testOlderExcelFormatFile() throws MalformedRecordException {
        final List<RecordField> fields = List.of(
                new RecordField("A", RecordFieldType.STRING.getDataType()),
                new RecordField("B", RecordFieldType.STRING.getDataType()),
                new RecordField("C", RecordFieldType.STRING.getDataType()));

        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(new SimpleRecordSchema(fields))
                .withInputFileType(InputFileType.XLS)
                .build();

        final ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream("olderFormat.xls"), logger);
        final List<Record> records = getRecords(recordReader, false, false);

        assertEquals(4, records.size());
    }

    @Test
    public void testMultipleRecordsSingleSheet() throws MalformedRecordException {
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(getDataFormattingSchema())
                .withInputFileType(InputFileType.XLSX)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(DATA_FORMATTING_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

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
        final List<RecordField> fields = List.of(
                new RecordField("Numbers", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("Timestamps", RecordFieldType.DATE.getDataType()));

        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(new SimpleRecordSchema(fields))
                .withInputFileType(InputFileType.XLSX)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(DATA_FORMATTING_FILE), logger);
        List<Record> records = getRecords(recordReader, false, dropUnknownFields);

        assertEquals(9, records.size());
        if (dropUnknownFields) {
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
                .withInputFileType(InputFileType.XLSX)
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
                .withInputFileType(InputFileType.XLSX)
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
                .withInputFileType(InputFileType.XLSX)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(3, records.size());
    }

    private RecordSchema getSpecificSheetSchema() {
        return  new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
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
                .withInputFileType(InputFileType.XLSX)
                .build();

        MalformedRecordException mre = assertThrows(MalformedRecordException.class,
                () -> new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger));
        assertInstanceOf(ProcessException.class, mre.getCause());
        assertTrue(mre.getCause().getMessage().startsWith("Required Excel Sheets not found"));
    }

    @Test
    public void testSelectAllSheets() throws MalformedRecordException {
        RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.STRING.getDataType())));
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withInputFileType(InputFileType.XLSX)
                .build();

        ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger);
        List<Record> records = getRecords(recordReader, false, false);

        assertEquals(7, records.size());
    }

    @Test
    void testWhereCellValueDoesNotMatchSchemaType()  {
        RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.FLOAT.getDataType())));
        List<String> requiredSheets = Collections.singletonList("TestSheetA");
        ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withFirstRow(2)
                .withRequiredSheets(requiredSheets)
                .withInputFileType(InputFileType.XLSX)
                .build();

        final MalformedRecordException mre = assertThrows(MalformedRecordException.class, () ->  {
            ExcelRecordReader recordReader = new ExcelRecordReader(configuration, getInputStream(MULTI_SHEET_FILE), logger);
            getRecords(recordReader, true, false);
        });

        assertInstanceOf(NumberFormatException.class, mre.getCause());
        assertTrue(mre.getMessage().contains("on row") && mre.getMessage().contains("in sheet"));
    }

    @ParameterizedTest
    @EnumSource(InputFileType.class)
    void testPasswordProtected(InputFileType inputFileType) throws Exception {
        final RecordSchema schema = getPasswordProtectedSchema();
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withPassword(PASSWORD)
                .withAvoidTempFiles(true)
                .withInputFileType(inputFileType)
                .build();

        final ByteArrayOutputStream fileStream = inputFileType == InputFileType.XLSX ? PASSWORD_PROTECTED : PASSWORD_PROTECTED_OLDER_EXCEL;
        final InputStream inputStream = new ByteArrayInputStream(fileStream.toByteArray());
        final ExcelRecordReader recordReader = new ExcelRecordReader(configuration, inputStream, logger);
        final List<Record> records = getRecords(recordReader, false, false);

        assertEquals(DATA.length, records.size());
    }

    @ParameterizedTest
    @EnumSource(InputFileType.class)
    void testPasswordProtectedWithoutPassword(InputFileType inputFileType) {
        final RecordSchema schema = getPasswordProtectedSchema();
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withInputFileType(inputFileType)
                .build();

        final ByteArrayOutputStream fileStream = inputFileType == InputFileType.XLSX ? PASSWORD_PROTECTED : PASSWORD_PROTECTED_OLDER_EXCEL;
        final InputStream inputStream = new ByteArrayInputStream(fileStream.toByteArray());
        assertThrows(Exception.class, () -> new ExcelRecordReader(configuration, inputStream, logger));
    }

    private RecordSchema getPasswordProtectedSchema() {
        return new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())));
    }

    @Test
    void testWithNumberColumnWhoseValueIsEmptyString() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("first", RecordFieldType.STRING.getDataType()),
                new RecordField("second", RecordFieldType.LONG.getDataType())));
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withInputFileType(InputFileType.XLSX)
                .build();

        final Object[][] data = {{"Manny", ""}};
        final InputStream workbook = createWorkbook(data, XSSFWorkbook::new);
        final ExcelRecordReader recordReader = new ExcelRecordReader(configuration, workbook, logger);

        assertDoesNotThrow(() -> getRecords(recordReader, true, true));
    }

    @Test
    void testWhereLongSpecifiedInSchemaAsString() throws Exception {
        final String fieldName = "Phone";
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField(fieldName, RecordFieldType.STRING.getDataType())));
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withSchema(schema)
                .withInputFileType(InputFileType.XLSX)
                .build();
        final Object[][] data = {{9876543210L}};
        final InputStream workbook = createWorkbook(data, XSSFWorkbook::new);
        final ExcelRecordReader recordReader = new ExcelRecordReader(configuration, workbook, logger);
        final List<Record> records = getRecords(recordReader, true, true);
        final Record firstRecord = records.getFirst();
        final String scientificNotationNumber = "9.87654321E9";

        assertEquals(scientificNotationNumber, firstRecord.getAsString(fieldName));
    }

    private static InputStream createWorkbook(Object[][] data, Supplier<Workbook> workbookSupplier) throws Exception {
        final ByteArrayOutputStream workbookOutputStream = new ByteArrayOutputStream();
        try (Workbook workbook = workbookSupplier.get()) {
            final Sheet sheet = workbook.createSheet("SomeSheetName");
            populateSheet(sheet, data);
            workbook.write(workbookOutputStream);
        }

        return new ByteArrayInputStream(workbookOutputStream.toByteArray());
    }

    private static void createPasswordProtectedWorkbook() throws Exception {
        // Generate an Excel file and populate it with data
        final InputStream workbook = createWorkbook(DATA, XSSFWorkbook::new);

        // Protect the Excel file with a password
        try (POIFSFileSystem poifsFileSystem = new POIFSFileSystem()) {
            EncryptionInfo encryptionInfo = new EncryptionInfo(EncryptionMode.agile);
            Encryptor encryptor = encryptionInfo.getEncryptor();
            encryptor.confirmPassword(PASSWORD);

            try (OPCPackage opc = OPCPackage.open(workbook);
                 OutputStream os = encryptor.getDataStream(poifsFileSystem)) {
                opc.save(os);
            }
            poifsFileSystem.writeFilesystem(PASSWORD_PROTECTED);
        }
    }

    private static void createPasswordProtectedOlderExcelWorkbook() throws Exception {
        Biff8EncryptionKey.setCurrentUserPassword(PASSWORD);
        try (final InputStream inputStream = createWorkbook(DATA, HSSFWorkbook::new)) {
            inputStream.transferTo(PASSWORD_PROTECTED_OLDER_EXCEL);
        } finally {
            Biff8EncryptionKey.setCurrentUserPassword(null);
        }
    }

    private static void populateSheet(Sheet sheet, Object[][] data) {
        //Adding the data to the Excel worksheet
        int rowCount = 0;
        for (Object[] dataRow : data) {
            Row row = sheet.createRow(rowCount++);
            int columnCount = 0;

            for (Object field : dataRow) {
                Cell cell = row.createCell(columnCount++);
                switch (field) {
                    case String string -> cell.setCellValue(string);
                    case Integer integer -> cell.setCellValue(integer.doubleValue());
                    case Long l -> cell.setCellValue(l.doubleValue());
                    default -> { }
                }
            }
        }
    }
}
