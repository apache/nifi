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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.Files.newDirectoryStream;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestExcelHeaderSchemaStrategy {
    private static final TimeValueInference TIME_VALUE_INFERENCE = new TimeValueInference("MM/dd/yyyy", "HH:mm:ss.SSS", "yyyy/MM/dd/ HH:mm");

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

    @Test
    void testWhereConfiguredStartRowIsEmpty() throws IOException {
        Object[][] data = {{}, {1, "Manny"}, {2, "Moe"}, {3, "Jack"}};
        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            SchemaNotFoundException schemaNotFoundException = assertThrows(SchemaNotFoundException.class, () -> schemaStrategy.getSchema(null, inputStream, null));
            assertTrue(schemaNotFoundException.getMessage().contains("no cells with data"));
        }
    }

    @Test
    void testWhereConfiguredStartRowHasEmptyCell() throws Exception {
        Object[][] data = {{"ID", "", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            RecordSchema schema = schemaStrategy.getSchema(null, inputStream, null);
            RecordField recordField = schema.getField(1);
            assertEquals("column_1", recordField.getFieldName());
        }
    }

    @Test
    void testWhereInferenceRowHasMoreCellsThanFieldNames() throws Exception {
        Object[][] data = {{"ID", "First", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M", "Extra"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            SchemaNotFoundException schemaNotFoundException = assertThrows(SchemaNotFoundException.class, () -> schemaStrategy.getSchema(null, inputStream, null));
            assertTrue(schemaNotFoundException.getMessage().contains("more than"));
        }
    }

    @Test
    void testWhereTotalRowsLessThanConfiguredInferenceRows() throws Exception {
        Object[][] data = {{"ID", "First", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            assertDoesNotThrow(() -> schemaStrategy.getSchema(null, inputStream, null));
        }
    }

    @Test
    void testWhereConfiguredInferenceRowsHasAnEmptyRow() throws IOException {
        Object[][] data = {{"ID", "First", "Middle"}, {1, "One", "O"}, {2, "Two", "T"}, {3, "Three", "T"},
                {4, "Four", "F"}, {5, "Five", "F"}, {}, {7, "Seven", "S"}, {8, "Eight", "E"},
                {9, "Nine", "N"}, {10, "Ten", "T"}};

        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            assertDoesNotThrow(() -> schemaStrategy.getSchema(null, inputStream, null));
        }
    }

    @Test
    void testWhereTotalRowsGreaterThanConfiguredInferenceRows() throws Exception {
        Object[][] data = {{"ID", "First", "Middle"}, {1, "One", "O"}, {2, "Two", "T"}, {3, "Three", "T"},
                {4, "Four", "F"}, {5, "Five", "F"}, {6, "Six", "S"}, {7, "Seven", "S"}, {8, "Eight", "E"},
                {9, "Nine", "N"}, {10, "Ten", "T"}, {11, "Eleven", "E"}};

        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            assertDoesNotThrow(() -> schemaStrategy.getSchema(null, inputStream, null));
        }
    }

    @Test
    void testWhereConfiguredInferenceRowsAreAllBlank() throws IOException {
        Object[][] data = {{"ID", "First", "Middle"}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {11, "Eleven", "E"}};
        final ByteArrayOutputStream outputStream = getSingleSheetWorkbook(data);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        final ExcelHeaderSchemaStrategy schemaStrategy = new ExcelHeaderSchemaStrategy(context, logger, TIME_VALUE_INFERENCE);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            SchemaNotFoundException schemaNotFoundException = assertThrows(SchemaNotFoundException.class, () -> schemaStrategy.getSchema(null, inputStream, null));
            assertTrue(schemaNotFoundException.getMessage().contains("empty"));
        }
    }

    private static ByteArrayOutputStream getSingleSheetWorkbook(Object[][] data) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            final XSSFSheet sheet = workbook.createSheet("Sheet 1");
            int rowCount = 0;
            for (Object[] dataRow : data) {
                Row row = sheet.createRow(rowCount++);
                int columnCount = 0;
                for (Object field : dataRow) {
                    Cell cell = row.createCell(columnCount++);
                    if (field instanceof String) {
                        cell.setCellValue((String) field);
                    } else if (field instanceof Number) {
                        cell.setCellValue(((Number) field).doubleValue());
                    }
                }
            }
            workbook.write(outputStream);
        }

        return outputStream;
    }
}
