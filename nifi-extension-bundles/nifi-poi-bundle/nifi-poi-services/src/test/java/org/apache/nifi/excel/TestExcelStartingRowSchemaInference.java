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
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.newDirectoryStream;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestExcelStartingRowSchemaInference {
    private static final TimeValueInference TIME_VALUE_INFERENCE = new TimeValueInference("MM/dd/yyyy", "HH:mm:ss.SSS", "yyyy/MM/dd/ HH:mm");

    @Mock
    private ComponentLog logger;

    private PropertyContext context;

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

    @BeforeEach
    void setUp() {
        final Map<PropertyDescriptor, String> properties = Map.of();
        context = new MockConfigurationContext(properties, null, null);
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testWhereConfiguredStartRowIsEmpty(RowEvaluationStrategy rowEvaluationStrategy) throws IOException {
        final Object[][] singleSheet = {{}, {1, "Manny"}, {2, "Moe"}, {3, "Jack"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);
            final IOException ioException = assertThrows(IOException.class, () -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
            assertInstanceOf(SchemaNotFoundException.class, ioException.getCause());
            assertTrue(ioException.getCause().getMessage().contains("Field names could not be determined from configured header row"));
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testWhereConfiguredStartRowHasEmptyCell(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        final Object[][] singleSheet = {{"ID", "", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);
            RecordSchema schema = inferSchemaAccessStrategy.getSchema(null, inputStream, null);
            assertEquals(List.of("ID", "column_1", "Middle"), schema.getFieldNames());
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testWhereInferenceRowHasMoreCellsThanFieldNames(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        final Object[][] singleSheet = {{"ID", "First", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M", "Extra"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);
            final IOException ioException = assertThrows(IOException.class, () -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
            assertInstanceOf(SchemaNotFoundException.class, ioException.getCause());
            assertTrue(ioException.getCause().getMessage().contains("more than"));
        }
    }

    @Test
    void testWhereTotalRowsLessThanConfiguredInferenceRows() throws Exception {
        final Object[][] singleSheet = {{"ID", "First", "Middle"}, {1, "Manny", "M"}, {2, "Moe", "M"}, {3, "Jack", "J"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(RowEvaluationStrategy.STANDARD);
            assertDoesNotThrow(() -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
        }
    }

    @Test
    void testWhereConfiguredInferenceRowsHasAnEmptyRow() throws Exception {
        final Object[][] singleSheet = {{"ID", "First", "Middle"}, {1, "One", "O"}, {2, "Two", "T"}, {3, "Three", "T"},
                {4, "Four", "F"}, {5, "Five", "F"}, {}, {7, "Seven", "S"}, {8, "Eight", "E"},
                {9, "Nine", "N"}, {10, "Ten", "T"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(RowEvaluationStrategy.STANDARD);
            assertDoesNotThrow(() -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
        }
    }

    @Test
    void testWhereTotalRowsGreaterThanConfiguredInferenceRows() throws Exception {
        Object[][] singleSheet = {{"ID", "First", "Middle"}, {1, "One", "O"}, {2, "Two", "T"}, {3, "Three", "T"},
                {4, "Four", "F"}, {5, "Five", "F"}, {6, "Six", "S"}, {7, "Seven", "S"}, {8, "Eight", "E"},
                {9, "Nine", "N"}, {10, "Ten", "T"}, {11, "Eleven", "E"}};

        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(RowEvaluationStrategy.STANDARD);
            assertDoesNotThrow(() -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testWhereConfiguredInferenceRowsAreAllBlank(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        Object[][] singleSheet = {{"ID", "First", "Middle"}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {11, "Eleven", "E"}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);

            switch (rowEvaluationStrategy) {
                case STANDARD -> {
                    final IOException ioException = assertThrows(IOException.class, () -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
                    assertInstanceOf(SchemaNotFoundException.class, ioException.getCause());
                    assertTrue(ioException.getCause().getMessage().contains("empty"));
                }
                case ALL -> assertDoesNotThrow(() -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testWhereRowsAreAllBlank(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        Object[][] singleSheet = {{"ID", "First", "Middle"}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}};
        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);

            final IOException ioException = assertThrows(IOException.class, () -> inferSchemaAccessStrategy.getSchema(null, inputStream, null));
            assertInstanceOf(SchemaNotFoundException.class, ioException.getCause());
            assertTrue(ioException.getCause().getMessage().contains("empty"));
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testAlignedDateColumnsAcrossTwoSheets(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        final String dateColumnName = "Date";
        final Object[] columnNames = {dateColumnName, "Something", "Name"};
        final Object[][] firstSheet =
            {columnNames, {LocalDate.of(2025, 2, 1), "test1", "Sheet1"}, {LocalDate.of(2024, 2, 12), "test2", "Sheet1"}};
        Object[][] secondSheet =
            {columnNames, {LocalDate.of(1976, 9, 11), "test1", "Sheet2"}, {LocalDate.of(1987, 2, 12), "test2", "Sheet2"}};
        final ByteArrayOutputStream outputStream = createWorkbook(firstSheet, secondSheet);

        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);

            final RecordSchema schema = inferSchemaAccessStrategy.getSchema(null, inputStream, null);
            final RecordField dateRecordField = schema.getField(dateColumnName).orElse(null);

            assertNotNull(dateRecordField);
            assertEquals(RecordFieldType.DATE, dateRecordField.getDataType().getFieldType(), String.format("Expected record field type to be %s but it was type %s",
                    RecordFieldType.DATE, dateRecordField.getDataType().getFieldType()));
        }
    }

    @ParameterizedTest
    @EnumSource(RowEvaluationStrategy.class)
    void testDuplicateColumnNames(RowEvaluationStrategy rowEvaluationStrategy) throws Exception {
        Object[][] singleSheet = {{"Frequency", "Intervals", "Frequency", "Name", "Frequency", "Intervals"},
                {6, "0-9", 13, "John", 15, 2}, {4, "10-19", 15, "Sue", 13, 3}};

        final ByteArrayOutputStream outputStream = createWorkbook(singleSheet);
        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> inferSchemaAccessStrategy = getInferSchemaAccessStrategy(rowEvaluationStrategy);

            final RecordSchema schema = inferSchemaAccessStrategy.getSchema(null, inputStream, null);
            assertEquals(6, schema.getFieldNames().size());
            assertEquals(List.of("Frequency", "Intervals", "Frequency_1", "Name", "Frequency_2", "Intervals_1"), schema.getFieldNames());
        }
    }

    private InferSchemaAccessStrategy<?> getInferSchemaAccessStrategy(RowEvaluationStrategy rowEvaluationStrategy) {
        return new InferSchemaAccessStrategy<>(
                (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                new ExcelStartingRowSchemaInference(rowEvaluationStrategy, 1, TIME_VALUE_INFERENCE), logger);
    }

    private static ByteArrayOutputStream createWorkbook(Object[][]... sheetData) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            CreationHelper creationHelper = workbook.getCreationHelper();
            CellStyle dayMonthYearCellStyle = workbook.createCellStyle();
            dayMonthYearCellStyle.setDataFormat(creationHelper.createDataFormat().getFormat("dd/mm/yyyy"));
            int sheetCount = 1;

            for (Object[][] singleSheet : sheetData) {
                final XSSFSheet sheet = workbook.createSheet("Sheet " + sheetCount);
                int rowCount = 0;

                for (Object[] singleRow : singleSheet) {
                    Row row = sheet.createRow(rowCount++);
                    int columnCount = 0;

                    for (Object field : singleRow) {
                        Cell cell = row.createCell(columnCount++);
                        switch (field) {
                            case String string -> cell.setCellValue(string);
                            case Number number -> cell.setCellValue(number.doubleValue());
                            case LocalDate localDate -> {
                                cell.setCellValue(localDate);
                                cell.setCellStyle(dayMonthYearCellStyle);
                            }
                            default -> throw new IllegalStateException("Unexpected value: " + field);
                        }
                    }
                }
                sheetCount++;
            }
            workbook.write(outputStream);
        }

        return outputStream;
    }
}
