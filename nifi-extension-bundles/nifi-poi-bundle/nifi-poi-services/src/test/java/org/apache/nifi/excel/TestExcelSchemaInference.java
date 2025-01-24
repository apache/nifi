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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.nio.file.Files.newDirectoryStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestExcelSchemaInference {
    private static final String EXPECTED_FIRST_FIELD_NAME = ExcelUtils.FIELD_NAME_PREFIX + "0";
    private static final String EXPECTED_SECOND_FIELD_NAME = ExcelUtils.FIELD_NAME_PREFIX + "1";
    private static final String EXPECTED_THIRD_FIELD_NAME = ExcelUtils.FIELD_NAME_PREFIX + "2";
    private static final String EXPECTED_FOURTH_FIELD_NAME = ExcelUtils.FIELD_NAME_PREFIX + "3";

    private static final String SIMPLE_FORMATTING_PATH = "/excel/simpleDataFormatting.xlsx";

    @Mock
    private ComponentLog logger;

    @Mock
    private TimeValueInference timeValueInference;

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
    public void testInferenceIncludesAllRecords() throws IOException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null, null);

        final RecordSchema schema;
        try (final InputStream inputStream = getResourceStream()) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timeValueInference), logger);
            schema = accessStrategy.getSchema(null, inputStream, null);
        }

        assertFieldNamesFound(schema);
        assertFieldDataTypeEquals(schema, EXPECTED_FIRST_FIELD_NAME,
                RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.LONG.getDataType(),
                        RecordFieldType.STRING.getDataType()
                )
        );
        assertFieldDataTypeEquals(schema, EXPECTED_SECOND_FIELD_NAME,
                RecordFieldType.CHOICE.getChoiceDataType(
                        // Assert Timestamp Data Type with standard Date and Time Pattern
                        RecordFieldType.TIMESTAMP.getDataType(),
                        RecordFieldType.STRING.getDataType()
                )
        );
        assertFieldDataTypeEquals(schema, EXPECTED_THIRD_FIELD_NAME,
                RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.DOUBLE.getDataType(),
                        RecordFieldType.STRING.getDataType()
                )
        );
        assertFieldDataTypeEquals(schema, EXPECTED_FOURTH_FIELD_NAME,
                RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.BOOLEAN.getDataType(),
                        RecordFieldType.STRING.getDataType()
                )
        );
    }

    @Test
    public void testInferenceIncludesAllRecordsWithEL() throws IOException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        properties.put(ExcelReader.REQUIRED_SHEETS, "${required.sheets}");
        properties.put(ExcelReader.STARTING_ROW, "${rows.to.skip}");
        final PropertyContext context = new MockConfigurationContext(properties, null, null);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("required.sheets", "Sheet1");
        attributes.put("rows.to.skip", "2");

        final RecordSchema schema;
        try (final InputStream inputStream = getResourceStream()) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timeValueInference), logger);
            schema = accessStrategy.getSchema(attributes, inputStream, null);
        }

        assertFieldNamesFound(schema);

        assertFieldDataTypeEquals(schema, EXPECTED_FIRST_FIELD_NAME, RecordFieldType.LONG.getDataType());
        assertFieldDataTypeEquals(schema, EXPECTED_SECOND_FIELD_NAME, RecordFieldType.TIMESTAMP.getDataType());
        assertFieldDataTypeEquals(schema, EXPECTED_THIRD_FIELD_NAME, RecordFieldType.DOUBLE.getDataType());
        assertFieldDataTypeEquals(schema, EXPECTED_FOURTH_FIELD_NAME, RecordFieldType.BOOLEAN.getDataType());
    }

    @Test
    public void testSchemaInferenceTimestampString() throws IOException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null, null);

        final String timestampCellValue = "2020-01-01 12:30:45";

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            final XSSFSheet sheet = workbook.createSheet(TestExcelSchemaInference.class.getSimpleName());

            final XSSFRow row = sheet.createRow(1);
            final XSSFCell cell = row.createCell(0, CellType.STRING);
            cell.setCellValue(timestampCellValue);

            workbook.write(outputStream);
        }

        final DataType timestampDataType = RecordFieldType.TIMESTAMP.getDataType();
        final String timestampFormat = timestampDataType.getFormat();
        final TimeValueInference timestampValueInference = new TimeValueInference(RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), timestampFormat);

        final RecordSchema schema;
        try (final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampValueInference), logger);
            schema = accessStrategy.getSchema(null, inputStream, null);
        }

        assertEquals(1, schema.getFieldCount());

        final RecordField firstField = schema.getField(0);
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(), firstField.getDataType());
    }

    private InputStream getResourceStream() {
        final InputStream resourceStream = getClass().getResourceAsStream(SIMPLE_FORMATTING_PATH);
        if (resourceStream == null) {
            throw new IllegalStateException(String.format("Resource [%s] not found", SIMPLE_FORMATTING_PATH));
        }
        return resourceStream;
    }

    private void assertFieldDataTypeEquals(final RecordSchema schema, final String fieldName, final DataType expectedDataType) {
        final DataType fieldDataType = schema.getDataType(fieldName).orElse(null);
        assertEquals(expectedDataType, fieldDataType);
    }

    private void assertFieldNamesFound(final RecordSchema schema) {
        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(
                Arrays.asList(
                        EXPECTED_FIRST_FIELD_NAME,
                        EXPECTED_SECOND_FIELD_NAME,
                        EXPECTED_THIRD_FIELD_NAME,
                        EXPECTED_FOURTH_FIELD_NAME
                ),
                fieldNames
        );
    }
}
