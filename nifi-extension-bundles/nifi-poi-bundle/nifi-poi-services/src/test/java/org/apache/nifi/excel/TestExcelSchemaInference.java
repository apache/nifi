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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestExcelSchemaInference {
    private static final String EXPECTED_FIRST_FIELD_NAME = ExcelSchemaInference.FIELD_NAME_PREFIX + "0";
    private static final String EXPECTED_SECOND_FIELD_NAME = ExcelSchemaInference.FIELD_NAME_PREFIX + "1";
    private static final String EXPECTED_THIRD_FIELD_NAME = ExcelSchemaInference.FIELD_NAME_PREFIX + "2";
    private static final String EXPECTED_FOURTH_FIELD_NAME = ExcelSchemaInference.FIELD_NAME_PREFIX + "3";
    private final TimeValueInference timestampInference = new TimeValueInference("MM/dd/yyyy", "HH:mm:ss.SSS", "yyyy/MM/dd/ HH:mm");

    @Mock
    ComponentLog logger;

    @ParameterizedTest
    @MethodSource("getLocales")
    public void testInferenceAgainstDifferentLocales(Locale locale) throws IOException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null, null);

        try (final InputStream inputStream = getResourceStream("/excel/numbers.xlsx")) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference, locale), logger);
            final RecordSchema schema = accessStrategy.getSchema(null, inputStream, null);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(Collections.singletonList(EXPECTED_FIRST_FIELD_NAME), fieldNames);

            if (Locale.FRENCH.equals(locale)) {
                assertEquals(RecordFieldType.STRING, schema.getDataType(EXPECTED_FIRST_FIELD_NAME).get().getFieldType());
            } else {
                assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.FLOAT.getDataType(), RecordFieldType.STRING.getDataType()),
                        schema.getDataType(EXPECTED_FIRST_FIELD_NAME).get());
            }
        }
    }

    private static Stream<Arguments> getLocales() {
        Locale hindi = Locale.of("hin");
        return Stream.of(
                Arguments.of(Locale.ENGLISH),
                Arguments.of(hindi),
                Arguments.of(Locale.JAPANESE),
                Arguments.of(Locale.FRENCH)
        );
    }

    @Test
    public void testInferenceIncludesAllRecords() throws IOException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null, null);

        final RecordSchema schema;
        try (final InputStream inputStream = getResourceStream("/excel/simpleDataFormatting.xlsx")) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference), Mockito.mock(ComponentLog.class));
            schema = accessStrategy.getSchema(null, inputStream, null);
        }

        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(Arrays.asList(EXPECTED_FIRST_FIELD_NAME, EXPECTED_SECOND_FIELD_NAME,
                EXPECTED_THIRD_FIELD_NAME, EXPECTED_FOURTH_FIELD_NAME), fieldNames);
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()), schema.getDataType(EXPECTED_FIRST_FIELD_NAME).get());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.TIMESTAMP.getDataType("yyyy/MM/dd/ HH:mm"), RecordFieldType.STRING.getDataType()),
                schema.getDataType(EXPECTED_SECOND_FIELD_NAME).get());
        assertEquals(RecordFieldType.STRING, schema.getDataType(EXPECTED_THIRD_FIELD_NAME).get().getFieldType());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.BOOLEAN.getDataType(),
                RecordFieldType.STRING.getDataType()), schema.getDataType(EXPECTED_FOURTH_FIELD_NAME).get());
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
        try (final InputStream inputStream = getResourceStream("/excel/simpleDataFormatting.xlsx")) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference), Mockito.mock(ComponentLog.class));
            schema = accessStrategy.getSchema(attributes, inputStream, null);
        }

        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(Arrays.asList(EXPECTED_FIRST_FIELD_NAME, EXPECTED_SECOND_FIELD_NAME,
                EXPECTED_THIRD_FIELD_NAME, EXPECTED_FOURTH_FIELD_NAME), fieldNames);
        assertEquals(RecordFieldType.INT.getDataType(), schema.getDataType(EXPECTED_FIRST_FIELD_NAME).get());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.TIMESTAMP.getDataType("yyyy/MM/dd/ HH:mm"), RecordFieldType.STRING.getDataType()),
                schema.getDataType(EXPECTED_SECOND_FIELD_NAME).get());
        assertEquals(RecordFieldType.STRING, schema.getDataType(EXPECTED_THIRD_FIELD_NAME).get().getFieldType());
        assertEquals(RecordFieldType.BOOLEAN.getDataType(), schema.getDataType(EXPECTED_FOURTH_FIELD_NAME).get());
    }

    private InputStream getResourceStream(final String relativePath) {
        final InputStream resourceStream = getClass().getResourceAsStream(relativePath);
        if (resourceStream == null) {
            throw new IllegalStateException(String.format("Resource [%s] not found", relativePath));
        }
        return resourceStream;
    }
}
