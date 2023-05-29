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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestExcelSchemaInference {
    private final TimeValueInference timestampInference = new TimeValueInference("MM/dd/yyyy", "HH:mm:ss.SSS", "yyyy/MM/dd/ HH:mm");

    @Mock
    ComponentLog logger;

    @ParameterizedTest
    @MethodSource("getLocales")
    public void testInferenceAgainstDifferentLocales(Locale locale) throws IOException {
        final File file = new File("src/test/resources/excel/numbers.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null);
        final RecordSchema schema;
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference, locale), logger);
            schema = accessStrategy.getSchema(null, inputStream, null);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(Collections.singletonList("0"), fieldNames);

            if (Locale.FRENCH.equals(locale)) {
                assertEquals(RecordFieldType.STRING, schema.getDataType("0").get().getFieldType());
            } else {
                assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.FLOAT.getDataType(), RecordFieldType.STRING.getDataType()),
                        schema.getDataType("0").get());
            }
        }
    }

    private static Stream<Arguments> getLocales() {
        Locale hindi = new Locale("hin");
        return Stream.of(
                Arguments.of(Locale.ENGLISH),
                Arguments.of(hindi),
                Arguments.of(Locale.JAPANESE),
                Arguments.of(Locale.FRENCH)
        );
    }

    @Test
    public void testInferenceIncludesAllRecords() throws IOException {
        final File file = new File("src/test/resources/excel/simpleDataFormatting.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null);

        final RecordSchema schema;
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference), Mockito.mock(ComponentLog.class));
            schema = accessStrategy.getSchema(null, inputStream, null);
        }

        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(Arrays.asList("0", "1", "2", "3"), fieldNames);
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()), schema.getDataType("0").get());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.TIMESTAMP.getDataType("yyyy/MM/dd/ HH:mm"), RecordFieldType.STRING.getDataType()),
                schema.getDataType("1").get());
        assertEquals(RecordFieldType.STRING, schema.getDataType("2").get().getFieldType());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.BOOLEAN.getDataType(),
                RecordFieldType.STRING.getDataType()), schema.getDataType("3").get());
    }

    @Test
    public void testInferenceIncludesAllRecordsWithEL() throws IOException {
        final File file = new File("src/test/resources/excel/simpleDataFormatting.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        properties.put(ExcelReader.REQUIRED_SHEETS, "${required.sheets}");
        properties.put(ExcelReader.STARTING_ROW, "${rows.to.skip}");
        final PropertyContext context = new MockConfigurationContext(properties, null);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("required.sheets", "Sheet1");
        attributes.put("rows.to.skip", "2");

        final RecordSchema schema;
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (variables, content) -> new ExcelRecordSource(content, context, variables, logger),
                    new ExcelSchemaInference(timestampInference), Mockito.mock(ComponentLog.class));
            schema = accessStrategy.getSchema(attributes, inputStream, null);
        }

        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(Arrays.asList("0", "1", "2", "3"), fieldNames);
        assertEquals(RecordFieldType.INT.getDataType(), schema.getDataType("0").get());
        assertEquals(RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.TIMESTAMP.getDataType("yyyy/MM/dd/ HH:mm"), RecordFieldType.STRING.getDataType()),
                schema.getDataType("1").get());
        assertEquals(RecordFieldType.STRING, schema.getDataType("2").get().getFieldType());
        assertEquals(RecordFieldType.BOOLEAN.getDataType(), schema.getDataType("3").get());
    }
}
