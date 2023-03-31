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
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestExcelHeaderSchemaStrategy {
    @ParameterizedTest
    @MethodSource("getLocales")
    public void testInferenceAgainstDifferentLocales(Locale locale) throws IOException, SchemaNotFoundException {
        final File file = new File("src/test/resources/excel/numbers.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null);
        ExcelHeaderSchemaStrategy headerSchemaStrategy = new ExcelHeaderSchemaStrategy(context, locale);
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            RecordSchema schema = headerSchemaStrategy.getSchema(null, inputStream, null);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(List.of("0"), fieldNames);
            if (Locale.FRENCH.equals(locale)) {
                assertEquals(RecordFieldType.STRING, schema.getDataType("0").get().getFieldType());
            } else {
                assertEquals(RecordFieldType.FLOAT, schema.getDataType("0").get().getFieldType());
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
    public void testColumnHeaders() throws IOException, SchemaNotFoundException {
        final File file = new File("src/test/resources/excel/simpleDataFormatting.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null);

        ExcelHeaderSchemaStrategy headerSchemaStrategy = new ExcelHeaderSchemaStrategy(context);
        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            RecordSchema schema = headerSchemaStrategy.getSchema(null, inputStream, null);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(Arrays.asList("0", "1", "2", "3"), fieldNames);
            assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("0").get());
            assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("1").get());
            assertEquals(RecordFieldType.STRING, schema.getDataType("2").get().getFieldType());
            assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("3").get());
        }
    }
    @Test
    public void testAfterColumnHeaders() throws IOException, SchemaNotFoundException{
        final File file = new File("src/test/resources/excel/simpleDataFormatting.xlsx");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
        final PropertyContext context = new MockConfigurationContext(properties, null);
        properties.put(ExcelReader.FIRST_ROW_NUM, "2");
        ExcelHeaderSchemaStrategy headerSchemaStrategy = new ExcelHeaderSchemaStrategy(context);

        try (final InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            RecordSchema schema = headerSchemaStrategy.getSchema(null, inputStream, null);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(Arrays.asList("0", "1", "2", "3"), fieldNames);
            assertEquals(RecordFieldType.INT.getDataType(), schema.getDataType("0").get());
            assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("1").get());
            assertEquals(RecordFieldType.STRING, schema.getDataType("2").get().getFieldType());
            assertEquals(RecordFieldType.BOOLEAN.getDataType(), schema.getDataType("3").get());
        }
    }
}
