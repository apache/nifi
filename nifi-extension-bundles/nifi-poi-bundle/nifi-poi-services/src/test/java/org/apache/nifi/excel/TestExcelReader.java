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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class TestExcelReader {

    private static final ByteArrayOutputStream ALL_TYPES_CONTENTS = new ByteArrayOutputStream();
    private static final Map<String, Class<?>> FIELD_NAMES_AND_CLASS_TYPES = new LinkedHashMap<>();

    @Mock
    private ComponentLog logger;

    private ExcelReader excelReader;
    private Map<PropertyDescriptor, String> properties;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        InputStream inputStream = Files.newInputStream(Paths.get("src/test/resources/excel/all_types.xlsx"));
        IOUtils.copyLarge(inputStream, ALL_TYPES_CONTENTS);
        FIELD_NAMES_AND_CLASS_TYPES.put("Date_Custom", Date.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Time_Custom", Time.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Timestamp", Timestamp.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Boolean_Function", Boolean.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Number_Function", Integer.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Blank_Cell", String.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("String", String.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Numeric", Float.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Variable_Missing", String.class);
        FIELD_NAMES_AND_CLASS_TYPES.put("Syntax_Error", String.class);
    }

    @BeforeEach
    void setUp() {
        excelReader = new ExcelReader();
        properties = new HashMap<>();
        new ExcelReader().getSupportedPropertyDescriptors().forEach(prop -> properties.put(prop, prop.getDefaultValue()));
    }

    @Test
    void testReadWithInferredSchema() throws Exception {
        properties.put(DateTimeUtils.DATE_FORMAT, "M/dd/yy");
        properties.put(DateTimeUtils.TIME_FORMAT, "HH:mm");
        properties.put(DateTimeUtils.TIMESTAMP_FORMAT, "M/dd/yyyy HH:mm:ss");
        properties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, ExcelHeaderSchemaStrategy.USE_STARTING_ROW.getValue());
        initializeExcelReader();
        InputStream inputStream = new ByteArrayInputStream(ALL_TYPES_CONTENTS.toByteArray());
        final RecordReader recordReader = excelReader.createRecordReader(new HashMap<>(), inputStream, ALL_TYPES_CONTENTS.size(), logger);

        final RecordSchema recordSchema = recordReader.getSchema();
        assertRecordSchemaFieldNamesAndTypes(recordSchema);

        final Record record = recordReader.nextRecord();
        assertRecordFieldNamesAndClassTypes(record);
    }

    @Test
    void testReadWithExplicitSchema() throws Exception {
        properties.put(DateTimeUtils.DATE_FORMAT, "M/dd/yy");
        properties.put(DateTimeUtils.TIME_FORMAT, "HH:mm");
        properties.put(DateTimeUtils.TIMESTAMP_FORMAT, "M/dd/yyyy HH:mm:ss");
        final String avroSchema = Files.readString(Paths.get("src/test/resources/allTypesSchema.avsc"));
        properties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        properties.put(SchemaAccessUtils.SCHEMA_TEXT, avroSchema);
        properties.put(ExcelReader.STARTING_ROW, "2");
        initializeExcelReader();

        InputStream inputStream = new ByteArrayInputStream(ALL_TYPES_CONTENTS.toByteArray());
        RecordReader recordReader = excelReader.createRecordReader(new HashMap<>(), inputStream, ALL_TYPES_CONTENTS.size(), logger);

        final RecordSchema recordSchema = recordReader.getSchema();
        assertRecordSchemaFieldNamesAndTypes(recordSchema);

        final Record record = recordReader.nextRecord();
        assertRecordFieldNamesAndClassTypes(record);
    }

    private void initializeExcelReader() throws Exception {
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        excelReader.initialize(new MockControllerServiceInitializationContext(excelReader, "someId"));
        excelReader.onEnabled(context);
        excelReader.storeSchemaAccessStrategy(context);
    }

    private void assertRecordSchemaFieldNamesAndTypes(RecordSchema recordSchema) {
        assertEquals(10, recordSchema.getFieldCount());
        final List<String> expectedFieldNames = new ArrayList<>(FIELD_NAMES_AND_CLASS_TYPES.keySet());
        assertEquals(expectedFieldNames, recordSchema.getFieldNames());

        final List<RecordFieldType> expectedRecordFieldTypes = Arrays.asList(RecordFieldType.DATE, RecordFieldType.TIME, RecordFieldType.TIMESTAMP, RecordFieldType.BOOLEAN,
                RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.FLOAT, RecordFieldType.STRING, RecordFieldType.STRING);
        final List<RecordFieldType> actualRecordFieldTypes = recordSchema.getDataTypes().stream()
                .map(DataType::getFieldType)
                .toList();
        assertEquals(expectedRecordFieldTypes, actualRecordFieldTypes);
    }

    private void assertRecordFieldNamesAndClassTypes(Record record) {
        assertNotNull(record);
        for (Map.Entry<String, Class<?>> entry : FIELD_NAMES_AND_CLASS_TYPES.entrySet()) {
            final String fieldName = entry.getKey();
            final Class<?> expectedRecordValueClass = entry.getValue();
            final Object recordValue = record.getValue(fieldName);
            assertInstanceOf(expectedRecordValueClass, recordValue);
        }
    }
}
