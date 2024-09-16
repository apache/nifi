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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class TestExcelReader {

    private static final ByteArrayOutputStream DATE_TIME_TIMESTAMP_CONTENTS = new ByteArrayOutputStream();

    @Mock
    private ComponentLog logger;

    private ExcelReader excelReader;
    private Map<PropertyDescriptor, String> properties;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        InputStream inputStream = Files.newInputStream(Paths.get("src/test/resources/excel/dateTimeTimestamp.xlsx"));
        IOUtils.copyLarge(inputStream, DATE_TIME_TIMESTAMP_CONTENTS);
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
        InputStream inputStream = new ByteArrayInputStream(DATE_TIME_TIMESTAMP_CONTENTS.toByteArray());
        final RecordReader recordReader = excelReader.createRecordReader(new HashMap<>(), inputStream, DATE_TIME_TIMESTAMP_CONTENTS.size(), logger);

        final RecordSchema recordSchema = recordReader.getSchema();
        assertRecordSchemaFieldNamesAndTypes(recordSchema);

        final Record record = recordReader.nextRecord();
        assertRecordFieldNamesAndTypes(record);
    }

    @Test
    void testReadWithExplicitSchema() throws Exception {
        properties.put(DateTimeUtils.DATE_FORMAT, "M/dd/yy");
        properties.put(DateTimeUtils.TIME_FORMAT, "HH:mm");
        properties.put(DateTimeUtils.TIMESTAMP_FORMAT, "M/dd/yyyy HH:mm:ss");
        final String avroSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/dateTimeTimestampSchema.avsc")));
        properties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        properties.put(SchemaAccessUtils.SCHEMA_TEXT, avroSchema);
        properties.put(ExcelReader.STARTING_ROW, "2");
        initializeExcelReader();

        InputStream inputStream = new ByteArrayInputStream(DATE_TIME_TIMESTAMP_CONTENTS.toByteArray());
        RecordReader recordReader = excelReader.createRecordReader(new HashMap<>(), inputStream, DATE_TIME_TIMESTAMP_CONTENTS.size(), logger);

        final RecordSchema recordSchema = recordReader.getSchema();
        assertRecordSchemaFieldNamesAndTypes(recordSchema);

        final Record record = recordReader.nextRecord();
        assertRecordFieldNamesAndTypes(record);
    }

    private void initializeExcelReader() throws Exception {
        final ConfigurationContext context = new MockConfigurationContext(properties, null, null);
        excelReader.initialize(new MockControllerServiceInitializationContext(excelReader, "someId"));
        excelReader.onEnabled(context);
        excelReader.storeSchemaAccessStrategy(context);
    }

    private void assertRecordSchemaFieldNamesAndTypes(RecordSchema recordSchema) {
        assertEquals(3, recordSchema.getFieldCount());
        assertEquals(Arrays.asList("Date_Custom", "Time_Custom", "Timestamp"), recordSchema.getFieldNames());
        assertEquals(Arrays.asList(RecordFieldType.DATE, RecordFieldType.TIME, RecordFieldType.TIMESTAMP),
                recordSchema.getDataTypes().stream()
                        .map(DataType::getFieldType)
                        .collect(Collectors.toList()));
    }

    private void assertRecordFieldNamesAndTypes(Record record) {
        assertNotNull(record);
        assertInstanceOf(Date.class, record.getValue("Date_Custom"));
        assertInstanceOf(Time.class, record.getValue("Time_Custom"));
        assertInstanceOf(Timestamp.class, record.getValue("Timestamp"));
    }
}
