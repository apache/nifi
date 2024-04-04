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
package org.apache.nifi.processors.salesforce.util;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.salesforce.schema.SalesforceToRecordSchemaConverter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SalesforceToRecordSchemaConverterTest {

    private static final String TEST_PATH = "src/test/resources/converter/";
    private static final String DATE_FORMAT = "yyyy-mm-dd";
    private static final String TIME_STAMP_FORMAT = "yyyy-mm-dd / hh:mm:ss";
    private static final String TIME_FORMAT = "hh:mm:ss";

    private static final SalesforceToRecordSchemaConverter converter = new SalesforceToRecordSchemaConverter(DATE_FORMAT, TIME_STAMP_FORMAT, TIME_FORMAT);

    @Test
    void testConvertSchema() throws IOException {
        final String salesforceSchemaFileName = "simple_sf_schema.json";
        final String fieldNames = "ExampleInt,ExampleLong,ExampleDouble,ExampleBoolean," +
                "ExampleID,ExampleString,ExampleJson,ExampleBase64Binary,ExampleAnyType," +
                "ExampleDate,ExampleDateTime,ExampleTime";

        final RecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                // primitives
                new RecordField("ExampleInt", RecordFieldType.INT.getDataType()),
                new RecordField("ExampleLong", RecordFieldType.LONG.getDataType()),
                new RecordField("ExampleDouble", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("ExampleBoolean", RecordFieldType.BOOLEAN.getDataType()),
                // string types
                new RecordField("ExampleID", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleString", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleJson", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleBase64Binary", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleAnyType", RecordFieldType.STRING.getDataType()),
                // date types
                new RecordField("ExampleDate", RecordFieldType.DATE.getDataType(DATE_FORMAT)),
                new RecordField("ExampleDateTime", RecordFieldType.TIMESTAMP.getDataType(TIME_STAMP_FORMAT)),
                new RecordField("ExampleTime", RecordFieldType.TIME.getDataType(TIME_FORMAT))
        ));

        try (final InputStream sfSchema = readFile(TEST_PATH + salesforceSchemaFileName)) {
            final SObjectDescription salesforceObject = converter.getSalesforceObject(sfSchema);
            final RecordSchema actual = converter.convertSchema(salesforceObject, fieldNames);

            assertEquals(expected, actual);
        }
    }

    @Test
    void testConvertComplexTypes() throws IOException {
        final String salesforceSchemaFileName = "complex_sf_schema.json";
        final String fieldNames = "ExampleAddress,ExampleLocation";

        final RecordSchema addressSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("city", RecordFieldType.STRING.getDataType(), true),
                new RecordField("country", RecordFieldType.STRING.getDataType(), true),
                new RecordField("countryCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("postalCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("state", RecordFieldType.STRING.getDataType(), true),
                new RecordField("stateCode", RecordFieldType.STRING.getDataType(), true),
                new RecordField("street", RecordFieldType.STRING.getDataType(), true),
                new RecordField("geocodeAccuracy", RecordFieldType.STRING.getDataType(), true)
        ));

        final RecordSchema locationSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("latitude", RecordFieldType.STRING.getDataType(), true),
                new RecordField("longitude", RecordFieldType.STRING.getDataType(), true)
        ));

        RecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                new RecordField("ExampleAddress", RecordFieldType.RECORD.getRecordDataType(addressSchema)),
                new RecordField("ExampleLocation", RecordFieldType.RECORD.getRecordDataType(locationSchema))
        ));

        try (final InputStream sfSchema = readFile(TEST_PATH + salesforceSchemaFileName)) {
            final SObjectDescription salesforceObject = converter.getSalesforceObject(sfSchema);
            final RecordSchema actual = converter.convertSchema(salesforceObject, fieldNames);

            assertEquals(expected, actual);
        }
    }

    @Test
    void testSelectFields() throws IOException {
        final String salesforceSchemaFileName = "simple_sf_schema.json";
        final String fieldNames = "ExampleInt,ExampleTime";

        final RecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                new RecordField("ExampleInt", RecordFieldType.INT.getDataType()),
                new RecordField("ExampleTime", RecordFieldType.TIME.getDataType(TIME_FORMAT))
        ));

        try (final InputStream sfSchema = readFile(TEST_PATH + salesforceSchemaFileName)) {
            final SObjectDescription salesforceObject = converter.getSalesforceObject(sfSchema);
            final RecordSchema actual = converter.convertSchema(salesforceObject, fieldNames);

            assertEquals(expected, actual);
        }
    }

    @Test
    void testSelectAllFields() throws IOException {
        final String salesforceSchemaFileName = "simple_sf_schema.json";
        final String fieldNames = "";

        final RecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                new RecordField("ExampleInt", RecordFieldType.INT.getDataType()),
                new RecordField("ExampleLong", RecordFieldType.LONG.getDataType()),
                new RecordField("ExampleDouble", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("ExampleBoolean", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("ExampleID", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleString", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleJson", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleBase64Binary", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleAnyType", RecordFieldType.STRING.getDataType()),
                new RecordField("ExampleDate", RecordFieldType.DATE.getDataType("yyyy-mm-dd")),
                new RecordField("ExampleDateTime", RecordFieldType.TIMESTAMP.getDataType("yyyy-mm-dd / hh:mm:ss")),
                new RecordField("ExampleTime", RecordFieldType.TIME.getDataType("hh:mm:ss"))
        ));

        try (final InputStream sfSchema = readFile(TEST_PATH + salesforceSchemaFileName)) {
            final SObjectDescription salesforceObject = converter.getSalesforceObject(sfSchema);
            final RecordSchema actual = converter.convertSchema(salesforceObject, fieldNames);

            assertEquals(expected, actual);
        }
    }

    @Test
    void testConvertEmptySchema() throws IOException {
        try (final InputStream sfSchema = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8))) {
            assertThrows(MismatchedInputException.class, () -> converter.getSalesforceObject(sfSchema));
        }
    }

    @Test
    void testConvertNullSchema() throws IOException {
        final InputStream sfSchema = null;
        assertThrows(IllegalArgumentException.class, () -> converter.getSalesforceObject(sfSchema));
    }

    @Test
    void testConvertUnknownDataType() throws IOException {
        try (final InputStream sfSchema = readFile(TEST_PATH + "unknown_type_sf_schema.json")) {
            final String fieldNames = "FieldWithUnknownType";
            final SObjectDescription salesforceObject = converter.getSalesforceObject(sfSchema);
            assertThrows(ProcessException.class, () -> converter.convertSchema(salesforceObject, fieldNames));
        }
    }

    private InputStream readFile(final String path) throws IOException {
        return new FileInputStream(path);
    }
}
