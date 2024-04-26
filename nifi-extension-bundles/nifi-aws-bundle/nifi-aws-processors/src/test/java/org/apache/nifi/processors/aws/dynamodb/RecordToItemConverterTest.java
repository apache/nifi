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
package org.apache.nifi.processors.aws.dynamodb;

import org.apache.nifi.action.Component;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordToItemConverterTest {

    @Test
    public void testConvertingSimpleFields() {
        final List<RecordField> schemaFields = new ArrayList<>();
        schemaFields.add(new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType()));
        schemaFields.add(new RecordField("short", RecordFieldType.SHORT.getDataType()));
        schemaFields.add(new RecordField("int", RecordFieldType.INT.getDataType()));
        schemaFields.add(new RecordField("long", RecordFieldType.LONG.getDataType()));
        schemaFields.add(new RecordField("byte", RecordFieldType.BYTE.getDataType()));
        schemaFields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        schemaFields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        schemaFields.add(new RecordField("bigint", RecordFieldType.BIGINT.getDataType()));
        schemaFields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDataType()));
        schemaFields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        schemaFields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        schemaFields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        schemaFields.add(new RecordField("char", RecordFieldType.CHAR.getDataType()));
        schemaFields.add(new RecordField("enum", RecordFieldType.ENUM.getDataType()));
        schemaFields.add(new RecordField("array", RecordFieldType.ARRAY.getDataType()));
        schemaFields.add(new RecordField("choice", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.BOOLEAN.getDataType(), RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(schemaFields);

        final Map<String, Object> values = new HashMap<>();
        values.put("boolean", Boolean.TRUE);
        values.put("short", Short.MAX_VALUE);
        values.put("int", Integer.MAX_VALUE);
        values.put("long", Long.MAX_VALUE);
        values.put("byte", Byte.MAX_VALUE);
        values.put("float", Float.valueOf(123.456F));
        values.put("double", Double.valueOf(1234.5678D));
        values.put("bigint", BigInteger.TEN);
        values.put("decimal", new BigDecimal("12345678901234567890.123456789012345678901234567890"));
        values.put("timestamp", new java.sql.Timestamp(37293723L));
        values.put("date", java.sql.Date.valueOf("1970-01-01"));
        values.put("time", new java.sql.Time(37293723L));
        values.put("char", 'c');
        values.put("enum", Component.Controller);
        values.put("array", new Integer[] {0,1,10});
        values.put("choice", Integer.MAX_VALUE);
        final Record record = new MapRecord(schema, values);

        final Map<String, AttributeValue> item = new HashMap<>();

        for (final RecordField schemaField : schema.getFields()) {
            RecordToItemConverter.addField(record, item, schemaField.getDataType().getFieldType(), schemaField.getFieldName());
        }

        assertEquals(bool(Boolean.TRUE), item.get("boolean"));

        // Internally Item stores numbers as BigDecimal
        assertEquals(number(Short.MAX_VALUE), item.get("short"));
        assertEquals(number(Integer.MAX_VALUE), item.get("int"));
        assertEquals(number(Long.MAX_VALUE), item.get("long"));
        assertEquals(number(Byte.MAX_VALUE), item.get("byte"));
        assertEquals(number(new BigDecimal("12345678901234567890.123456789012345678901234567890")), item.get("decimal"));
        assertEquals(123.456F, Float.valueOf(item.get("float").n()).floatValue(), 0.0001);
        assertEquals(1234.5678D, Float.valueOf(item.get("double").n()).doubleValue(), 0.0001);

        assertEquals(number(10), item.get("bigint"));

        // DynamoDB uses string to represent time and date
        assertNotNull(item.get("timestamp").s());
        assertNotNull(item.get("date").s());
        assertNotNull(item.get("time").s());

        // Character is unknown type for DynamoDB, as well as enum
        assertEquals(string("c"), item.get("char"));

        // Enum is not supported in DynamoDB
        assertEquals(string(Component.Controller.name()), item.get("enum"));

        // DynamoDB uses lists and still keeps the payload datatype
        Assertions.assertIterableEquals(Arrays.asList(number(BigDecimal.ZERO), number(BigDecimal.ONE), number(BigDecimal.TEN)),
                item.get("array").l());

        // DynamoDB cannot handle choice, all values enveloped into choice are handled as strings
        assertEquals(string("2147483647"), item.get("choice"));
    }

    @Test
    public void testConvertingMapField() {
        final List<RecordField> starSystemSchemaFields = new ArrayList<>();
        starSystemSchemaFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        starSystemSchemaFields.add(new RecordField("observedPlanets", RecordFieldType.INT.getDataType()));
        starSystemSchemaFields.add(new RecordField("star", RecordFieldType.MAP.getDataType()));
        final RecordSchema starSystemSchema = new SimpleRecordSchema(starSystemSchemaFields);

        final Map<String, Object> star = new HashMap<>();
        star.put("type", 'G');
        star.put("isDwarf", false);

        final Map<String, Object> starSystemValues = new HashMap<>();
        starSystemValues.put("name", "Tau Ceti");
        starSystemValues.put("observedPlanets", "5");
        starSystemValues.put("star", star);
        final Record starSystem = new MapRecord(starSystemSchema, starSystemValues);

        final Map<String, AttributeValue> item = new HashMap<>();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.MAP, "star");

        final AttributeValue result = item.get("star");
        assertTrue(result.hasM());
        final Map<String, AttributeValue> resultMap = result.m();
        assertEquals(2, resultMap.size());
        assertEquals(bool(false), resultMap.get("isDwarf"));
        assertEquals(string("G"), resultMap.get("type"));
    }

    @Test
    public void testConvertingMultipleLevelsOfMaps() {
        final List<RecordField> starSystemSchemaFields = new ArrayList<>();
        starSystemSchemaFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        starSystemSchemaFields.add(new RecordField("observedPlanets", RecordFieldType.INT.getDataType()));
        starSystemSchemaFields.add(new RecordField("star", RecordFieldType.MAP.getDataType()));
        final RecordSchema starSystemSchema = new SimpleRecordSchema(starSystemSchemaFields);

        final Map<String, Object> starType = new HashMap<>();
        starType.put("type", 'G');
        starType.put("isDwarf", false);

        final Map<String, Object> star = new HashMap<>();
        star.put("starType", starType);

        final Map<String, Object> starSystemValues = new HashMap<>();
        starSystemValues.put("name", "Tau Ceti");
        starSystemValues.put("observedPlanets", "5");
        starSystemValues.put("star", star);
        final Record starSystem = new MapRecord(starSystemSchema, starSystemValues);

        final Map<String, AttributeValue> item = new HashMap<>();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.MAP, "star");

        final AttributeValue result = item.get("star");
        assertTrue(result.hasM());
        final Map<String, AttributeValue> resultMap = result.m();
        assertEquals(1, resultMap.size());
        final AttributeValue starTypeResult = resultMap.get("starType");
        assertTrue(starTypeResult.hasM());
        final Map<String, AttributeValue> starTypeResultMap = starTypeResult.m();
        assertEquals(bool(false), starTypeResultMap.get("isDwarf"));
        assertEquals(string("G"), starTypeResultMap.get("type"));
    }

    @Test
    public void testConvertingRecordField() {
        final List<RecordField> starSchemaFields = new ArrayList<>();
        starSchemaFields.add(new RecordField("type", RecordFieldType.CHAR.getDataType()));
        starSchemaFields.add(new RecordField("isDwarf", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema starSchema = new SimpleRecordSchema(starSchemaFields);

        final List<RecordField> starSystemSchemaFields = new ArrayList<>();
        starSystemSchemaFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        starSystemSchemaFields.add(new RecordField("observedPlanets", RecordFieldType.INT.getDataType()));
        starSystemSchemaFields.add(new RecordField("star", RecordFieldType.RECORD.getRecordDataType(starSchema)));
        final RecordSchema starSystemSchema = new SimpleRecordSchema(starSystemSchemaFields);

        final Map<String, Object> starValues = new HashMap<>();
        starValues.put("type", 'G');
        starValues.put("isDwarf", false);
        final Record star = new MapRecord(starSchema, starValues);

        final Map<String, Object> starSystemValues = new HashMap<>();
        starSystemValues.put("name", "Tau Ceti");
        starSystemValues.put("observedPlanets", 5);
        starSystemValues.put("star", star);
        final Record starSystem = new MapRecord(starSystemSchema, starSystemValues);

        final Map<String, AttributeValue> item = new HashMap<>();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.RECORD, "star");

        final AttributeValue result = item.get("star");
        assertNotNull(result.m());
        final Map<String, AttributeValue> resultMap = result.m();
        assertEquals(2, resultMap.size());
        assertEquals(bool(false), resultMap.get("isDwarf"));
        assertEquals(string("G"), resultMap.get("type"));
    }

    @Test
    public void testConvertingMultipleLevelsOfRecords() {
        final List<RecordField> starTypeSchemaFields = new ArrayList<>();
        starTypeSchemaFields.add(new RecordField("type", RecordFieldType.CHAR.getDataType()));
        starTypeSchemaFields.add(new RecordField("isDwarf", RecordFieldType.BOOLEAN.getDataType()));
        final RecordSchema starTypeSchema = new SimpleRecordSchema(starTypeSchemaFields);

        final List<RecordField> starSchemaFields = new ArrayList<>();
        starSchemaFields.add(new RecordField("starType", RecordFieldType.RECORD.getRecordDataType(starTypeSchema)));
        final RecordSchema starSchema = new SimpleRecordSchema(starSchemaFields);

        final List<RecordField> starSystemSchemaFields = new ArrayList<>();
        starSystemSchemaFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        starSystemSchemaFields.add(new RecordField("observedPlanets", RecordFieldType.INT.getDataType()));
        starSystemSchemaFields.add(new RecordField("star", RecordFieldType.RECORD.getRecordDataType(starSchema)));
        final RecordSchema starSystemSchema = new SimpleRecordSchema(starSystemSchemaFields);

        final Map<String, Object> starTypeValues = new HashMap<>();
        starTypeValues.put("type", 'G');
        starTypeValues.put("isDwarf", false);
        final Record starType = new MapRecord(starTypeSchema, starTypeValues);

        final Map<String, Object> starValues = new HashMap<>();
        starValues.put("starType", starType);
        final Record star = new MapRecord(starSchema, starValues);

        final Map<String, Object> starSystemValues = new HashMap<>();
        starSystemValues.put("name", "Tau Ceti");
        starSystemValues.put("observedPlanets", 5);
        starSystemValues.put("star", star);
        final Record starSystem = new MapRecord(starSystemSchema, starSystemValues);

        final Map<String, AttributeValue> item = new HashMap<>();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.RECORD, "star");

        final AttributeValue result = item.get("star");
        assertNotNull(result.m());
        final Map<String, AttributeValue> resultMap = result.m();
        assertEquals(1, resultMap.size());
        final AttributeValue fieldResult = resultMap.get("starType");
        Assertions.assertNotNull(fieldResult.m());
        final Map<String, AttributeValue> fieldResultMap = fieldResult.m();
        assertEquals(bool(false), fieldResultMap.get("isDwarf"));
        assertEquals(string("G"), fieldResultMap.get("type"));
    }

    private static AttributeValue bool(final Boolean value) {
        return AttributeValue.builder().bool(value).build();
    }

    private static AttributeValue string(final String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(final Number value) {
        return AttributeValue.builder().n(String.valueOf(value)).build();
    }
}