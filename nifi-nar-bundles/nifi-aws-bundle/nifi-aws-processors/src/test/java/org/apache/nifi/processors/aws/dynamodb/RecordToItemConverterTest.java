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

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.nifi.action.Component;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        final Item item = new Item();

        for (final RecordField schemaField : schema.getFields()) {
            RecordToItemConverter.addField(record, item, schemaField.getDataType().getFieldType(), schemaField.getFieldName());
        }

        Assertions.assertEquals(Boolean.TRUE, item.get("boolean"));

        // Internally Item stores numbers as BigDecimal
        Assertions.assertEquals(BigDecimal.valueOf(Short.MAX_VALUE), item.get("short"));
        Assertions.assertEquals(BigDecimal.valueOf(Integer.MAX_VALUE), item.get("int"));
        Assertions.assertEquals(BigDecimal.valueOf(Long.MAX_VALUE), item.get("long"));
        Assertions.assertEquals(BigDecimal.valueOf(Byte.MAX_VALUE), item.get("byte"));
        Assertions.assertEquals(new BigDecimal("12345678901234567890.123456789012345678901234567890"), item.get("decimal"));
        Assertions.assertEquals(Float.valueOf(123.456F), ((BigDecimal) item.get("float")).floatValue(), 0.0001);
        Assertions.assertEquals(Double.valueOf(1234.5678D), ((BigDecimal) item.get("double")).floatValue(), 0.0001);

        Assertions.assertEquals(BigDecimal.valueOf(10), item.get("bigint"));

        // DynamoDB uses string to represent time and date
        Assertions.assertTrue(item.get("timestamp") instanceof String);
        Assertions.assertTrue(item.get("date") instanceof String);
        Assertions.assertTrue(item.get("time") instanceof String);

        // Character is unknown type for DynamoDB, as well as enum
        Assertions.assertEquals("c", item.get("char"));

        // Enum is not supported in DynamoDB
        Assertions.assertEquals(Component.Controller.name(), item.get("enum"));

        // DynamoDB uses lists and still keeps the payload datatype
        Assertions.assertIterableEquals(Arrays.asList(new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN}), (Iterable<?>) item.get("array"));

        // DynamoDB cannot handle choice, all values enveloped into choice are handled as strings
        Assertions.assertEquals("2147483647", item.get("choice"));
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

        final Item item = new Item();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.MAP, "star");

        final Object result = item.get("star");
        Assertions.assertTrue(result instanceof Map);
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        Assertions.assertEquals(2, resultMap.size());
        Assertions.assertEquals(false, resultMap.get("isDwarf"));
        Assertions.assertEquals("G", resultMap.get("type"));
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

        final Item item = new Item();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.MAP, "star");

        final Object result = item.get("star");
        Assertions.assertTrue(result instanceof Map);
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        Assertions.assertEquals(1, resultMap.size());
        final Object starTypeResult = resultMap.get("starType");
        Assertions.assertTrue(starTypeResult instanceof Map);
        final Map<String, Object> starTypeResultMap = (Map<String, Object>) starTypeResult;
        Assertions.assertEquals(false, starTypeResultMap.get("isDwarf"));
        Assertions.assertEquals("G", starTypeResultMap.get("type"));
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

        final Item item = new Item();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.RECORD, "star");

        final Object result = item.get("star");
        Assertions.assertTrue(result instanceof Map);
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        Assertions.assertEquals(2, resultMap.size());
        Assertions.assertEquals(false, resultMap.get("isDwarf"));
        Assertions.assertEquals("G", resultMap.get("type"));
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

        final Item item = new Item();

        RecordToItemConverter.addField(starSystem, item, RecordFieldType.RECORD, "star");

        final Object result = item.get("star");
        Assertions.assertTrue(result instanceof Map);
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        Assertions.assertEquals(1, resultMap.size());
        final Object fieldResult = resultMap.get("starType");
        Assertions.assertTrue(fieldResult instanceof Map);
        final Map<String, Object> fieldResultMap = (Map<String, Object>) fieldResult;
        Assertions.assertEquals(false, fieldResultMap.get("isDwarf"));
        Assertions.assertEquals("G", fieldResultMap.get("type"));
    }
}