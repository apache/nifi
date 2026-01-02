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
package org.apache.nifi.schema.access;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class InferenceSchemaStrategyTest {

    private static final List<TestFieldEntry> CONTENT_FIELDS = Arrays.asList(
            new TestFieldEntry("integer", 1, RecordFieldType.INT.getDataType()),
            new TestFieldEntry("long", 1L, RecordFieldType.LONG.getDataType()),
            new TestFieldEntry("boolean", true, RecordFieldType.BOOLEAN.getDataType()),
            new TestFieldEntry("double", 1D, RecordFieldType.DOUBLE.getDataType()),
            new TestFieldEntry("date", new Date(), RecordFieldType.DATE.getDataType()),
            new TestFieldEntry("decimal", BigDecimal.valueOf(123.456D), RecordFieldType.DECIMAL.getDecimalDataType(6, 3)),
            new TestFieldEntry("array", new ArrayList<>(), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),

            // date subclasses
            new TestFieldEntry("time", new Time(System.currentTimeMillis()), RecordFieldType.DATE.getDataType()),
            new TestFieldEntry("timestamp", new Timestamp(System.currentTimeMillis()), RecordFieldType.DATE.getDataType()),

            // others are considered as string
            new TestFieldEntry("byte", (byte) 1, RecordFieldType.STRING.getDataType()),
            new TestFieldEntry("short", (short) 1, RecordFieldType.STRING.getDataType()),
            new TestFieldEntry("bigint", BigInteger.ONE, RecordFieldType.STRING.getDataType()),
            new TestFieldEntry("float", (float) 1, RecordFieldType.STRING.getDataType()),
            new TestFieldEntry("char", (char) 1, RecordFieldType.STRING.getDataType())
    );

    private final InferenceSchemaStrategy testSubject = new InferenceSchemaStrategy();

    @Test
    public void testSchemaConversion() throws Exception {
        // when
        final Map<String, Object> inputMap = CONTENT_FIELDS.stream().collect(Collectors.toMap(
                TestFieldEntry::name,
                TestFieldEntry::value
        ));

        final RecordSchema result = testSubject.getSchema(null, inputMap, null);

        // then
        assertNotNull(result);
        thenFieldsAreConvertedProperly(result, true);
    }

    @Test
    public void testSchemaConversionWhenMap() throws Exception {
        // given
        final Map<String, Object> input = new HashMap<>();
        final Map<String, Integer> field = new HashMap<>();
        field.put("a1", 1);
        field.put("a2", 2);
        input.put("f1", field);

        // when
        final RecordSchema result = testSubject.getSchema(null, input, null);

        // then
        assertNotNull(result);
        assertInstanceOf(RecordDataType.class, result.getField("f1").get().getDataType());
        final RecordDataType recordDataType = (RecordDataType) result.getField("f1").get().getDataType();

        final RecordSchema childSchema = recordDataType.getChildSchema();
        assertNotNull(childSchema);
        assertEquals(RecordFieldType.INT.getDataType(), childSchema.getField("a1").get().getDataType());
        assertEquals(RecordFieldType.INT.getDataType(), childSchema.getField("a2").get().getDataType());
    }

    @Test
    public void testSchemaConversionFromJsonString() throws Exception {
        // given
        final String json = "{\"double\":1.0,\"integer\":1,\"long\":9223372036854775,\"boolean\":true,\"array\":[]}";

        // when
        final RecordSchema result = testSubject.getSchema(null, new ByteArrayInputStream(json.getBytes()), null);

        // then
        assertNotNull(result);
        thenFieldsAreConvertedProperly(result, false);
    }

    @Test
    public void testSchemaConversionFromJsonStringWithNestedRecordArray() throws Exception {
        // given
        final String json = "{\"test-array\":[{\"string\":\"string-value\",\"int\":1}]}";

        // when
        final RecordSchema result = testSubject.getSchema(null, new ByteArrayInputStream(json.getBytes()), null);

        // then
        assertNotNull(result);
        assertInstanceOf(ArrayDataType.class, result.getField("test-array").get().getDataType());
        final ArrayDataType arrayDataType = (ArrayDataType) result.getField("test-array").get().getDataType();

        final DataType elementDataType = arrayDataType.getElementType();
        assertInstanceOf(RecordDataType.class, elementDataType);

        final RecordSchema childSchema = ((RecordDataType) elementDataType).getChildSchema();
        assertNotNull(childSchema);
        assertEquals(RecordFieldType.STRING.getDataType(), childSchema.getField("string").get().getDataType());
        assertEquals(RecordFieldType.INT.getDataType(), childSchema.getField("int").get().getDataType());
    }

    private void thenFieldsAreConvertedProperly(final RecordSchema result, final boolean mustPresent) {
        final List<RecordField> fields = result.getFields();

        for (final TestFieldEntry expected : CONTENT_FIELDS) {
            final Optional<RecordField> field = fields.stream().filter(f -> f.getFieldName().equals(expected.name())).findFirst();

            if (field.isPresent()) {
                assertEquals(expected.dataType(), field.get().getDataType(),
                        "\"" + expected.name() + "\" is expected to be converted " + expected.dataType().toString());
            } else if (mustPresent) {
                fail();
            }
        }
    }

    public record TestFieldEntry(String name, Object value, DataType dataType) {
    }
}
