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
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InferenceSchemaStrategyTest {

    private static final Object[][] CONTENT_FIELDS = new Object[][] {
            {"integer", 1, RecordFieldType.INT.getDataType()},
            {"long", 1L, RecordFieldType.LONG.getDataType()},
            {"boolean", true, RecordFieldType.BOOLEAN.getDataType()},
            {"double", 1D, RecordFieldType.DOUBLE.getDataType()},
            {"date", new Date(), RecordFieldType.DATE.getDataType()},
            {"decimal", BigDecimal.valueOf(123.456D), RecordFieldType.DECIMAL.getDecimalDataType(6, 3)},
            {"array", new ArrayList<String>(), RecordFieldType.ARRAY.getDataType()},

            // date subclasses
            {"time", new Time(System.currentTimeMillis()), RecordFieldType.DATE.getDataType()},
            {"timestamp", new Timestamp(System.currentTimeMillis()), RecordFieldType.DATE.getDataType()},

            // others are considered as string
            {"byte", (byte) 1, RecordFieldType.STRING.getDataType()},
            {"short", (short) 1, RecordFieldType.STRING.getDataType()},
            {"bigint", BigInteger.ONE, RecordFieldType.STRING.getDataType()},
            {"float", (float) 1, RecordFieldType.STRING.getDataType()},
            {"char", (char) 1, RecordFieldType.STRING.getDataType()},
    };

    private final InferenceSchemaStrategy testSubject = new InferenceSchemaStrategy();

    @Test
    public void testSchemaConversion() throws Exception {
        // when
        final RecordSchema result = testSubject.getSchema(null, givenContent(), null);

        // then
        Assert.assertNotNull(result);
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
        Assert.assertNotNull(result);
        Assert.assertTrue(RecordDataType.class.isInstance(result.getField("f1").get().getDataType()));
        final RecordDataType recordDataType = (RecordDataType) result.getField("f1").get().getDataType();

        final RecordSchema childSchema = recordDataType.getChildSchema();
        Assert.assertNotNull(childSchema);
        Assert.assertEquals(RecordFieldType.INT.getDataType(), childSchema.getField("a1").get().getDataType());
        Assert.assertEquals(RecordFieldType.INT.getDataType(), childSchema.getField("a2").get().getDataType());
    }

    @Test
    public void testSchemaConversionFromJsonString() throws Exception {
        // given
        final String json = "{\"double\":1.0,\"integer\":1,\"long\":9223372036854775,\"boolean\":true,\"array\":[]}";

        // when
        final RecordSchema result = testSubject.getSchema(  null, new ByteArrayInputStream(json.getBytes()), null);

        // then
        Assert.assertNotNull(result);
        thenFieldsAreConvertedProperly(result, false);
    }

    private Map<String, Object> givenContent() {
        final HashMap<String, Object> result = new HashMap<>();

        for (final Object[] contentField : CONTENT_FIELDS) {
            result.put((String) contentField[0], contentField[1]);
        }

        return result;
    }

    private Map<String, DataType> givenExpected() {
        final HashMap<String, DataType> result = new HashMap<>();

        for (final Object[] contentField : CONTENT_FIELDS) {
            result.put((String) contentField[0], (DataType) contentField[2]);
        }

        return result;
    }

    private void thenFieldsAreConvertedProperly(final RecordSchema result, final boolean mustPresent) {
        final List<RecordField> fields = result.getFields();

        for (final Map.Entry<String, DataType> expected : givenExpected().entrySet()) {
            final Optional<RecordField> field = fields.stream().filter(f -> f.getFieldName().equals(expected.getKey())).findFirst();

            if (field.isPresent()) {
                Assert.assertEquals("\"" + expected.getKey() + "\" is expected to be converted " + expected.getValue().toString(), expected.getValue(), field.get().getDataType());
            } else if (mustPresent) {
                Assert.fail();
            }
        }
    }
}