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
package org.apache.nifi.json;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestJsonSchemaInference {

    private final TimeValueInference timestampInference = new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Test
    void testInferenceIncludesAllRecords() throws IOException {
        final RecordSchema schema = inferSchema(new File("src/test/resources/json/data-types.json"));

        assertSame(RecordFieldType.STRING, schema.getDataType("varcharc").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("uuid").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("tinyintc").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("textc").get().getFieldType());
        assertEquals(RecordFieldType.DATE.getDataType("yyyy-MM-dd"), schema.getDataType("datec").get());
        assertSame(RecordFieldType.INT, schema.getDataType("smallintc").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("mediumintc").get().getFieldType());
        assertSame(RecordFieldType.LONG, schema.getDataType("longintc").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("intc").get().getFieldType());
        assertSame(RecordFieldType.BIGINT, schema.getDataType("bigintc").get().getFieldType());
        assertSame(RecordFieldType.DOUBLE, schema.getDataType("floatc").get().getFieldType());
        assertSame(RecordFieldType.DOUBLE, schema.getDataType("doublec").get().getFieldType());
        assertSame(RecordFieldType.DOUBLE, schema.getDataType("decimalc").get().getFieldType());
        assertEquals(RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), schema.getDataType("timestampc").get());
        assertEquals(RecordFieldType.TIME.getDataType("HH:mm:ss"), schema.getDataType("timec").get());
        assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("charc").get());
        assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("tinytextc").get());
        assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("blobc").get());
        assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("mediumtextc").get());
        assertSame(RecordFieldType.INT, schema.getDataType("enumc").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("setc").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("boolc").get().getFieldType());
        assertEquals(RecordFieldType.STRING.getDataType(), schema.getDataType("binaryc").get());
        // We currently do not read BigDecimal from JSON as ObjectMapper in InferenceSchemaStrategy automatically reads it as double

        final List<String> fieldNames = schema.getFieldNames();
        assertEquals(Arrays.asList("varcharc", "uuid", "tinyintc", "textc", "datec", "smallintc", "mediumintc", "longintc", "intc", "bigintc",
                "floatc", "doublec", "decimalc", "timestampc", "timec", "charc", "tinytextc", "blobc", "mediumtextc", "enumc", "setc", "boolc", "binaryc"), fieldNames);
    }

    @Test
    public void testNestedArrayOfRecords() throws IOException {
        final RecordSchema schema = inferSchema(new File("src/test/resources/json/choice-of-array-empty-or-array-record.json"));
        final RecordField dataField = schema.getField("data").get();
        assertSame(RecordFieldType.RECORD, dataField.getDataType().getFieldType());

        final RecordDataType dataFieldType = (RecordDataType) dataField.getDataType();
        final RecordSchema dataSchema = dataFieldType.getChildSchema();

        final DataType itemsDataType = dataSchema.getDataType("items").get();
        assertSame(RecordFieldType.ARRAY, itemsDataType.getFieldType());

        final ArrayDataType itemsArrayType = (ArrayDataType) itemsDataType;
        final DataType itemsElementType = itemsArrayType.getElementType();
        assertEquals(RecordFieldType.RECORD, itemsElementType.getFieldType());

        final RecordSchema itemsSchema = ((RecordDataType) itemsElementType).getChildSchema();
        final RecordField itemSchedulesField = itemsSchema.getField("itemSchedules").get();
        final DataType itemSchedulesDataType = itemSchedulesField.getDataType();
        assertEquals(RecordFieldType.ARRAY, itemSchedulesDataType.getFieldType());

        final ArrayDataType schedulesArrayType = (ArrayDataType) itemSchedulesDataType;
        final DataType schedulesElementType = schedulesArrayType.getElementType();
        assertEquals(RecordFieldType.RECORD, schedulesElementType.getFieldType());
    }

    @Test
    public void testEmptyArrays() throws IOException {
        final RecordSchema schema = inferSchema(new File("src/test/resources/json/empty-arrays.json"));
        final DataType itemsDataType = schema.getDataType("items").get();
        assertSame(RecordFieldType.ARRAY, itemsDataType.getFieldType());

        final ArrayDataType itemsArrayType = (ArrayDataType) itemsDataType;
        final DataType itemsElementType = itemsArrayType.getElementType();
        assertEquals(RecordFieldType.RECORD, itemsElementType.getFieldType());

        final RecordSchema itemsSchema = ((RecordDataType) itemsElementType).getChildSchema();
        final RecordField itemDataField = itemsSchema.getField("itemData").get();
        final DataType itemDataDatatype = itemDataField.getDataType();
        assertEquals(RecordFieldType.ARRAY, itemDataDatatype.getFieldType());

        final ArrayDataType itemDataArrayType = (ArrayDataType) itemDataDatatype;
        final DataType itemDataElementType = itemDataArrayType.getElementType();
        // Empty arrays should be inferred as array<string>
        assertEquals(RecordFieldType.STRING, itemDataElementType.getFieldType());
    }

    @Test
    public void testNestedChoiceOfArrayTypes() throws IOException {
        final RecordSchema schema = inferSchema(new File("src/test/resources/json/nested-choice-of-record-array-or-string.json"));
        final DataType testRecordDataType = schema.getDataType("test_record").get();
        assertSame(RecordFieldType.RECORD, testRecordDataType.getFieldType());

        final RecordDataType recordDataType = (RecordDataType) testRecordDataType;
        final DataType childDataType = recordDataType.getChildSchema().getDataType("array_test_record").get();
        assertSame(RecordFieldType.RECORD, childDataType.getFieldType());

        final RecordSchema mergedChildSchema = ((RecordDataType) childDataType).getChildSchema();
        final DataType testArrayDataType = mergedChildSchema.getDataType("test_array").get();
        assertSame(RecordFieldType.CHOICE, testArrayDataType.getFieldType());

        final ChoiceDataType testArrayChoiceType = (ChoiceDataType) testArrayDataType;
        final List<DataType> choices = testArrayChoiceType.getPossibleSubTypes();
        assertEquals(2, choices.size());

        boolean hasArrayOfRecord = false;
        boolean hasArrayOfString = false;
        for (final DataType choice : choices) {
            assertSame(RecordFieldType.ARRAY, choice.getFieldType());
            final DataType elementType = ((ArrayDataType) choice).getElementType();
            if (elementType.getFieldType() == RecordFieldType.RECORD) {
                hasArrayOfRecord = true;
            } else if (elementType.getFieldType() == RecordFieldType.STRING) {
                hasArrayOfString = true;
            }
        }
        
        assertTrue(hasArrayOfRecord);
        assertTrue(hasArrayOfString);
    }

    @Test
    public void testNestedChoiceOfEmptyOrStringArray() throws IOException {
        final RecordSchema schema = inferSchema(new File("src/test/resources/json/nested-choice-of-empty-array-or-string.json"));
        final DataType testRecordDataType = schema.getDataType("test_record").get();
        assertSame(RecordFieldType.RECORD, testRecordDataType.getFieldType());

        final RecordDataType recordDataType = (RecordDataType) testRecordDataType;
        final DataType childDataType = recordDataType.getChildSchema().getDataType("array_test_record").get();
        assertSame(RecordFieldType.RECORD, childDataType.getFieldType());

        final RecordSchema childSchema = ((RecordDataType) childDataType).getChildSchema();
        final DataType arrayDataType = childSchema.getDataType("test_array").get();
        assertSame(RecordFieldType.ARRAY, arrayDataType.getFieldType());

        final DataType arrayElementType = ((ArrayDataType) arrayDataType).getElementType();
        assertSame(RecordFieldType.STRING, arrayElementType.getFieldType());
    }

    private RecordSchema inferSchema(final File jsonFile) throws IOException {
        try (final InputStream in = new FileInputStream(jsonFile);
             final InputStream bufferedIn = new BufferedInputStream(in)) {

            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                (variables, content) -> new JsonRecordSource(content),
                new JsonSchemaInference(timestampInference), Mockito.mock(ComponentLog.class));

            final RecordSchema schema = accessStrategy.getSchema(null, bufferedIn, null);
            return schema;
        }

    }
}
