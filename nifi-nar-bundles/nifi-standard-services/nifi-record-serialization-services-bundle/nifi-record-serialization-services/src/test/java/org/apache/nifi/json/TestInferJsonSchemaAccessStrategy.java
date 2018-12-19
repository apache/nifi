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
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.codehaus.jackson.JsonNode;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInferJsonSchemaAccessStrategy {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = "yyyy-MM-DD'T'HH:mm:ss.SSS'Z'";

    private final SchemaInferenceEngine<JsonNode> timestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    private final SchemaInferenceEngine<JsonNode> noTimestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

    @Test
    @Ignore
    public void testPerformanceOfSchemaInferenceWithTimestamp() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final byte[] data = Files.readAllBytes(file.toPath());
        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        final byte[] manyCopies = new byte[data.length * 20];
        for (int i=0; i < 20; i++) {
            System.arraycopy(data, 0, manyCopies, data.length * i, data.length);
        }

        final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>((var,content) -> new JsonRecordSource(content), timestampInference, Mockito.mock(ComponentLog.class));

        for (int j = 0; j < 10; j++) {
            final long start = System.nanoTime();

            for (int i = 0; i < 10_000; i++) {
                try (final InputStream in = new ByteArrayInputStream(manyCopies)) {
                    final RecordSchema schema = accessStrategy.getSchema(null, in, null);
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println(millis);
        }
    }

    @Test
    @Ignore
    public void testPerformanceOfSchemaInferenceWithoutTimestamp() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final byte[] data = Files.readAllBytes(file.toPath());
        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        final byte[] manyCopies = new byte[data.length * 20];
        for (int i=0; i < 20; i++) {
            System.arraycopy(data, 0, manyCopies, data.length * i, data.length);
        }

        for (int j = 0; j < 10; j++) {
            final long start = System.nanoTime();

            for (int i = 0; i < 10_000; i++) {
                try (final InputStream in = new ByteArrayInputStream(manyCopies)) {
                    final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>((var,content) -> new JsonRecordSource(content),
                         noTimestampInference, Mockito.mock(ComponentLog.class));

                    final RecordSchema schema = accessStrategy.getSchema(null, in, null);
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println(millis);
        }
    }

    @Test
    public void testInferenceIncludesAllRecords() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final RecordSchema schema = inferSchema(file);

        final RecordField extraField1 = schema.getField("extra field 1").get();
        assertSame(RecordFieldType.STRING, extraField1.getDataType().getFieldType());

        final RecordField extraField2 = schema.getField("extra field 2").get();
        assertSame(RecordFieldType.STRING, extraField2.getDataType().getFieldType());

        final RecordField updatedAttributesField = schema.getField("updatedAttributes").get();
        final DataType updatedAttributesDataType = updatedAttributesField.getDataType();
        assertSame(RecordFieldType.RECORD, updatedAttributesDataType.getFieldType());

        final List<String> expectedAttributeNames = Arrays.asList("path", "filename", "drop reason", "uuid", "reporting.task.type", "s2s.address", "schema.cache.identifier", "reporting.task.uuid",
            "record.count", "s2s.host", "reporting.task.transaction.id", "reporting.task.name", "mime.type");

        final RecordSchema updatedAttributesSchema = ((RecordDataType) updatedAttributesDataType).getChildSchema();
        assertEquals(expectedAttributeNames.size(), updatedAttributesSchema.getFieldCount());

        for (final String attributeName : expectedAttributeNames) {
            assertSame(RecordFieldType.STRING, updatedAttributesSchema.getDataType(attributeName).get().getFieldType());
        }
    }

    @Test
    public void testDateAndTimestampsInferred() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final RecordSchema schema = inferSchema(file);

        final RecordField timestampField = schema.getField("timestamp").get();
        assertEquals(RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), timestampField.getDataType());

        final RecordField dateField = schema.getField("eventDate").get();
        assertEquals(RecordFieldType.DATE.getDataType("yyyy-MM-dd"), dateField.getDataType());

        final RecordField timeField = schema.getField("eventTime").get();
        assertEquals(RecordFieldType.TIME.getDataType("HH:mm:ss"), timeField.getDataType());

        // TIME value and a STRING should be inferred as a STRING field
        final RecordField maybeTimeField = schema.getField("maybeTime").get();
        assertEquals(RecordFieldType.STRING, maybeTimeField.getDataType().getFieldType());

        // DATE value and a null value should be inferred as a DATE field
        final RecordField maybeDateField = schema.getField("maybeDate").get();
        assertEquals(RecordFieldType.DATE.getDataType("yyyy-MM-dd"), maybeDateField.getDataType());
    }

    /**
     * Test is intended to ensure that all inference rules that are explained in the readers' additionalDetails.html are correct
     */
    @Test
    public void testDocsExample() throws IOException {
        final File file = new File("src/test/resources/json/docs-example.json");
        final RecordSchema schema = inferSchema(file);

        assertSame(RecordFieldType.STRING, schema.getDataType("name").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("age").get().getFieldType());

        final DataType valuesDataType = schema.getDataType("values").get();
        assertSame(RecordFieldType.CHOICE, valuesDataType.getFieldType());

        final ChoiceDataType valuesChoiceType = (ChoiceDataType) valuesDataType;
        final List<DataType> possibleTypes = valuesChoiceType.getPossibleSubTypes();
        assertEquals(2, possibleTypes.size());
        assertTrue(possibleTypes.contains(RecordFieldType.STRING.getDataType()));
        assertTrue(possibleTypes.contains(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));

        assertSame(RecordFieldType.STRING, schema.getDataType("nullValue").get().getFieldType());
    }

    private RecordSchema inferSchema(final File file) throws IOException {
        try (final InputStream in = new FileInputStream(file);
             final InputStream bufferedIn = new BufferedInputStream(in)) {

            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>((var,content) -> new JsonRecordSource(content),
                timestampInference, Mockito.mock(ComponentLog.class));

            return accessStrategy.getSchema(null, bufferedIn, null);
        }
    }
}
