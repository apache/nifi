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
package org.apache.nifi.xml;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.xml.inference.XmlNode;
import org.apache.nifi.xml.inference.XmlRecordSource;
import org.apache.nifi.xml.inference.XmlSchemaInference;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInferXmlSchema {

    private final TimeValueInference timeValueInference = new TimeValueInference("MM/dd/yyyy", "HH:mm:ss", "MM/dd/yyyy HH:mm:ss.SSS");

    @Test
    public void testFlatXml() throws IOException {
        final RecordSchema schema = inferSchema("src/test/resources/xml/person.xml", false);

        assertSame(RecordFieldType.STRING, schema.getDataType("NAME").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("AGE").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("COUNTRY").get().getFieldType());

        assertEquals(RecordFieldType.DATE.getDataType(timeValueInference.getDateFormat()), schema.getDataType("DOB").get());
        assertEquals(RecordFieldType.TIME.getDataType(timeValueInference.getTimeFormat()), schema.getDataType("TOB").get());
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(timeValueInference.getTimestampFormat()), schema.getDataType("TSOB").get());
    }

    @Test
    public void testFieldsFromAllRecordsIncluded() throws IOException {
        final RecordSchema schema = inferSchema("src/test/resources/xml/people_nested.xml", true);

        assertSame(RecordFieldType.STRING, schema.getDataType("ID").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("NAME").get().getFieldType());
        assertSame(RecordFieldType.INT, schema.getDataType("AGE").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("COUNTRY").get().getFieldType());

        assertEquals(RecordFieldType.DATE.getDataType(timeValueInference.getDateFormat()), schema.getDataType("DOB").get());
        assertEquals(
                RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.TIME.getDataType("HH:mm:ss"),
                        RecordFieldType.STRING.getDataType()
                ),
                schema.getDataType("TOB").get()
        );
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(timeValueInference.getTimestampFormat()), schema.getDataType("TSOB").get());

        final DataType addressDataType = schema.getDataType("ADDRESS").get();
        final RecordSchema addressSchema = ((RecordDataType) addressDataType).getChildSchema();

        assertSame(RecordFieldType.STRING, addressSchema.getDataType("STREET").get().getFieldType());
        assertSame(RecordFieldType.STRING, addressSchema.getDataType("CITY").get().getFieldType());
        assertSame(RecordFieldType.STRING, addressSchema.getDataType("STATE").get().getFieldType());
    }

    @Test
    public void testStringFieldWithAttributes() throws IOException {
        final RecordSchema schema = inferSchema("src/test/resources/xml/TextNodeWithAttribute.xml", true);
        assertSame(RecordFieldType.INT, schema.getDataType("num").get().getFieldType());
        assertSame(RecordFieldType.STRING, schema.getDataType("name").get().getFieldType());

        final DataType softwareDataType = schema.getDataType("software").get();
        assertSame(RecordFieldType.RECORD, softwareDataType.getFieldType());
        assertTrue(softwareDataType instanceof RecordDataType);

        final RecordSchema childSchema = ((RecordDataType) softwareDataType).getChildSchema();
        assertSame(RecordFieldType.BOOLEAN, childSchema.getDataType("favorite").get().getFieldType());
        assertSame(RecordFieldType.STRING, childSchema.getDataType("value").get().getFieldType());
    }

    private RecordSchema inferSchema(final String filename, final boolean ignoreWrapper) throws IOException {
        final File file = new File(filename);
        final RecordSourceFactory<XmlNode> xmlSourceFactory = (var, in) ->  new XmlRecordSource(in, ignoreWrapper);
        final SchemaInferenceEngine<XmlNode> schemaInference = new XmlSchemaInference(timeValueInference);
        final InferSchemaAccessStrategy<XmlNode> inferStrategy = new InferSchemaAccessStrategy<>(xmlSourceFactory, schemaInference, Mockito.mock(ComponentLog.class));

        final RecordSchema schema;
        try (final InputStream fis = new FileInputStream(file);
             final InputStream in = new BufferedInputStream(fis)) {
            return inferStrategy.getSchema(Collections.emptyMap(), in, null);
        }
    }
}
