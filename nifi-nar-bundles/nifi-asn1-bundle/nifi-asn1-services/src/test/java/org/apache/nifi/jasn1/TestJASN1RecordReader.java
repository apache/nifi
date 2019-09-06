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
package org.apache.nifi.jasn1;

import org.apache.nifi.jasn1.util.JASN1ReadRecordTester;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.MockComponentLog;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestJASN1RecordReader implements JASN1ReadRecordTester {

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.jasn1", "DEBUG");
    }

    @Test
    public void testBasicTypes() throws Exception {
        try (final InputStream input = TestJASN1RecordReader.class.getResourceAsStream("/examples/basic-types.dat")) {

            final JASN1RecordReader reader = new JASN1RecordReader("org.apache.nifi.jasn1.example.BasicTypes", null,
                new RecordSchemaProvider(), Thread.currentThread().getContextClassLoader(), null,
                input, new MockComponentLog("id", new JASN1Reader()));

            final RecordSchema schema = reader.getSchema();
            assertEquals("BasicTypes", schema.getSchemaName().orElse(null));

            Record record = reader.nextRecord(true, false);
            assertNotNull(record);

            assertEquals(true, record.getAsBoolean("b"));
            assertEquals(789, record.getAsInt("i").intValue());
            assertEquals("0102030405", record.getValue("octStr"));
            assertEquals("Some UTF-8 String. こんにちは世界。", record.getValue("utf8Str"));

            record = reader.nextRecord(true, false);
            assertNull(record);
        }
    }

    @Test
    public void testComposite() throws Exception {
        try (final InputStream input = TestJASN1RecordReader.class.getResourceAsStream("/examples/composite.dat")) {

            final JASN1RecordReader reader = new JASN1RecordReader("org.apache.nifi.jasn1.example.Composite", null,
                new RecordSchemaProvider(), Thread.currentThread().getContextClassLoader(), null,
                input, new MockComponentLog("id", new JASN1Reader()));

            final RecordSchema schema = reader.getSchema();
            assertEquals("Composite", schema.getSchemaName().orElse(null));

            Record record = reader.nextRecord(true, false);
            assertNotNull(record);

            // Assert child
            final Optional<RecordField> childSchema = schema.getField("child");
            assertTrue(childSchema.isPresent());
            Record child = record.getAsRecord("child", ((RecordDataType) childSchema.get().getDataType()).getChildSchema());
            assertNotNull(child);

            assertEquals(true, child.getAsBoolean("b"));
            assertEquals(789, child.getAsInt("i").intValue());
            assertEquals("0102030405", child.getValue("octStr"));

            // Assert children
            final Object[] children = record.getAsArray("children");
            assertEquals(3, children.length);
            for (int i = 0; i < children.length; i++) {
                child = (Record) children[i];
                assertEquals(i % 2 == 0, child.getAsBoolean("b"));
                assertEquals(i, child.getAsInt("i").intValue());
                assertEquals(octetStringExpectedValueConverter(new byte[]{(byte) i, (byte) i, (byte) i}), child.getValue("octStr"));
            }

            // Assert integers
            final Object[] numbers = (Object[]) record.getValue("numbers");
            assertEquals(4, numbers.length);
            assertEquals(new BigInteger("0"), numbers[0]);
            assertEquals(new BigInteger("1"), numbers[1]);
            assertEquals(new BigInteger("2"), numbers[2]);
            assertEquals(new BigInteger("3"), numbers[3]);

            // Assert unordered
            final Object[] unordered = record.getAsArray("unordered");
            assertEquals(2, unordered.length);
            for (int i = 0; i < unordered.length; i++) {
                child = (Record) unordered[i];
                assertEquals(i % 2 == 0, child.getAsBoolean("b"));
                assertEquals(i, child.getAsInt("i").intValue());
                assertEquals(octetStringExpectedValueConverter(new byte[]{(byte) i, (byte) i, (byte) i}), child.getValue("octStr"));
            }

            record = reader.nextRecord(true, false);
            assertNull(record);
        }
    }
}
