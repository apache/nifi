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
package org.apache.nifi.record.sink.lookup;


import org.apache.nifi.record.sink.TestProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestRecordSinkServiceLookup {

    private MockRecordSinkServiceLookup sinkLookup;
    private TestRunner runner;
    private MockRecordSinkService sinkA, sinkB;
    private RecordSet recordSet;

    @Before
    public void setup() throws InitializationException {
        sinkA = new MockRecordSinkService("a");
        sinkB = new MockRecordSinkService("b");
        sinkLookup = new MockRecordSinkServiceLookup();

        runner = TestRunners.newTestRunner(TestProcessor.class);

        final String sinkServiceAIdentifier = "a";
        runner.addControllerService(sinkServiceAIdentifier, sinkA);

        final String sinkServiceBIdentifier = "b";
        runner.addControllerService(sinkServiceBIdentifier, sinkB);

        runner.addControllerService("sink-lookup", sinkLookup);
        runner.setProperty(sinkLookup, "a", sinkServiceAIdentifier);
        runner.setProperty(sinkLookup, "b", sinkServiceBIdentifier);

        runner.enableControllerService(sinkA);
        runner.enableControllerService(sinkB);
        runner.enableControllerService(sinkLookup);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("a", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.BOOLEAN.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap1 = new HashMap<>();
        valueMap1.put("a", "Hello");
        valueMap1.put("b", true);
        final Record record1 = new MapRecord(schema, valueMap1);

        final Map<String, Object> valueMap2 = new HashMap<>();
        valueMap2.put("a", "World");
        valueMap2.put("b", false);
        final Record record2 = new MapRecord(schema, valueMap2);

        recordSet = RecordSet.of(schema, record1, record2);
    }

    @Test
    public void testLookupServiceA() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "a");

        try {
            final WriteResult writeResult = sinkLookup.sendData(recordSet, attributes, false);
            assertNotNull(writeResult);
            assertEquals(2, writeResult.getRecordCount());
            String returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("a", returnedName);
        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }
    }

    @Test
    public void testLookupServiceB() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "b");

        try {
            final WriteResult writeResult = sinkLookup.sendData(recordSet, attributes, false);
            assertNotNull(writeResult);
            assertEquals(2, writeResult.getRecordCount());
            String returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("b", returnedName);
        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }
    }

    @Test
    public void testLookupServiceAThenB() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "a");

        try {
            final WriteResult writeResult = sinkLookup.sendData(recordSet, attributes, false);
            String returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("a", returnedName);
            assertEquals(1, sinkA.getResetCount());
        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }

        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "b");

        try {
            final WriteResult writeResult = sinkLookup.sendData(recordSet, attributes, false);
            String returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("b", returnedName);
            assertEquals(1, sinkB.getResetCount());
        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }
        // reset() was called on the retrieved sinks (not the lookup itself) in sendData() when the sink changed
        assertEquals(0, sinkLookup.getResetCount());
        sinkLookup.reset();
        assertEquals(1, sinkLookup.getResetCount());
    }

    @Test
    public void testLookupServiceAThenA() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "a");

        try {
            WriteResult writeResult = sinkLookup.sendData(recordSet, attributes, false);
            String returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("a", returnedName);
            assertEquals(1, sinkA.getResetCount());
            writeResult = sinkLookup.sendData(recordSet, attributes, false);
            returnedName = writeResult.getAttributes().get("my.name");
            assertEquals("a", returnedName);
            assertEquals(1, sinkA.getResetCount());

        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }

        // reset() was called on the retrieved sinks (not the lookup itself) in sendData() when the sink changed
        assertEquals(0, sinkLookup.getResetCount());
        sinkLookup.reset();
        assertEquals(1, sinkLookup.getResetCount());
    }

    @Test(expected = IOException.class)
    public void testLookupWithoutAttributes() throws IOException {
        sinkLookup.sendData(recordSet, Collections.emptyMap(), false);
    }

    @Test(expected = IOException.class)
    public void testLookupMissingRecordSinkNameAttribute() throws IOException {
        sinkLookup.sendData(recordSet, Collections.emptyMap(), false);
    }

    @Test(expected = IOException.class)
    public void testLookupWithDatabaseNameThatDoesNotExist() throws IOException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE, "DOES-NOT-EXIST");
        sinkLookup.sendData(recordSet, attributes, false);
    }

    public static class MockRecordSinkServiceLookup extends RecordSinkServiceLookup {
        private int resetCount = 0;

        @Override
        public void reset() {
            resetCount++;
        }

        public int getResetCount() {
            return resetCount;
        }
    }
}