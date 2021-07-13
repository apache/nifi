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
package org.apache.nifi.lookup;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.TestRecordSetWriterProcessor;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestRecordSetWriterLookup {
    private final String DEFAULT_ATTRIBUTE_NAME = "recordsetwriter.name";

    private MockRecordSetWriterFactory recordSetWriterA;
    private MockRecordSetWriterFactory recordSetWriterB;

    private RecordSetWriterLookup recordSetWriterLookup;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        recordSetWriterA = new MockRecordSetWriterFactory("A");
        recordSetWriterB = new MockRecordSetWriterFactory("B");

        recordSetWriterLookup = new RecordSetWriterLookup();
        runner = TestRunners.newTestRunner(TestRecordSetWriterProcessor.class);

        final String rrServiceAIdentifier = "rr-A";
        runner.addControllerService(rrServiceAIdentifier, recordSetWriterA);

        final String rrServiceBIdentifier = "rr-B";
        runner.addControllerService(rrServiceBIdentifier, recordSetWriterB);

        runner.addControllerService("rr-lookup", recordSetWriterLookup);
        runner.setProperty(recordSetWriterLookup, "A", rrServiceAIdentifier);
        runner.setProperty(recordSetWriterLookup, "B", rrServiceBIdentifier);

        runner.enableControllerService(recordSetWriterA);
        runner.enableControllerService(recordSetWriterB);
        runner.enableControllerService(recordSetWriterLookup);
    }

    @Test
    public void testLookupServiceByName() throws SchemaNotFoundException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(DEFAULT_ATTRIBUTE_NAME, "A");

        RecordSchema recordSchema = recordSetWriterLookup.getSchema(attributes, null);
        assertNotNull(recordSchema);
        assertEquals(recordSetWriterA.name, recordSchema.getIdentifier().getName().get());

        MockRecordSetWriter writer = (MockRecordSetWriter) recordSetWriterLookup.createWriter(null, null, null, attributes);
        assertNotNull(writer);
        assertEquals(recordSetWriterA.name, writer.name);

        attributes.put(DEFAULT_ATTRIBUTE_NAME, "B");

        recordSchema = recordSetWriterLookup.getSchema(attributes, null);
        assertNotNull(recordSchema);
        assertEquals(recordSetWriterB.name, recordSchema.getIdentifier().getName().get());

        writer = (MockRecordSetWriter) recordSetWriterLookup.createWriter(null, null, null, attributes);
        assertNotNull(writer);
        assertEquals(recordSetWriterB.name, writer.name);
    }

    @Test(expected = ProcessException.class)
    public void testLookupMissingNameAttribute() throws SchemaNotFoundException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        recordSetWriterLookup.createWriter(null, null, null, attributes);
    }

    @Test(expected = ProcessException.class)
    public void testLookupSchemaMissingNameAttribute() throws SchemaNotFoundException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        recordSetWriterLookup.getSchema(attributes, null);
    }

    @Test(expected = ProcessException.class)
    public void testLookupWithNameThatDoesNotExist() throws SchemaNotFoundException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(DEFAULT_ATTRIBUTE_NAME, "DOES-NOT-EXIST");
        recordSetWriterLookup.createWriter(null, null, null, attributes);
    }

    @Test(expected = ProcessException.class)
    public void testLookupSchemaWithNameThatDoesNotExist() throws SchemaNotFoundException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(DEFAULT_ATTRIBUTE_NAME, "DOES-NOT-EXIST");
        recordSetWriterLookup.getSchema(attributes, null);
    }

    @Test
    public void testCustomValidateAtLeaseOneServiceDefined() throws InitializationException {
        // enable lookup service with no services registered, verify not valid
        runner = TestRunners.newTestRunner(TestRecordSetWriterProcessor.class);
        runner.addControllerService("rr-lookup", recordSetWriterLookup);
        runner.assertNotValid(recordSetWriterLookup);

        final String rrServiceAIdentifier = "rr-A";
        runner.addControllerService(rrServiceAIdentifier, recordSetWriterA);

        // register a service and now verify valid
        runner.setProperty(recordSetWriterLookup, "A", rrServiceAIdentifier);
        runner.enableControllerService(recordSetWriterLookup);
        runner.assertValid(recordSetWriterLookup);
    }

    @Test
    public void testCustomValidateSelfReferenceNotAllowed() throws InitializationException {
        runner = TestRunners.newTestRunner(TestRecordSetWriterProcessor.class);
        runner.addControllerService("rr-lookup", recordSetWriterLookup);
        runner.setProperty(recordSetWriterLookup, "lookup", "lookup");
        runner.assertNotValid(recordSetWriterLookup);
    }

    /**
     * A mock RecordSetWriterFactory that has a name for tracking purposes.
     */
    private static class MockRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {

        private String name;

        public MockRecordSetWriterFactory(String name) {
            this.name = name;
        }


        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
            return new SimpleRecordSchema(SchemaIdentifier.builder().name(name).build());
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out) throws SchemaNotFoundException, IOException {
            return new MockRecordSetWriter(name);
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) throws SchemaNotFoundException, IOException {
            return new MockRecordSetWriter(name);
        }
    }

    private static class MockRecordSetWriter implements RecordSetWriter {
        public String name;

        public MockRecordSetWriter(String name) {
            this.name = name;
        }


        @Override
        public WriteResult write(RecordSet recordSet) throws IOException {
            return null;
        }

        @Override
        public void beginRecordSet() throws IOException {

        }

        @Override
        public WriteResult finishRecordSet() throws IOException {
            return null;
        }

        @Override
        public WriteResult write(Record record) throws IOException {
            return null;
        }

        @Override
        public String getMimeType() {
            return null;
        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}