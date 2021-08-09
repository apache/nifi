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
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.TestRecordReaderProcessor;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestReaderLookup {
    private final String DEFAULT_ATTRIBUTE_NAME = "recordreader.name";

    private MockRecordReaderFactory recordReaderA;
    private MockRecordReaderFactory recordReaderB;

    private ReaderLookup readerLookup;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        recordReaderA = new MockRecordReaderFactory("A");
        recordReaderB = new MockRecordReaderFactory("B");

        readerLookup = new ReaderLookup();
        runner = TestRunners.newTestRunner(TestRecordReaderProcessor.class);

        final String rrServiceAIdentifier = "rr-A";
        runner.addControllerService(rrServiceAIdentifier, recordReaderA);

        final String rrServiceBIdentifier = "rr-B";
        runner.addControllerService(rrServiceBIdentifier, recordReaderB);

        runner.addControllerService("rr-lookup", readerLookup);
        runner.setProperty(readerLookup, "A", rrServiceAIdentifier);
        runner.setProperty(readerLookup, "B", rrServiceBIdentifier);

        runner.enableControllerService(recordReaderA);
        runner.enableControllerService(recordReaderB);
        runner.enableControllerService(readerLookup);
    }

    @Test
    public void testLookupServiceByName() throws SchemaNotFoundException, MalformedRecordException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(DEFAULT_ATTRIBUTE_NAME, "A");

        MockRecordReader recordReader = (MockRecordReader) readerLookup.createRecordReader(attributes, null, -1, null);
        assertNotNull(recordReader);
        assertEquals(recordReaderA.name, recordReader.name);

        attributes.put(DEFAULT_ATTRIBUTE_NAME, "B");

        recordReader = (MockRecordReader) readerLookup.createRecordReader(attributes, null, -1, null);
        assertNotNull(recordReader);
        assertEquals(recordReaderB.name, recordReader.name);
    }

    @Test(expected = ProcessException.class)
    public void testLookupMissingNameAttribute() throws SchemaNotFoundException, MalformedRecordException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        readerLookup.createRecordReader(attributes, null, -1, null);
    }

    @Test(expected = ProcessException.class)
    public void testLookupWithNameThatDoesNotExist() throws SchemaNotFoundException, MalformedRecordException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        attributes.put(DEFAULT_ATTRIBUTE_NAME, "DOES-NOT-EXIST");
        readerLookup.createRecordReader(attributes, null, -1, null);
    }

    @Test
    public void testCustomValidateAtLeaseOneServiceDefined() throws InitializationException {
        // enable lookup service with no services registered, verify not valid
        runner = TestRunners.newTestRunner(TestRecordReaderProcessor.class);
        runner.addControllerService("rr-lookup", readerLookup);
        runner.assertNotValid(readerLookup);

        final String rrServiceAIdentifier = "rr-A";
        runner.addControllerService(rrServiceAIdentifier, recordReaderA);

        // register a service and now verify valid
        runner.setProperty(readerLookup, "A", rrServiceAIdentifier);
        runner.enableControllerService(readerLookup);
        runner.assertValid(readerLookup);
    }

    @Test
    public void testCustomValidateSelfReferenceNotAllowed() throws InitializationException {
        runner = TestRunners.newTestRunner(TestRecordReaderProcessor.class);
        runner.addControllerService("rr-lookup", readerLookup);
        runner.setProperty(readerLookup, "lookup", "lookup");
        runner.assertNotValid(readerLookup);
    }

    /**
     * A mock RecordReaderFactory that has a name for tracking purposes.
     */
    private static class MockRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

        private String name;

        public MockRecordReaderFactory(String name) {
            this.name = name;
        }

        @Override
        public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger)
                throws MalformedRecordException, IOException, SchemaNotFoundException {
            return new MockRecordReader(this.name);
        }
    }

    private static class MockRecordReader implements RecordReader {
        public String name;

        public MockRecordReader(String name) {
            this.name = name;
        }

        @Override
        public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
            return null;
        }

        @Override
        public RecordSchema getSchema() throws MalformedRecordException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}