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

package org.apache.nifi.processors.standard;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestLookupRecord {

    private TestRunner runner;
    private MapLookup lookupService;
    private MockRecordParser recordReader;
    private MockRecordWriter recordWriter;

    @Before
    public void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        recordWriter = new MockRecordWriter(null, false);
        lookupService = new MapLookup();

        runner = TestRunners.newTestRunner(LookupRecord.class);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        runner.setProperty(LookupRecord.RECORD_READER, "reader");
        runner.setProperty(LookupRecord.RECORD_WRITER, "writer");
        runner.setProperty(LookupRecord.LOOKUP_SERVICE, "lookup");
        runner.setProperty(LookupRecord.LOOKUP_RECORD_PATH, "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/sport");
        runner.setProperty(LookupRecord.ROUTING_STRATEGY, LookupRecord.ROUTE_TO_MATCHED_UNMATCHED);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, null);
        recordReader.addRecord("Jane Doe", 47, null);
        recordReader.addRecord("Jimmy Doe", 14, null);
    }

    @Test
    public void testAllMatch() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,Soccer\nJane Doe,47,Basketball\nJimmy Doe,14,Football\n");
    }

    @Test
    public void testAllUnmatched() throws InitializationException {
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testMixtureOfMatch() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(LookupRecord.REL_FAILURE, 0);
        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);

        final MockFlowFile matched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        matched.assertAttributeEquals("record.count", "2");
        matched.assertAttributeEquals("mime.type", "text/plain");
        matched.assertContentEquals("John Doe,48,Soccer\nJimmy Doe,14,Football\n");

        final MockFlowFile unmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        unmatched.assertAttributeEquals("record.count", "1");
        unmatched.assertAttributeEquals("mime.type", "text/plain");
        unmatched.assertContentEquals("Jane Doe,47,\n");
    }


    @Test
    public void testResultPathNotFound() throws InitializationException {
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/other");

        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testLookupPathNotFound() throws InitializationException {
        runner.setProperty(LookupRecord.LOOKUP_RECORD_PATH, "/other");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testUnparseableData() throws InitializationException {
        recordReader.failAfter(1);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_FAILURE).get(0);
        out.assertAttributeNotExists("record.count");
        out.assertContentEquals("");
    }

    @Test
    public void testNoResultPath() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.removeProperty(LookupRecord.RESULT_RECORD_PATH);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }


    @Test
    public void testMultipleLookupPaths() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.setProperty(LookupRecord.LOOKUP_RECORD_PATH, "/*");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }



    private static class MapLookup extends AbstractControllerService implements StringLookupService {
        private final Map<String, String> values = new HashMap<>();

        public void addValue(final String key, final String value) {
            values.put(key, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Optional<String> lookup(final String key) {
            return Optional.ofNullable(values.get(key));
        }
    }

}
