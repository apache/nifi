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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class TestExtractRecordSchema {

    final static Path NAME_AGE_SCHEMA_PATH = Paths.get("src/test/resources/TestExtractRecordSchema/name_age_schema.avsc");

    @Test
    public void testSuccessfulExtraction() throws Exception {
        final MockRecordParser readerService = new MockRecordParser();
        final TestRunner runner = TestRunners.newTestRunner(ExtractRecordSchema.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(ExtractRecordSchema.RECORD_READER, "reader");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractRecordSchema.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ExtractRecordSchema.REL_SUCCESS).get(0);

        final String expectedAttributeValue = new String(Files.readAllBytes(NAME_AGE_SCHEMA_PATH));
        out.assertAttributeEquals(ExtractRecordSchema.SCHEMA_ATTRIBUTE_NAME, expectedAttributeValue);
    }

    @Test
    public void testNoSchema() throws Exception {
        final MockRecordParser readerService = new MockRecordParserSchemaNotFound();
        final TestRunner runner = TestRunners.newTestRunner(ExtractRecordSchema.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(ExtractRecordSchema.RECORD_READER, "reader");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractRecordSchema.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ExtractRecordSchema.REL_FAILURE).get(0);

        out.assertAttributeEquals("record.error.message", "org.apache.nifi.schema.access.SchemaNotFoundException Thrown");
    }

    private static class MockRecordParserSchemaNotFound extends MockRecordParser {
        @Override
        public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws SchemaNotFoundException {
            throw new SchemaNotFoundException("test");
        }
    }
}