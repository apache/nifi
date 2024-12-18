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
package org.apache.nifi.processors.script;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class TestScriptedRouterProcessor {
    private static final String HEADER = "header§";

    protected TestRunner testRunner;
    protected MockRecordParser recordReader;
    protected MockRecordWriter recordWriter;
    protected String incomingFlowFileContent;

    @BeforeEach
    public void setUp() throws Exception {
        testRunner = TestRunners.newTestRunner(givenProcessorType());

        testRunner.setProperty(ScriptedTransformRecord.RECORD_READER, "record-reader");
        testRunner.setProperty(ScriptedTransformRecord.RECORD_WRITER, "record-writer");

        recordReader = new MockRecordParser();
        recordReader.addSchemaField("first", RecordFieldType.INT);
        recordReader.addSchemaField("second", RecordFieldType.STRING);

        recordWriter = new MockRecordWriter(HEADER);

        testRunner.addControllerService("record-reader", recordReader);
        testRunner.addControllerService("record-writer", recordWriter);

        if (getScriptBody() != null) {
            testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_FILE);
            testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getScriptBody());
        } else if (getScriptFile() != null) {
            testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_BODY);
            testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, getScriptFile());
        }

        testRunner.setProperty(ScriptedTransformRecord.LANGUAGE, "Groovy");

        testRunner.enableControllerService(recordReader);
        testRunner.enableControllerService(recordWriter);

        incomingFlowFileContent = UUID.randomUUID().toString();
    }

    protected void whenTriggerProcessor() {
        testRunner.enqueue(incomingFlowFileContent);
        testRunner.run();
    }

    protected void thenIncomingFlowFileIsRoutedToOriginal() {
        testRunner.assertTransferCount(getOriginalRelationship(), 1);
        testRunner.assertTransferCount(getFailedRelationship(), 0);
        assertEquals(incomingFlowFileContent, testRunner.getFlowFilesForRelationship(getOriginalRelationship()).getFirst().getContent());
    }

    protected void thenIncomingFlowFileIsRoutedToFailed() {
        testRunner.assertTransferCount(getOriginalRelationship(), 0);
        testRunner.assertTransferCount(getFailedRelationship(), 1);
        assertEquals(incomingFlowFileContent, testRunner.getFlowFilesForRelationship(getFailedRelationship()).getFirst().getContent());
    }

    /**
     * Generates the expected flow file content based on the records. Results the same format as the {@code MockRecordWriter} uses.
     */
    protected String givenExpectedFlowFile(final Object[]... records) {
        final StringBuilder expectedFlowFile = new StringBuilder(HEADER).append('\n');

        for (final Object[] record : records) {
            for (int i = 0; i < record.length; i++) {
                expectedFlowFile.append('"').append(record[i].toString()).append('"');

                if (i < record.length - 1) {
                    expectedFlowFile.append(',');
                }
            }

            expectedFlowFile.append('\n');
        }

        return expectedFlowFile.toString();
    }

    protected abstract Class<? extends Processor> givenProcessorType();

    protected abstract Relationship getOriginalRelationship();

    protected abstract Relationship getFailedRelationship();

    protected String getScriptBody() {
        return null;
    }

    protected String getScriptFile() {
        return null;
    }
}
