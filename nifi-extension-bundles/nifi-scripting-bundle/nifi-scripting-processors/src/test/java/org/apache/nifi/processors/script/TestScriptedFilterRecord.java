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
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestScriptedFilterRecord extends TestScriptedRouterProcessor {
    private static final String SCRIPT = "return record.getValue(\"first\") == 1";

    private static final Object[] MATCHING_RECORD_1 = new Object[] {1, "lorem"};
    private static final Object[] MATCHING_RECORD_2 = new Object[] {1, "ipsum"};
    private static final Object[] NON_MATCHING_RECORD_1 = new Object[] {2, "lorem"};
    private static final Object[] NON_MATCHING_RECORD_2 = new Object[] {2, "ipsum"};

    @Test
    public void testIncomingFlowFileContainsMatchingRecordsOnly() {
        // given
        recordReader.addRecord(MATCHING_RECORD_1);
        recordReader.addRecord(MATCHING_RECORD_2);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenMatchingFlowFileContains(MATCHING_RECORD_1, MATCHING_RECORD_2);
    }

    @Test
    public void testIncomingFlowFileContainsNonMatchingRecordsOnly() {
        // given
        recordReader.addRecord(NON_MATCHING_RECORD_1);
        recordReader.addRecord(NON_MATCHING_RECORD_2);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenMatchingFlowFileIsEmpty();
    }

    @Test
    public void testIncomingFlowFileContainsMatchingAndNonMatchingRecords() {
        // given
        recordReader.addRecord(MATCHING_RECORD_1);
        recordReader.addRecord(NON_MATCHING_RECORD_1);
        recordReader.addRecord(MATCHING_RECORD_2);
        recordReader.addRecord(NON_MATCHING_RECORD_2);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenMatchingFlowFileContains(MATCHING_RECORD_1, MATCHING_RECORD_2);
    }

    @Test
    public void testIncomingFlowFileContainsNoRecords() {
        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenMatchingFlowFileIsEmpty();
    }

    @Test
    public void testIncomingFlowFileCannotBeRead() {
        // given
        recordReader.failAfter(0);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToFailed();
        thenMatchingFlowFileIsEmpty();
    }

    private void thenMatchingFlowFileContains(final Object[]... records) {
        testRunner.assertTransferCount(ScriptedFilterRecord.RELATIONSHIP_SUCCESS, 1);
        final MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(ScriptedFilterRecord.RELATIONSHIP_SUCCESS).getFirst();
        assertEquals(givenExpectedFlowFile(records), resultFlowFile.getContent());
        assertEquals("text/plain", resultFlowFile.getAttribute("mime.type"));

    }

    private void thenMatchingFlowFileIsEmpty() {
        testRunner.assertTransferCount(ScriptedFilterRecord.RELATIONSHIP_SUCCESS, 0);
    }

    @Override
    protected Class<? extends Processor> givenProcessorType() {
        return ScriptedFilterRecord.class;
    }

    @Override
    protected String getScriptBody() {
        return SCRIPT;
    }

    @Override
    protected Relationship getOriginalRelationship() {
        return ScriptedFilterRecord.RELATIONSHIP_ORIGINAL;
    }

    @Override
    protected Relationship getFailedRelationship() {
        return ScriptedFilterRecord.RELATIONSHIP_FAILURE;
    }
}
