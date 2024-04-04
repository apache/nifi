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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestScriptedPartitionRecord extends TestScriptedRouterProcessor {
    private static final String PARTITION_ATTRIBUTE = "partition";
    private static final Object[] PARTITION_1_RECORD_1 = new Object[] {1, "lorem"};
    private static final Object[] PARTITION_1_RECORD_2 = new Object[] {1, "ipsum"};
    private static final Object[] PARTITION_2_RECORD_1 = new Object[] {2, "lorem"};
    private static final Object[] PARTITION_3_RECORD_1 = new Object[] {3, "lorem"};
    private static final Object[] PARTITION_4_RECORD_1 = new Object[] {4, "lorem"};
    private static final String PARTITION_1 = "partition1";
    private static final String PARTITION_2 = "partition2";
    private static final Integer PARTITION_3 = 3;
    private static final String PARTITION_4 = "<null partition>";

    @Test
    public void testIncomingFlowFileContainsNoRecords() {
        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenNoPartitionExists();
    }

    @Test
    public void testWhenSinglePartitionAndSingleRecord() {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);

        // when
        whenTriggerProcessor();
        thenIncomingFlowFileIsRoutedToOriginal();

        // then
        thenTheFollowingPartitionsExists(PARTITION_1);
        thenPartitionContains(PARTITION_1, 0, 1, PARTITION_1_RECORD_1);
    }

    @Test
    public void testWhenSinglePartitionAndMultipleRecords() {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_1_RECORD_2);

        // when
        whenTriggerProcessor();
        thenIncomingFlowFileIsRoutedToOriginal();

        // then
        thenTheFollowingPartitionsExists(PARTITION_1);
        thenPartitionContains(PARTITION_1, 0, 1, PARTITION_1_RECORD_1, PARTITION_1_RECORD_2);
    }

    @Test
    public void testWhenMultiplePartitions() {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_1_RECORD_2);
        recordReader.addRecord(PARTITION_2_RECORD_1);

        // when
        whenTriggerProcessor();
        thenIncomingFlowFileIsRoutedToOriginal();

        // then
        thenTheFollowingPartitionsExists(PARTITION_1, PARTITION_2);
        thenPartitionContains(PARTITION_2, 0, 2, PARTITION_2_RECORD_1);
        thenPartitionContains(PARTITION_1, 1, 2, PARTITION_1_RECORD_1, PARTITION_1_RECORD_2);
    }

    @Test
    public void testWhenPartitionIsNotString() {
        // given
        recordReader.addRecord(PARTITION_4_RECORD_1);

        // when
        whenTriggerProcessor();
        thenIncomingFlowFileIsRoutedToOriginal();

        // then
        thenTheFollowingPartitionsExists(PARTITION_4);
        thenPartitionContains(PARTITION_4, 0, 1, PARTITION_4_RECORD_1);
    }

    @Test
    public void testWhenPartitionIsNull() {
        // given
        recordReader.addRecord(PARTITION_3_RECORD_1);

        // when
        whenTriggerProcessor();
        thenIncomingFlowFileIsRoutedToOriginal();

        // then
        thenTheFollowingPartitionsExists(PARTITION_3.toString());
        thenPartitionContains(PARTITION_3.toString(), 0, 1, PARTITION_3_RECORD_1);
    }

    private void thenNoPartitionExists() {
        assertEquals(0, testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS).size());
    }

    private void thenTheFollowingPartitionsExists(final String... partitions) {
        final List<MockFlowFile> outgoingFlowFiles = testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS);

        assertEquals(partitions.length, outgoingFlowFiles.size());

        final Set<String> outgoingPartitions = outgoingFlowFiles.stream().map(ff -> ff.getAttribute(PARTITION_ATTRIBUTE)).collect(Collectors.toSet());

        for (final String partition : partitions) {
            assertTrue(outgoingPartitions.contains(partition));
        }
    }

    private void thenPartitionContains(final String partition, int index, int count, final Object[]... records) {
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS);
        Set<MockFlowFile> outgoingFlowFiles = new HashSet<>();

        for (final MockFlowFile flowFile : flowFiles) {
            // If the partition is deliberately <code>null</code>, we also check if the attribute is added to the collection of attributes.
            // This is in order to differentiate from the situation where the "partition" attribute is not added at all.
            if (partition == null && flowFile.getAttributes().containsKey(PARTITION_ATTRIBUTE) && flowFile.getAttribute(PARTITION_ATTRIBUTE) == null) {
                outgoingFlowFiles.add(flowFile);
            } else if (flowFile.getAttribute(PARTITION_ATTRIBUTE).equals(partition)) {
                outgoingFlowFiles.add(flowFile);
            }
        }

        assertEquals(1, outgoingFlowFiles.size());
        final MockFlowFile resultFlowFile = outgoingFlowFiles.iterator().next();
        assertEquals(givenExpectedFlowFile(records), resultFlowFile.getContent());
        assertEquals("text/plain", resultFlowFile.getAttribute("mime.type"));
        assertEquals(String.valueOf(index), resultFlowFile.getAttribute("fragment.index"));
        assertEquals(String.valueOf(count), resultFlowFile.getAttribute("fragment.count"));


    }

    @Override
    protected Class<? extends Processor> givenProcessorType() {
        return ScriptedPartitionRecord.class;
    }

    @Override
    protected String getScriptFile() {
        return "src/test/resources/groovy/test_scripted_partition_record.groovy";
    }

    @Override
    protected Relationship getOriginalRelationship() {
        return ScriptedPartitionRecord.RELATIONSHIP_ORIGINAL;
    }

    @Override
    protected Relationship getFailedRelationship() {
        return ScriptedPartitionRecord.RELATIONSHIP_FAILURE;
    }
}
