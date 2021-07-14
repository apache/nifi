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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TestScriptedPartitionRecord extends TestScriptedRouterProcessor {
    private static final Object[] PARTITION_1_RECORD_1 = new Object[] {1, "lorem"};
    private static final Object[] PARTITION_1_RECORD_2 = new Object[] {1, "ipsum"};
    private static final Object[] PARTITION_2_RECORD_1 = new Object[] {2, "lorem"};
    private static final String PARTITION_1 = "partition1";
    private static final String PARTITION_2 = "partition2";

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

    private void thenNoPartitionExists() {
        Assert.assertEquals(0, testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS).size());
    }

    private void thenTheFollowingPartitionsExists(final String... partitions) {
        final List<MockFlowFile> outgoingFlowFiles = testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS);

        Assert.assertEquals(partitions.length, outgoingFlowFiles.size());

        final Set<String> outgoingPartitions = outgoingFlowFiles.stream().map(ff -> ff.getAttribute("partition")).collect(Collectors.toSet());

        for (final String partition : partitions) {
            Assert.assertTrue(outgoingPartitions.contains(partition));
        }
    }

    private void thenPartitionContains(final String partition, int index, int count, final Object[]... records) {
        final Set<MockFlowFile> outgoingFlowFiles = testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_SUCCESS)
                .stream().filter(ff -> ff.getAttribute("partition").equals(partition)).collect(Collectors.toSet());

        Assert.assertEquals(1, outgoingFlowFiles.size());
        final MockFlowFile resultFlowFile = outgoingFlowFiles.iterator().next();
        Assert.assertEquals(givenExpectedFlowFile(records), resultFlowFile.getContent());
        Assert.assertEquals("text/plain", resultFlowFile.getAttribute("mime.type"));
        Assert.assertEquals(String.valueOf(index), resultFlowFile.getAttribute("fragment.index"));
        Assert.assertEquals(String.valueOf(count), resultFlowFile.getAttribute("fragment.count"));


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
