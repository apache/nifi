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
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class TestScriptedPartitionRecord extends TestScriptedRouterProcessor {
    private static final String SCRIPT =
        "if (record.getValue(\"first\") == 1) {\n" +
        "   return \"partition1\";\n" +
        "} else if (record.getValue(\"first\") == 2) {\n" +
        "   return \"partition2\";\n" +
        "} else {\n" +
        "   return \"partition3\";\n" +
        "}\n";

    private static final Object[] PARTITION_1_RECORD_1 = new Object[] {1, "lorem"};
    private static final Object[] PARTITION_1_RECORD_2 = new Object[] {1, "ipsum"};
    private static final Object[] PARTITION_2_RECORD_1 = new Object[] {2, "lorem"};
    private static final Object[] PARTITION_3_RECORD_1 = new Object[] {3, "lorem"};
    private static final String PARTITION_1 = "partition1";
    private static final String PARTITION_2 = "partition2";
    private static final String PARTITION_3 = "partition3";
    private static final String RELATIONSHIP_1 = "relationship1";
    private static final String RELATIONSHIP_2 = "relationship2";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testRunner.setProperty(PARTITION_1, RELATIONSHIP_1);
        testRunner.setProperty(PARTITION_2, RELATIONSHIP_2);
    }

    @Test
    public void testIncomingFlowFilesContainsRecordForOneRoute() throws Exception {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_1_RECORD_2);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteContains(RELATIONSHIP_1, new Object[][]{PARTITION_1_RECORD_1, PARTITION_1_RECORD_2});
        thenGivenRouteIsEmpty(RELATIONSHIP_2);
        thenNonMatchingIsEmpty();
    }

    @Test
    public void testIncomingFlowFilesContainsRecordForMultipleRoutes() throws Exception {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_2_RECORD_1);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteContains(RELATIONSHIP_1, new Object[][]{PARTITION_1_RECORD_1});
        thenGivenRouteContains(RELATIONSHIP_2, new Object[][]{PARTITION_2_RECORD_1});
        thenNonMatchingIsEmpty();
    }

    @Test
    public void testIncomingFlowFilesContainsNonMatchingRecord() throws Exception {
        // given
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_2_RECORD_1);
        recordReader.addRecord(PARTITION_3_RECORD_1);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteContains(RELATIONSHIP_1, new Object[][]{PARTITION_1_RECORD_1});
        thenGivenRouteContains(RELATIONSHIP_2, new Object[][]{PARTITION_2_RECORD_1});
        thenNonMatchingContains(new Object[][]{PARTITION_3_RECORD_1});
    }

    @Test
    public void testIncomingFlowFilesContainsOnlyNonMatchingRecord() throws Exception {
        // given
        recordReader.addRecord(PARTITION_3_RECORD_1);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteIsEmpty(RELATIONSHIP_1);
        thenGivenRouteIsEmpty(RELATIONSHIP_2);
        thenNonMatchingContains(new Object[][]{PARTITION_3_RECORD_1});
    }

    @Test
    public void testIncomingFlowFileContainsNoRecords() throws Exception {
        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteIsEmpty(RELATIONSHIP_1);
        thenGivenRouteIsEmpty(RELATIONSHIP_2);
        thenNonMatchingIsEmpty();
    }

    @Test
    public void testIncomingFlowFileCannotBeRead() throws Exception {
        // given
        recordReader.failAfter(0);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToFailed();
        thenGivenRouteIsEmpty(RELATIONSHIP_1);
        thenGivenRouteIsEmpty(RELATIONSHIP_2);
        thenNonMatchingIsEmpty();
    }

    @Test
    public void testMultiplePartitionPointToTheSameRelationship() throws Exception {
        // given
        testRunner.setProperty(PARTITION_3, RELATIONSHIP_1);
        recordReader.addRecord(PARTITION_1_RECORD_1);
        recordReader.addRecord(PARTITION_3_RECORD_1);

        // when
        whenTriggerProcessor();

        // then
        thenIncomingFlowFileIsRoutedToOriginal();
        thenGivenRouteContains(RELATIONSHIP_1, new Object[][]{PARTITION_1_RECORD_1, PARTITION_3_RECORD_1});
        thenGivenRouteIsEmpty(RELATIONSHIP_2);
        thenNonMatchingIsEmpty();
    }

    @Test
    public void testPartitionPointsToStaticRelationship() {
        // given
        testRunner.setProperty(PARTITION_3, ScriptedPartitionRecord.RELATIONSHIP_FAILED.getName());

        // when
        testRunner.assertNotValid();
    }

    @Test
    public void testDynamicRelationshipsAreManagedProperly() {
        // The default test set up
        thenProcessorRelationshipsAre(RELATIONSHIP_1, RELATIONSHIP_2);

        // Adding additional partition pointing to relationship2
        testRunner.setProperty(PARTITION_3, RELATIONSHIP_2);
        thenProcessorRelationshipsAre(RELATIONSHIP_1, RELATIONSHIP_2);

        // Removing additional partition pointing to relationship2 - should not remove relationship2
        testRunner.removeProperty(PARTITION_3);
        thenProcessorRelationshipsAre(RELATIONSHIP_1, RELATIONSHIP_2);

        // Removing the remaining partition pointing to relationship2 - relationship2 should be removed
        testRunner.removeProperty(PARTITION_2);
        thenProcessorRelationshipsAre(RELATIONSHIP_1);

        // Adding a partition pointing to relationship2 - should add the relationship back
        testRunner.setProperty(PARTITION_2, RELATIONSHIP_2);
        thenProcessorRelationshipsAre(RELATIONSHIP_1, RELATIONSHIP_2);
    }

    private void thenGivenRouteIsEmpty(final String route) {
        testRunner.assertTransferCount(route, 0);
    }

    private void thenGivenRouteContains(final String route, final Object[][] records) {
        testRunner.assertTransferCount(route, 1);
        final MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(route).get(0);
        Assert.assertEquals(givenExpectedFlowFile(records), resultFlowFile.getContent());
        Assert.assertEquals("text/plain", resultFlowFile.getAttribute("mime.type"));
    }

    private void thenNonMatchingContains(final Object[][] records) {
        testRunner.assertTransferCount(ScriptedPartitionRecord.RELATIONSHIP_UNMATCHED, 1);
        final MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(ScriptedPartitionRecord.RELATIONSHIP_UNMATCHED).get(0);
        Assert.assertEquals(givenExpectedFlowFile(records), resultFlowFile.getContent());
        Assert.assertEquals("text/plain", resultFlowFile.getAttribute("mime.type"));
    }

    private void thenNonMatchingIsEmpty() {
        testRunner.assertTransferCount(ScriptedPartitionRecord.RELATIONSHIP_UNMATCHED, 0);
    }

    private void thenProcessorRelationshipsAre(final String... dynamicRelationships) {
        final Set<String> relationshipNames = testRunner.getProcessor().getRelationships().stream().map(r -> r.getName()).collect(Collectors.toSet());

        Assert.assertEquals(dynamicRelationships.length + 3, relationshipNames.size());
        Assert.assertTrue(relationshipNames.contains(ScriptedPartitionRecord.RELATIONSHIP_ORIGINAL.getName()));
        Assert.assertTrue(relationshipNames.contains(ScriptedPartitionRecord.RELATIONSHIP_FAILED.getName()));
        Assert.assertTrue(relationshipNames.contains(ScriptedPartitionRecord.RELATIONSHIP_UNMATCHED.getName()));

        for (final String dynamicRelationship : dynamicRelationships) {
            Assert.assertTrue(relationshipNames.contains(dynamicRelationship));
        }
    }

    @Override
    protected Class<? extends Processor> givenProcessorType() {
        return ScriptedPartitionRecord.class;
    }

    @Override
    protected String getScript() {
        return SCRIPT;
    }

    @Override
    protected Relationship getOriginalRelationship() {
        return ScriptedPartitionRecord.RELATIONSHIP_ORIGINAL;
    }

    @Override
    protected Relationship getFailedRelationship() {
        return ScriptedPartitionRecord.RELATIONSHIP_FAILED;
    }
}
