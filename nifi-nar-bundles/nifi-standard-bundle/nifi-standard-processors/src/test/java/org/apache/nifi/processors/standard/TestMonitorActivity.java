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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestMonitorActivity {

    @Test
    public void testFirstMessage() throws InterruptedException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(1000L));
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        runner.clearTransferState();

        Thread.sleep(1000L);

        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        // ensure we don't keep creating the message
        for (int i = 0; i < 10; i++) {
            runNext(runner);
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            Thread.sleep(100L);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");
        Thread.sleep(200L);

        for (int i = 0; i < 10; i++) {
            runNext(runner);
            Thread.sleep(200L);
        }

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 10);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);

        restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");
    }

    private void runNext(TestRunner runner) {
        // Don't initialize, otherwise @OnScheduled is called and state gets reset
        runner.run(1, false, false);
    }

    @Test
    public void testFirstMessageWithInherit() throws InterruptedException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(1000L));
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS).get(0);
        runner.clearTransferState();

        Thread.sleep(1000L);

        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        // ensure we don't keep creating the message
        for (int i = 0; i < 10; i++) {
            runNext(runner);
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            Thread.sleep(100L);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");

        // verify the UUIDs are not the same
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertTrue(
                String.format("file sizes match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getSize(), restoredFlowFile.getSize()), restoredFlowFile.getSize() != originalFlowFile.getSize());
        Assert.assertTrue(
                String.format("lineage start dates match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getLineageStartDate(), restoredFlowFile.getLineageStartDate()), restoredFlowFile.getLineageStartDate() != originalFlowFile.getLineageStartDate());

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");
        Thread.sleep(200L);

        for (int i = 0; i < 10; i++) {
            runNext(runner);
            Thread.sleep(200L);
        }

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 10);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);

        restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertTrue(
                String.format("file sizes match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getSize(), restoredFlowFile.getSize()), restoredFlowFile.getSize() != originalFlowFile.getSize());
        Assert.assertTrue(
                String.format("lineage start dates match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getLineageStartDate(), restoredFlowFile.getLineageStartDate()), restoredFlowFile.getLineageStartDate() != originalFlowFile.getLineageStartDate());
    }

    @Test(timeout=5000)
    public void testFirstRunNoMessages() throws InterruptedException, IOException {
        // don't use the TestableProcessor, we want the real timestamp from @OnScheduled
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity());
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        int threshold = 100;
        boolean rerun = false;
        do {
            rerun = false;
            runner.setProperty(MonitorActivity.THRESHOLD, threshold + " millis");

            Thread.sleep(1000L);

            // shouldn't generate inactivity b/c run() will reset the lastSuccessfulTransfer if @OnSchedule & onTrigger
            // does not  get called more than MonitorActivity.THRESHOLD apart
            runner.run();
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            List<MockFlowFile> inactiveFlowFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_INACTIVE);
            if (inactiveFlowFiles.size() == 1) {
                // Seems Threshold was not sufficient, which has caused One inactive message.
                // Step-up and rerun the test until successful or jUnit times out
                threshold += threshold;
                rerun = true;
            } else {
                runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            }
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            runner.clearTransferState();
        }
        while(rerun);
    }

    /**
     * Since each call to run() will call @OnScheduled methods which will set the lastSuccessfulTransfer to the
     * current time, we need a way to create an artificial time difference between calls to run.
     */
    private class TestableProcessor extends MonitorActivity {

        private final long timestampDifference;

        public TestableProcessor(final long timestampDifference) {
            this.timestampDifference = timestampDifference;
        }

        @Override
        public void resetLastSuccessfulTransfer() {
            setLastSuccessfulTransfer(System.currentTimeMillis() - timestampDifference);
        }
    }

    @Test
    public void testClusterMonitorInvalidReportingNode() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));

        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);

        runner.assertNotValid();
    }

    @Test
    public void testClusterMonitorActive() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull("Latest timestamp should be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        // Should be null because COPY_ATTRIBUTES is null.
        assertNull(updatedState.get("key1"));
        assertNull(updatedState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveFallbackToNodeScope() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));
        runner.setClustered(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNull("Latest timestamp should NOT be persisted, because it's running as 'node' scope",
                updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
    }

    @Test
    public void testClusterMonitorActiveWithLatestTimestamp() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        // Set future timestamp in state
        final HashMap<String, String> existingState = new HashMap<>();
        final long existingTimestamp = System.currentTimeMillis() - 1_000;
        existingState.put(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER,
                String.valueOf(existingTimestamp));
        existingState.put("key1", "value1");
        existingState.put("key2", "value2");
        runner.getStateManager().setState(existingState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), existingState, Scope.CLUSTER);

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap postProcessedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertTrue("Existing timestamp should be updated",
                existingTimestamp < Long.parseLong(postProcessedState.get(
                        MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER)));
        // State should be updated. Null in this case.
        assertNull(postProcessedState.get("key1"));
        assertNull(postProcessedState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveMoreRecentTimestampExisted() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        // Set future timestamp in state
        final HashMap<String, String> existingState = new HashMap<>();
        final long existingTimestamp = System.currentTimeMillis() + 10_000;
        existingState.put(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER,
                String.valueOf(existingTimestamp));
        existingState.put("key1", "value1");
        existingState.put("key2", "value2");
        runner.getStateManager().setState(existingState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), existingState, Scope.CLUSTER);

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap postProcessedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals("Existing timestamp should NOT be updated",
                String.valueOf(existingTimestamp),
                postProcessedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        // State should stay the same.
        assertEquals(postProcessedState.get("key1"), existingState.get("key1"));
        assertEquals(postProcessedState.get("key2"), existingState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveCopyAttribute() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(100));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull("Latest timestamp should be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
    }

    @Test
    public void testClusterMonitorInactivity() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        final List<MockFlowFile> inactiveFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_INACTIVE);
        assertEquals(1, inactiveFiles.size());

        final MockFlowFile inactiveFile = inactiveFiles.get(0);
        assertNotNull(inactiveFile.getAttribute("inactivityStartMillis"));
        assertNotNull(inactiveFile.getAttribute("inactivityDurationMillis"));

        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorInactivityFallbackToNodeScope() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        final List<MockFlowFile> inactiveFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_INACTIVE);
        assertEquals(1, inactiveFiles.size());

        final MockFlowFile inactiveFile = inactiveFiles.get(0);
        assertNotNull(inactiveFile.getAttribute("inactivityStartMillis"));
        assertNotNull(inactiveFile.getAttribute("inactivityDurationMillis"));

        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorInactivityOnPrimaryNode() throws Exception {
        final TestableProcessor processor = new TestableProcessor(10000);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setClustered(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        final List<MockFlowFile> inactiveFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_INACTIVE);
        assertEquals(1, inactiveFiles.size());

        final MockFlowFile inactiveFile = inactiveFiles.get(0);
        assertNotNull(inactiveFile.getAttribute("inactivityStartMillis"));
        assertNotNull(inactiveFile.getAttribute("inactivityDurationMillis"));

        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorInactivityOnNode() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive, but this not shouldn't send flow file
        runner.run();
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);

        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelf() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored
        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals(1, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));

        // Latest activity should be persisted
        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull("Latest timestamp should be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnNode() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        // This node won't send notification files
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.clearTransferState();

        // Activity restored
        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runNext(runner);
        // This node should not send restored flow file
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);

        // Latest activity should be persisted
        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull("Latest timestamp should be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnPrimaryNode() throws Exception {
        final TestableProcessor processor = new TestableProcessor(10000);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setClustered(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored
        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals(1, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));

        // Latest activity should be persisted
        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull("Latest timestamp should be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnPrimaryNodeFallbackToNodeScope() throws Exception {
        final TestableProcessor processor = new TestableProcessor(10000);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setClustered(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored
        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals(1, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));

        // Latest activity should NOT be persisted
        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNull("Latest timestamp should NOT be persisted", updatedState.get(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNode() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER, String.valueOf(System.currentTimeMillis()));
        clusterState.put("key1", "value1");
        clusterState.put("key2", "value2");
        runner.getStateManager().setState(clusterState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), clusterState, Scope.CLUSTER);

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals("Should be zero since it doesn't have incoming file.", 0, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));
        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNodeOnPrimary() throws Exception {

        final TestableProcessor processor = new TestableProcessor(10000);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setClustered(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER, String.valueOf(System.currentTimeMillis()));
        clusterState.put("key1", "value1");
        clusterState.put("key2", "value2");
        runner.getStateManager().setState(clusterState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), clusterState, Scope.CLUSTER);

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals("Should be zero since it doesn't have incoming file.", 0, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));
        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNodeOnNode() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(10000));
        runner.setClustered(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "100 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_LATEST_SUCCESS_TRANSFER, String.valueOf(System.currentTimeMillis()));
        clusterState.put("key1", "value1");
        clusterState.put("key2", "value2");
        runner.getStateManager().setState(clusterState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), clusterState, Scope.CLUSTER);

        runNext(runner);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.clearTransferState();

    }

}
