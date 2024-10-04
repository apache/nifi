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

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestMonitorActivity {

    @Test
    public void testFirstMessage() throws InterruptedException {
        final TestableProcessor processor = new TestableProcessor(1000);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        runner.clearTransferState();

        TimeUnit.MILLISECONDS.sleep(200);

        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");

        TimeUnit.MILLISECONDS.sleep(200);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
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
        assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");
    }

    @Test
    public void testFirstMessageWithWaitForActivityTrue() throws InterruptedException {
        final TestableProcessor processor = new TestableProcessor(1000);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");
        runner.setProperty(MonitorActivity.WAIT_FOR_ACTIVITY, "true");

        runner.run(1, false);
        TimeUnit.MILLISECONDS.sleep(200);
        runNext(runner);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);

        runner.enqueue(new byte[0]);
        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        runner.clearTransferState();

        TimeUnit.MILLISECONDS.sleep(200);

        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");

        TimeUnit.MILLISECONDS.sleep(200);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);

        restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");
    }
    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySet() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.enqueue("lorem ipsum");
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);
        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInput() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.run(1, false);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInputButClusterIsActive() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis());
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 minutes");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.run(1, false);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInputAndWasInactiveLastTime() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.setProperty(MonitorActivity.RESET_STATE_ON_RESTART, Boolean.FALSE.toString());
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.LOCAL
        );

        runner.run(1, false);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        final StateMap updatedLocalState = runner.getStateManager().getState(Scope.LOCAL);
        assertEquals(lastSuccessInCluster, updatedLocalState.get(MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInputAndWasActiveLastTime() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis());
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 minutes");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.setProperty(MonitorActivity.RESET_STATE_ON_RESTART, Boolean.FALSE.toString());
        // if was active, there is no local state

        runner.run(1, false);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInputAndTurnedInactiveSinceLastTime() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.setProperty(MonitorActivity.RESET_STATE_ON_RESTART, Boolean.FALSE.toString());
        // if was active, there is no local state

        runner.run(1, false);

        final StateMap updatedClusterState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedClusterState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        final StateMap updatedLocalState = runner.getStateManager().getState(Scope.LOCAL);
        assertEquals(lastSuccessInCluster, updatedLocalState.get(MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileAfterFirstStartWhenLastSuccessIsAlreadySetAndNoInputAndTurnedActiveSinceLastTime() throws Exception {
        final String lastSuccessInCluster = String.valueOf(currentTimeMillis());
        final String lastSuccessInLocal = String.valueOf(currentTimeMillis() - TimeUnit.MINUTES.toMillis(5));
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(0));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, lastSuccessInCluster), Scope.CLUSTER);

        runner.setProperty(MonitorActivity.RESET_STATE_ON_RESTART, Boolean.FALSE.toString());
        runner.getStateManager().setState(
                singletonMap(MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO, lastSuccessInLocal), Scope.LOCAL
        );

        runner.run(1, false);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(lastSuccessInCluster, updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertNull(runner.getStateManager().getState(Scope.LOCAL).get(MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO));

        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testReconcileWhenSharedStateIsNotYetSet() throws Exception {
        final TestableProcessor processor = new TestableProcessor(0);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "5 secs");

        runner.setConnected(false);
        runner.enqueue("lorem ipsum");
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        runner.setConnected(true);
        runNext(runner);

        final long tLocal = processor.getLastSuccessfulTransfer();
        final long tCluster = getLastSuccessFromCluster(runner);
        assertEquals(tLocal, tCluster);
    }

    @Test
    public void testReconcileAfterReconnectWhenPrimary() throws InterruptedException, IOException {
        final TestRunner runner = getRunnerScopeCluster(new MonitorActivity(), true);
        final StateManager stateManager = runner.getStateManager();

        // First trigger will write last success transfer into cluster.
        runner.enqueue("lorem ipsum");
        runner.run(1, false);

        final String lastSuccessTransferAfterFirstTrigger = stateManager.getState(Scope.CLUSTER)
                .get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);

        assertTransferCountSuccessInactiveRestored(runner, 1, 0);

        // At second trigger it's not connected, new last success transfer stored only locally.
        runner.setConnected(false);
        runner.enqueue("lorem ipsum");
        TimeUnit.MILLISECONDS.sleep(500); // This sleep is needed to guarantee, that the stored timestamp will be different.
        runNext(runner);

        assertEquals(lastSuccessTransferAfterFirstTrigger,
                stateManager.getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        assertTransferCountSuccessInactiveRestored(runner, 2, 0);

        // The third trigger is without flow file, but reconcile is triggered and value is written ot cluster.
        runner.setConnected(true);
        TimeUnit.MILLISECONDS.sleep(500);
        runNext(runner);

        assertNotEquals(lastSuccessTransferAfterFirstTrigger,
                stateManager.getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        // Inactive message is being sent after the connection is back.
        assertTransferCountSuccessInactiveRestored(runner, 2, 1);
    }

    @Test
    public void testReconcileAfterReconnectWhenNotPrimary() throws IOException, InterruptedException {
        final TestableProcessor processor = new TestableProcessor(1000);
        final TestRunner runner = getRunnerScopeCluster(processor, false);
        final StateManager stateManager = runner.getStateManager();

        // First trigger will write last success transfer into cluster.
        runner.enqueue("lorem ipsum");
        runner.run(1, false);

        final String lastSuccessTransferAfterFirstTrigger = stateManager.getState(Scope.CLUSTER)
                .get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);

        assertTransferCountSuccessInactiveRestored(runner, 1, 0);

        // At second trigger it's not connected, new last success transfer stored only locally.
        runner.setConnected(false);
        runner.enqueue("lorem ipsum");
        TimeUnit.MILLISECONDS.sleep(500); // This sleep is needed to guarantee, that the stored timestamp will be different.
        runNext(runner);

        assertEquals(lastSuccessTransferAfterFirstTrigger,
                stateManager.getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        assertTransferCountSuccessInactiveRestored(runner, 2, 0);

        // The third trigger is without flow file, but reconcile is triggered and value is written ot cluster.
        runner.setConnected(true);
        runNext(runner);

        assertNotEquals(lastSuccessTransferAfterFirstTrigger,
                stateManager.getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));

        // No inactive message because of the node is not primary
        assertTransferCountSuccessInactiveRestored(runner, 2, 0);
    }

    private void runNext(TestRunner runner) {
        // Don't initialize, otherwise @OnScheduled is called and state gets reset
        runner.run(1, false, false);
    }

    private TestRunner getRunnerScopeCluster(final MonitorActivity processor, final boolean isPrimary) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(isPrimary);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "10 millis");
        return runner;
    }

    private Long getLastSuccessFromCluster(final TestRunner runner) throws IOException {
        return Long.valueOf(runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
    }

    private void assertTransferCountSuccessInactiveRestored(TestRunner runner, final int success, final int inactive) {
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, success);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, inactive);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
    }

    @Test
    public void testNoReportingWhenDisconnected() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(5)));

        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "3 minutes");

        runner.setConnected(false);
        runner.run(1, false);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);

        runner.setConnected(true);
        runner.run(1, false);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
    }

    @Test
    public void testFirstMessageWithInherit() throws InterruptedException {
        final TestableProcessor processor = new TestableProcessor(1000);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS).get(0);
        runner.clearTransferState();

        TimeUnit.MILLISECONDS.sleep(200);

        runNext(runner);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");

        // verify the UUIDs are not the same
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertNotEquals(restoredFlowFile.getSize(), originalFlowFile.getSize());

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");

        TimeUnit.MILLISECONDS.sleep(200);
        runNext(runner);

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
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
        assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertNotEquals(restoredFlowFile.getSize(), originalFlowFile.getSize());
    }

    @Timeout(5)
    @Test
    public void testFirstRunNoMessages() throws InterruptedException {
        // don't use the TestableProcessor, we want the real timestamp from @OnScheduled
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity());
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        int threshold = 100;
        boolean rerun;
        do {
            rerun = false;
            runner.setProperty(MonitorActivity.THRESHOLD, threshold + " millis");

            TimeUnit.MILLISECONDS.sleep(1000L);

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
        while (rerun);
    }

    /**
     * Since each call to run() will call @OnScheduled methods which will set the lastSuccessfulTransfer to the
     * current time, we need a way to create an artificial time difference between calls to run.
     */
    private static class TestableProcessor extends MonitorActivity {

        private final long startupTime;

        public TestableProcessor(final long timestampDifference) {
            this.startupTime = currentTimeMillis() - timestampDifference;
        }

        @Override
        protected long getStartupTime() {
            return startupTime;
        }
    }

    @Test
    public void testClusterMonitorActive() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        // Should be null because COPY_ATTRIBUTES is null.
        assertNull(updatedState.get("key1"));
        assertNull(updatedState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveFallbackToNodeScope() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        runner.run();

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
    }

    @Test
    public void testClusterMonitorActiveWithLatestTimestamp() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        // Set future timestamp in state
        final HashMap<String, String> existingState = new HashMap<>();
        final long existingTimestamp = currentTimeMillis() - 1_000;
        existingState.put(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO,
                String.valueOf(existingTimestamp));
        existingState.put("key1", "value1");
        existingState.put("key2", "value2");
        runner.getStateManager().setState(existingState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), existingState, Scope.CLUSTER);

        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap postProcessedState = runner.getStateManager().getState(Scope.CLUSTER);
        long postProcessedTimestamp = Long.parseLong(postProcessedState.get(
                MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertTrue(existingTimestamp < postProcessedTimestamp);
        // State should be updated. Null in this case.
        assertNull(postProcessedState.get("key1"));
        assertNull(postProcessedState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveMoreRecentTimestampExisted() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");

        runner.enqueue("Incoming data");

        // Set future timestamp in state
        final HashMap<String, String> existingState = new HashMap<>();
        final long existingTimestamp = currentTimeMillis() + 10_000;
        existingState.put(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO,
                String.valueOf(existingTimestamp));
        existingState.put("key1", "value1");
        existingState.put("key2", "value2");
        runner.getStateManager().setState(existingState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), existingState, Scope.CLUSTER);

        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap postProcessedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(
                String.valueOf(existingTimestamp),
                postProcessedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        // State should stay the same.
        assertEquals(postProcessedState.get("key1"), existingState.get("key1"));
        assertEquals(postProcessedState.get("key2"), existingState.get("key2"));
    }

    @Test
    public void testClusterMonitorActiveCopyAttribute() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        // This has to be very small threshold, otherwise, MonitorActivity skip persisting state.
        runner.setProperty(MonitorActivity.THRESHOLD, "1 ms");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        final HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        runner.enqueue("Incoming data", attributes);

        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);

        final StateMap updatedState = runner.getStateManager().getState(Scope.CLUSTER);
        assertNotNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
    }

    @Test
    public void testClusterMonitorInactivity() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
    public void testClusterMonitorInactivityFallbackToNodeScope() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
    public void testClusterMonitorInactivityOnPrimaryNode() {
        final TestableProcessor processor = new TestableProcessor(TimeUnit.MINUTES.toMillis(120));

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
    public void testClusterMonitorInactivityOnNode() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);

        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
        assertNotNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnNode() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
        assertNotNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnPrimaryNode() throws Exception {
        final TestableProcessor processor = new TestableProcessor(TimeUnit.MINUTES.toMillis(120));

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
        assertNotNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        assertEquals("value1", updatedState.get("key1"));
        assertEquals("value2", updatedState.get("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredBySelfOnPrimaryNodeFallbackToNodeScope() throws Exception {
        final TestableProcessor processor = new TestableProcessor(TimeUnit.MINUTES.toMillis(120));

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
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
        assertNull(updatedState.get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNode() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, "10 sec");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(currentTimeMillis()));
        clusterState.put("key1", "value1");
        clusterState.put("key2", "value2");
        runner.getStateManager().setState(clusterState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), clusterState, Scope.CLUSTER);

        // Common state is not sampled on each trigger. We need to wait a little to get notified about the update.
        TimeUnit.MILLISECONDS.sleep(3334); // Sampling rate is threshold/3

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals(0, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));
        runner.clearTransferState();

    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNodeOnPrimary() throws Exception {

        final TestableProcessor processor = new TestableProcessor(TimeUnit.MINUTES.toMillis(120));

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "10 sec");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(currentTimeMillis()));
        clusterState.put("key1", "value1");
        clusterState.put("key2", "value2");
        runner.getStateManager().setState(clusterState, Scope.CLUSTER);
        runner.getStateManager().replace(runner.getStateManager().getState(Scope.CLUSTER), clusterState, Scope.CLUSTER);

        // Common state is not sampled on each trigger. We need to wait a little to get notified about the update.
        TimeUnit.MILLISECONDS.sleep(3334); // Sampling rate is threshold/3

        runNext(runner);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS);
        final List<MockFlowFile> activityRestoredFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED);
        assertEquals(0, successFiles.size());
        assertEquals(1, activityRestoredFiles.size());
        assertEquals("value1", activityRestoredFiles.get(0).getAttribute("key1"));
        assertEquals("value2", activityRestoredFiles.get(0).getAttribute("key2"));
        runner.clearTransferState();
    }

    @Test
    public void testClusterMonitorActivityRestoredByOtherNodeOnNode() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 mins");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        // Becomes inactive
        runner.run();
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.clearTransferState();

        // Activity restored, even if this node doesn't have activity, other node updated the cluster state.
        final HashMap<String, String> clusterState = new HashMap<>();
        clusterState.put(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(currentTimeMillis()));
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

    @Test
    public void testDisconnectedNodeActivatesFlow() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Disconnect the node, and feed in a flow file
        runner.setConnected(false);
        runner.enqueue("Incoming data");
        runNext(runner);

        // We expect both activation marker, and forwarded FF
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
    }

    @Test
    public void testDisconnectedNodeDeactivatesFlowOnlyWhenConnected() {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(false);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.clearTransferState();

        // Disconnect the node, and expect marker
        runner.setConnected(true);
        runNext(runner);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
    }

    @Test
    public void testLocalStateIsNotDeletedInStandaloneCaseWhenStopped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());

        // Stop the processor and expect the local state still there
        runner.stop();
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());
    }

    @Test
    public void testLocalStateIsNotDeletedInClusteredCaseNodeScopeWhenStopped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());

        // Stop the processor and expect the local state still there
        runner.stop();
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());
    }

    @Test
    public void testLocalStateIsNotDeletedInClusteredCaseClusterScopeWhenStopped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());

        // Stop the processor and expect the local state still there
        runner.stop();
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());
    }

    @Test
    public void testLocalStateIsNotDeletedInClusteredCaseWhenDisconnectedAndStopped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.MINUTES.toMillis(120)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "1 min");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());

        // Disconnect the node & stop the processor and expect the local state still there
        runner.setConnected(false);
        runner.stop();
        assertFalse(runner.getStateManager().getState(Scope.LOCAL).toMap().isEmpty());
    }

    @Test
    public void testActivationMarkerIsImmediateWhenAnyOtherNodeActivatesTheFlow() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.DAYS.toMillis(1)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(true);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_PRIMARY);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 hours");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Update the cluster state
        runner.getStateManager().setState(singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(currentTimeMillis())), Scope.CLUSTER);
        runNext(runner);

        // We expect activation marker only
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_ACTIVITY_RESTORED);
    }

    @Test
    public void testDisconnectNodeAndActivateBothTheOtherNodesAndTheDisconnectedNodeIndependently() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.DAYS.toMillis(1)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 hours");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Disconnect the node, and feed in a flow file to activate it
        runner.setConnected(false);
        runner.enqueue("Incoming data");
        runNext(runner);

        // We expect both activation marker, and forwarded FF
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.clearTransferState();

        // Update the cluster state too, and reconnect. This simulates other nodes being activated.
        runner.getStateManager().setState(singletonMap(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(currentTimeMillis())), Scope.CLUSTER);
        runner.setConnected(true);
        runNext(runner);

        // We expect no output
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
    }

    @Test
    public void testClusterStateIsImmediatelyUpdatedOnActivation() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.DAYS.toMillis(1)));
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.REPORTING_NODE, MonitorActivity.REPORT_NODE_ALL);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 hours");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        runner.enqueue("Incoming data");
        runNext(runner);
        assertNotNull(runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
    }

    @Test
    public void testResetStateOnStartupByDefault() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity());
        runner.setIsConfiguredForClustering(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.THRESHOLD, "24 hours");

        runner.getStateManager().setState(
                singletonMap(
                        MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO,
                        String.valueOf(currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                ),
                Scope.LOCAL
        );

        runner.enqueue("Incoming data");
        runner.run();

        // We expect only the FF as output
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS);
    }

    @Test
    public void testResetStateOnStartupDisabled() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity());
        runner.setIsConfiguredForClustering(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.THRESHOLD, "24 hours");
        runner.setProperty(MonitorActivity.RESET_STATE_ON_RESTART, Boolean.FALSE.toString());

        runner.getStateManager().setState(
                singletonMap(
                        MonitorActivity.STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO,
                        String.valueOf(currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                ),
                Scope.LOCAL
        );

        runner.enqueue("Incoming data");
        runner.run();

        // We expect only the FF as output
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
    }

    @Test
    public void testMultipleFlowFilesActivateTheFlowInSingleTriggerResultsInSingleMarker() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(TimeUnit.DAYS.toMillis(1)));
        runner.setIsConfiguredForClustering(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_NODE);
        runner.setProperty(MonitorActivity.THRESHOLD, "3 hours");

        // Becomes inactive
        runner.run(1, false);
        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 1);
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE);
        runner.clearTransferState();

        // Adding flow files
        runner.enqueue("Incoming data 1");
        runner.enqueue("Incoming data 2");
        runner.enqueue("Incoming data 3");
        runNext(runner);

        // We expect only the FF as output
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 3);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
    }

    @Test
    public void testInfrequentFlowFilesTriggerImmediateSynchronization() throws IOException, InterruptedException {
        final long threshold_seconds = 30;
        final long startup_time_seconds = 1;
        final AtomicLong nowProvider = new AtomicLong(TimeUnit.SECONDS.toMillis(startup_time_seconds));
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity() {
            @Override
            protected long nowMillis() {
                return nowProvider.get();
            }
        });
        runner.setIsConfiguredForClustering(true);
        runner.setConnected(true);
        runner.setPrimaryNode(false);
        runner.setProperty(MonitorActivity.MONITORING_SCOPE, MonitorActivity.SCOPE_CLUSTER);
        runner.setProperty(MonitorActivity.THRESHOLD, threshold_seconds + " seconds");

        // Initialize
        runner.run(1, false);
        final String state_0 = runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);
        assertNull(state_0);

        // First ever FlowFile triggers sync
        runner.enqueue("Incoming data 1");
        runNext(runner);
        final String state_1 = runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);
        assertNotNull(state_1);

        // Wait > (2/3 * T)
        nowProvider.set(TimeUnit.SECONDS.toMillis(startup_time_seconds + ((2 * threshold_seconds) / 3) + 1));
        runNext(runner);
        runner.enqueue("Incoming data 2");
        runNext(runner);
        final String state_2 = runner.getStateManager().getState(Scope.CLUSTER).get(MonitorActivity.STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);
        assertNotEquals(state_1, state_2);
    }
}
