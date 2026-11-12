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
package org.apache.nifi.tests.system.pg;

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.StatelessGroupScheduledState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.DrainBlockingComponentDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProcessGroupDrainIT extends NiFiSystemIT {
    private static final String WRITE_LIFECYCLE_EVENTS = "WriteLifecycleEvents";
    private static final String DRAINING = "DRAINING";
    private static final String ENDED = "ENDED";

    @Override
    protected void destroyFlow() throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().endProcessGroupDrains("root");
        super.destroyFlow();
    }

    /**
     * Drains a group that has a running source in the group itself and in a child group. The last Processor of the group removes FlowFiles only
     * once a gate file exists, so the start/stop rules are checked while data is still queued. The source in the child group is still waiting
     * for its own gate file when the drain begins, as a source does while it waits for an external system, so the drain does not end while that
     * source can still produce data, even once nothing is queued. Once both gates open, the data flows out and NiFi ends the drain without any
     * request from the client. WriteLifecycleEvents records the calls to its {@code @OnDrainInitiated} and {@code @OnDrainCompleted} methods.
     * An empty Stateless child group can receive no data, so the drain does not start it. Ending the drain leaves every component as it is,
     * and the run states that the drain set are kept when NiFi restarts.
     */
    @Test
    public void testDrainEmptiesGroupWhileSourcesStayStoppedAndThenEnds() throws NiFiClientException, IOException, InterruptedException {
        final File eventFile = new File(getNiFiInstance().getInstanceDirectory(), "drain-lifecycle-events.txt");
        Files.deleteIfExists(eventFile.toPath());
        final File gateFile = createGateFile("drain-gate.txt");
        final File sourceGateFile = createGateFile("drain-source-gate.txt");
        try {
            drainGroupWithSourceStillRunning(eventFile, gateFile, sourceGateFile);
        } finally {
            // The gated source cannot stop until its gate file exists
            sourceGateFile.createNewFile();
        }
    }

    private void drainGroupWithSourceStillRunning(final File eventFile, final File gateFile, final File sourceGateFile) throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Drained", "root");
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, group.getId());
        final ProcessorEntity writeLifecycleEvents = getClientUtil().createProcessor(WRITE_LIFECYCLE_EVENTS, group.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, group.getId());
        final ConnectionEntity connection = getClientUtil().createConnection(generate, writeLifecycleEvents, SUCCESS, group.getId());
        final ConnectionEntity gatedConnection = getClientUtil().createConnection(writeLifecycleEvents, terminate, SUCCESS, group.getId());
        getClientUtil().updateProcessorProperties(writeLifecycleEvents, Map.of("Event File", eventFile.getAbsolutePath()));
        getClientUtil().updateProcessorProperties(terminate, Map.of("Gate File", gateFile.getAbsolutePath()));

        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("Child", group.getId());
        final ProcessorEntity childGenerate = getClientUtil().createProcessor(GENERATE_FLOWFILE, childGroup.getId());
        final ProcessorEntity childTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, childGroup.getId());
        final ConnectionEntity childConnection = getClientUtil().createConnection(childGenerate, childTerminate, SUCCESS, childGroup.getId());
        final ProcessGroupEntity emptyStatelessGroup = getClientUtil().markStateless(getClientUtil().createProcessGroup("Empty Stateless", group.getId()), "1 min");

        // Each source generates its FlowFiles when it starts and then stays running until its next scheduled run, 10 minutes later.
        // The source in the child group waits for its gate file before it generates anything.
        getClientUtil().startProcessor(getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", "3")));
        getClientUtil().startProcessor(getClientUtil().updateProcessorProperties(childGenerate, Map.of("Batch Size", "1", "Gate File", sourceGateFile.getAbsolutePath())));
        waitForQueueCount(connection.getId(), 3);
        waitFor(() -> getProcessor(childGenerate.getId()).getStatus().getAggregateSnapshot().getActiveThreadCount() == 1);

        // NiFi writes the flow 500 milliseconds after a change, so wait until the changes above are written before the drain changes the run states
        Thread.sleep(1_000L);

        final ProcessGroupDrainRequestDTO startedDrain = getClientUtil().startProcessGroupDrain(group.getId()).getDrainRequest();
        assertEquals(DRAINING, startedDrain.getState());
        assertEquals(3, startedDrain.getOriginalCount());
        final String drainRequestId = startedDrain.getId();

        waitForQueueCount(gatedConnection.getId(), 3);
        waitFor(() -> Files.readAllLines(eventFile.toPath()).contains("OnDrainInitiated"));
        getClientUtil().waitForStoppedProcessor(generate.getId());
        getClientUtil().waitForRunningProcessor(terminate.getId());
        getClientUtil().waitForRunningProcessor(childTerminate.getId());
        assertStatelessGroupState(emptyStatelessGroup.getId(), StatelessGroupScheduledState.STOPPED);

        // While data is queued, sources cannot be started or run, other components cannot be stopped, and no overlapping drain can begin
        assertConflict(() -> getNifiClient().getProcessorClient().startProcessor(getProcessor(generate.getId())));
        assertConflict(() -> getNifiClient().getProcessorClient().runProcessorOnce(getProcessor(generate.getId())));
        assertConflict(() -> getNifiClient().getProcessorClient().stopProcessor(getProcessor(terminate.getId())));

        final ScheduleComponentsEntity stopGroupRequest = new ScheduleComponentsEntity();
        stopGroupRequest.setId(group.getId());
        stopGroupRequest.setState(ScheduledState.STOPPED.name());
        assertConflict(() -> getNifiClient().getFlowClient().scheduleProcessGroupComponents(group.getId(), stopGroupRequest));
        assertConflict(() -> getClientUtil().startProcessGroupDrain(childGroup.getId()));

        assertEquals(drainRequestId, getDrainRequestId(group.getId()));
        assertFalse(Files.readAllLines(eventFile.toPath()).contains("OnDrainCompleted"));

        assertTrue(gateFile.createNewFile());
        waitFor(() -> getDrainRequest(group.getId(), drainRequestId).getCurrentCount() == 0);

        // NiFi checks the drain every two seconds, so this covers more than two checks
        Thread.sleep(5_000L);
        assertEquals(DRAINING, getDrainState(group.getId(), drainRequestId));

        assertTrue(sourceGateFile.createNewFile());
        getClientUtil().waitForStoppedProcessor(childGenerate.getId());
        getClientUtil().waitForProcessGroupDrainEnded(group.getId(), drainRequestId);
        assertNull(getDrainRequestId(group.getId()));
        assertTrue(Files.readAllLines(eventFile.toPath()).contains("OnDrainCompleted"));
        assertEquals(0, getConnectionQueueSize(childConnection.getId()));

        final Map<String, ScheduledState> expectedProcessorStates = Map.of(generate.getId(), ScheduledState.STOPPED, childGenerate.getId(), ScheduledState.STOPPED,
            terminate.getId(), ScheduledState.RUNNING, childTerminate.getId(), ScheduledState.RUNNING);
        assertProcessorStates(expectedProcessorStates);
        assertStatelessGroupState(emptyStatelessGroup.getId(), StatelessGroupScheduledState.STOPPED);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        assertProcessorStates(expectedProcessorStates);

        getClientUtil().startProcessor(getProcessor(generate.getId()));
        getClientUtil().waitForRunningProcessor(generate.getId());
    }

    /**
     * A stopped Processor that is invalid, and a disabled Processor, each with FlowFiles queued, cannot process their data during a drain.
     * Both are reported as blocking the drain, and the invalid Processor is not started. Ending the drain leaves the FlowFiles queued and the
     * components as they are.
     */
    @Test
    public void testEndDrainLeavesQueuedDataAndComponentsAsTheyAre() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Drained", "root");
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, group.getId());
        final ProcessorEntity validate = getClientUtil().createProcessor("ValidateFileExists", group.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, group.getId());
        final ConnectionEntity generateToValidate = getClientUtil().createConnection(generate, validate, SUCCESS, group.getId());
        final ConnectionEntity generateToTerminate = getClientUtil().createConnection(generate, terminate, SUCCESS, group.getId());
        getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", "3"));

        getClientUtil().runProcessorOnce(getProcessor(generate.getId()));
        waitForQueueCount(generateToValidate.getId(), 3);
        waitForQueueCount(generateToTerminate.getId(), 3);
        getClientUtil().waitForStoppedProcessor(generate.getId());
        getClientUtil().waitForInvalidProcessor(validate.getId());
        getNifiClient().getProcessorClient().disableProcessor(getProcessor(terminate.getId()));
        getClientUtil().waitForProcessorState(terminate.getId(), ScheduledState.DISABLED.name());

        final String drainRequestId = getClientUtil().startProcessGroupDrain(group.getId()).getDrainRequest().getId();
        final ProcessGroupDrainRequestDTO drainingRequest = getNifiClient().getProcessGroupClient().getDrainRequest(group.getId(), drainRequestId).getDrainRequest();
        assertEquals(DRAINING, drainingRequest.getState());
        assertEquals(6, drainingRequest.getCurrentCount());

        final Map<String, String> blockingReasons = new HashMap<>();
        for (final DrainBlockingComponentDTO blockingComponent : drainingRequest.getBlockingComponents()) {
            blockingReasons.put(blockingComponent.getId(), blockingComponent.getReason());
        }
        assertEquals(Map.of(validate.getId(), "INVALID", terminate.getId(), "DISABLED"), blockingReasons);

        assertEquals(ENDED, getClientUtil().endProcessGroupDrain(group.getId(), drainRequestId).getDrainRequest().getState());
        assertNull(getDrainRequestId(group.getId()));

        assertProcessorStates(Map.of(generate.getId(), ScheduledState.STOPPED, validate.getId(), ScheduledState.STOPPED, terminate.getId(), ScheduledState.DISABLED));
        assertEquals(3, getConnectionQueueSize(generateToValidate.getId()));
        assertEquals(3, getConnectionQueueSize(generateToTerminate.getId()));
    }

    private File createGateFile(final String fileName) throws IOException {
        final File gateFile = new File(getNiFiInstance().getInstanceDirectory(), fileName);
        gateFile.deleteOnExit();
        Files.deleteIfExists(gateFile.toPath());
        return gateFile;
    }

    private ProcessorEntity getProcessor(final String processorId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessorClient().getProcessor(processorId);
    }

    private ProcessGroupDrainRequestDTO getDrainRequest(final String groupId, final String drainRequestId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessGroupClient().getDrainRequest(groupId, drainRequestId).getDrainRequest();
    }

    /**
     * @return the state of the given drain, or null if the group has no record of the drain
     */
    private String getDrainState(final String groupId, final String drainRequestId) throws NiFiClientException, IOException {
        final ProcessGroupDrainRequestDTO drainRequest = getDrainRequest(groupId, drainRequestId);
        return drainRequest == null ? null : drainRequest.getState();
    }

    private void assertProcessorStates(final Map<String, ScheduledState> expectedProcessorStates) throws NiFiClientException, IOException {
        for (final Map.Entry<String, ScheduledState> entry : expectedProcessorStates.entrySet()) {
            final ProcessorEntity processor = getProcessor(entry.getKey());
            assertEquals(entry.getValue().name(), processor.getComponent().getState(), processor.getComponent().getName());
        }
    }

    /**
     * Returns the drain request ID of the given group, after verifying that the flow of the group, which the canvas shows when the group is entered,
     * reports the same drain request ID as the group itself.
     */
    private String getDrainRequestId(final String groupId) throws NiFiClientException, IOException {
        final String drainRequestId = getNifiClient().getProcessGroupClient().getProcessGroup(groupId).getComponent().getDrainRequestId();
        assertEquals(drainRequestId, getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getDrainRequestId());
        return drainRequestId;
    }

    private void assertStatelessGroupState(final String groupId, final StatelessGroupScheduledState expectedState) throws NiFiClientException, IOException {
        assertEquals(expectedState.name(), getNifiClient().getProcessGroupClient().getProcessGroup(groupId).getComponent().getStatelessGroupScheduledState());
    }

    /**
     * Asserts that the given request is rejected with a 409 response
     */
    private void assertConflict(final Callable<?> request) {
        try {
            request.call();
        } catch (final Exception e) {
            if (e instanceof NiFiClientException && e.getCause() instanceof final WebApplicationException webApplicationException) {
                assertEquals(409, webApplicationException.getResponse().getStatus(), e.getMessage());
                return;
            }

            fail("Expected a 409 response but the request failed", e);
        }

        fail("Expected a 409 response but the request succeeded");
    }
}
