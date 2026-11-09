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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScheduledStateChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String PROC_A = "proc-a";
    private static final String PROC_B = "proc-b";
    private static final String TASK_A = "task-a";
    private static final String REMOTE_GROUP = "remote-group";
    private static final String REMOTE_OUTPUT_PORT = "remote-output-port";

    private ScheduledStateChangedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new ScheduledStateChangedRebaseHandler();
    }

    @Test
    void testNoUpstreamChange() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), ScheduledState.ENABLED);
        final FlowDifference upstreamOtherComponent = createDifference(createProcessor(PROC_B), ScheduledState.RUNNING);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamOtherComponent), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToSameState() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), ScheduledState.ENABLED);
        final FlowDifference upstreamDifference = createDifference(createProcessor(PROC_A), ScheduledState.ENABLED);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToDifferentState() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), ScheduledState.ENABLED);
        final FlowDifference upstreamDifference = createDifference(createProcessor(PROC_A), ScheduledState.RUNNING);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_SCHEDULED_STATE, result.getConflictCode());
    }

    @Test
    void testUnsupportedComponentType() {
        final VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setIdentifier(TASK_A);
        final FlowDifference localDifference = createDifference(reportingTask, ScheduledState.ENABLED);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), createRoot());

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE, result.getConflictCode());
    }

    @Test
    void testApplyProcessor() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedProcessor processor = createProcessor(PROC_A);
        processor.setScheduledState(ScheduledState.DISABLED);
        mergedFlow.getProcessors().add(processor);

        handler.apply(createDifference(createProcessor(PROC_A), ScheduledState.ENABLED), mergedFlow);

        assertEquals(ScheduledState.ENABLED, processor.getScheduledState());
    }

    @Test
    void testApplyRemoteGroupPort() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedRemoteGroupPort remotePort = createRemoteGroupPort();
        remotePort.setScheduledState(ScheduledState.DISABLED);
        final VersionedRemoteProcessGroup remoteProcessGroup = new VersionedRemoteProcessGroup();
        remoteProcessGroup.setIdentifier(REMOTE_GROUP);
        remoteProcessGroup.setOutputPorts(Set.of(remotePort));
        mergedFlow.getRemoteProcessGroups().add(remoteProcessGroup);

        final FlowDifference localDifference = createDifference(createRemoteGroupPort(), ScheduledState.ENABLED);
        assertEquals(RebaseClassification.COMPATIBLE, handler.classify(localDifference, Collections.emptySet(), mergedFlow).getClassification());
        handler.apply(localDifference, mergedFlow);

        assertEquals(ScheduledState.ENABLED, remotePort.getScheduledState());
    }

    private FlowDifference createDifference(final VersionedComponent component, final ScheduledState newState) {
        return new StandardFlowDifference(DifferenceType.SCHEDULED_STATE_CHANGED, component, component,
                ScheduledState.DISABLED, newState, "Scheduled state changed");
    }

    private VersionedProcessor createProcessor(final String identifier) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        return processor;
    }

    private VersionedRemoteGroupPort createRemoteGroupPort() {
        final VersionedRemoteGroupPort remotePort = new VersionedRemoteGroupPort();
        remotePort.setIdentifier(REMOTE_OUTPUT_PORT);
        return remotePort;
    }

    private VersionedProcessGroup createRoot() {
        final VersionedProcessGroup root = new VersionedProcessGroup();
        root.setIdentifier(ROOT);
        return root;
    }
}
