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

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConcurrentTasksChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String PROC_A = "proc-a";
    private static final String PROC_B = "proc-b";
    private static final String SERVICE_A = "service-a";
    private static final String REMOTE_GROUP = "remote-group";
    private static final String REMOTE_INPUT_PORT = "remote-input-port";
    private static final int ORIGINAL_TASKS = 1;
    private static final int LOCAL_TASKS = 6;
    private static final int UPSTREAM_TASKS = 3;

    private ConcurrentTasksChangedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new ConcurrentTasksChangedRebaseHandler();
    }

    @Test
    void testNoUpstreamChange() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), LOCAL_TASKS);
        final FlowDifference upstreamOtherComponent = createDifference(createProcessor(PROC_B), UPSTREAM_TASKS);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamOtherComponent), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToSameValue() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), LOCAL_TASKS);
        final FlowDifference upstreamDifference = createDifference(createProcessor(PROC_A), LOCAL_TASKS);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToDifferentValue() {
        final FlowDifference localDifference = createDifference(createProcessor(PROC_A), LOCAL_TASKS);
        final FlowDifference upstreamDifference = createDifference(createProcessor(PROC_A), UPSTREAM_TASKS);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_CONCURRENT_TASKS, result.getConflictCode());
    }

    @Test
    void testUnsupportedComponentType() {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(SERVICE_A);
        final FlowDifference localDifference = createDifference(service, LOCAL_TASKS);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), createRoot());

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE, result.getConflictCode());
    }

    @Test
    void testApplyProcessor() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedProcessor processor = createProcessor(PROC_A);
        processor.setConcurrentlySchedulableTaskCount(ORIGINAL_TASKS);
        mergedFlow.getProcessors().add(processor);

        handler.apply(createDifference(createProcessor(PROC_A), LOCAL_TASKS), mergedFlow);

        assertEquals(LOCAL_TASKS, processor.getConcurrentlySchedulableTaskCount());
    }

    @Test
    void testApplyRemoteGroupPort() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedRemoteGroupPort remotePort = createRemoteGroupPort();
        remotePort.setConcurrentlySchedulableTaskCount(ORIGINAL_TASKS);
        final VersionedRemoteProcessGroup remoteProcessGroup = new VersionedRemoteProcessGroup();
        remoteProcessGroup.setIdentifier(REMOTE_GROUP);
        remoteProcessGroup.setInputPorts(Set.of(remotePort));
        mergedFlow.getRemoteProcessGroups().add(remoteProcessGroup);

        final FlowDifference localDifference = createDifference(createRemoteGroupPort(), LOCAL_TASKS);
        assertEquals(RebaseClassification.COMPATIBLE, handler.classify(localDifference, Collections.emptySet(), mergedFlow).getClassification());
        handler.apply(localDifference, mergedFlow);

        assertEquals(LOCAL_TASKS, remotePort.getConcurrentlySchedulableTaskCount());
    }

    private FlowDifference createDifference(final VersionedComponent component, final int newValue) {
        return new StandardFlowDifference(DifferenceType.CONCURRENT_TASKS_CHANGED, component, component,
                ORIGINAL_TASKS, newValue, "Concurrent tasks changed");
    }

    private VersionedProcessor createProcessor(final String identifier) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        return processor;
    }

    private VersionedRemoteGroupPort createRemoteGroupPort() {
        final VersionedRemoteGroupPort remotePort = new VersionedRemoteGroupPort();
        remotePort.setIdentifier(REMOTE_INPUT_PORT);
        return remotePort;
    }

    private VersionedProcessGroup createRoot() {
        final VersionedProcessGroup root = new VersionedProcessGroup();
        root.setIdentifier(ROOT);
        return root;
    }
}
