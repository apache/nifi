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

import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NameChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String CHILD_GROUP = "child-group";
    private static final String PROC_A = "proc-a";
    private static final String PROC_B = "proc-b";
    private static final String ORIGINAL_NAME = "Original";
    private static final String LOCAL_NAME = "Local";
    private static final String UPSTREAM_NAME = "Upstream";

    private NameChangedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new NameChangedRebaseHandler();
    }

    @Test
    void testNoUpstreamChange() {
        final FlowDifference localDifference = createNameDifference(PROC_A, LOCAL_NAME);
        final FlowDifference upstreamOtherComponent = createNameDifference(PROC_B, UPSTREAM_NAME);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamOtherComponent), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToSameName() {
        final FlowDifference localDifference = createNameDifference(PROC_A, LOCAL_NAME);
        final FlowDifference upstreamDifference = createNameDifference(PROC_A, LOCAL_NAME);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testUpstreamChangedToDifferentName() {
        final FlowDifference localDifference = createNameDifference(PROC_A, LOCAL_NAME);
        final FlowDifference upstreamDifference = createNameDifference(PROC_A, UPSTREAM_NAME);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Set.of(upstreamDifference), createRoot());

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_COMPONENT_NAME, result.getConflictCode());
    }

    @Test
    void testApplyProcessorName() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedProcessor processor = createProcessor(PROC_A, ORIGINAL_NAME);
        mergedFlow.getProcessors().add(processor);

        handler.apply(createNameDifference(PROC_A, LOCAL_NAME), mergedFlow);

        assertEquals(LOCAL_NAME, processor.getName());
    }

    @Test
    void testApplyProcessGroupName() {
        final VersionedProcessGroup mergedFlow = createRoot();
        final VersionedProcessGroup childGroup = createGroup(CHILD_GROUP, ORIGINAL_NAME);
        mergedFlow.getProcessGroups().add(childGroup);

        final VersionedProcessGroup localGroup = createGroup(CHILD_GROUP, LOCAL_NAME);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.NAME_CHANGED, createGroup(CHILD_GROUP, ORIGINAL_NAME), localGroup,
                ORIGINAL_NAME, LOCAL_NAME, "Name changed");

        assertEquals(RebaseClassification.COMPATIBLE, handler.classify(localDifference, Collections.emptySet(), mergedFlow).getClassification());
        handler.apply(localDifference, mergedFlow);

        assertEquals(LOCAL_NAME, childGroup.getName());
    }

    private FlowDifference createNameDifference(final String identifier, final String newName) {
        return new StandardFlowDifference(DifferenceType.NAME_CHANGED, createProcessor(identifier, ORIGINAL_NAME), createProcessor(identifier, newName),
                ORIGINAL_NAME, newName, "Name changed");
    }

    private VersionedProcessor createProcessor(final String identifier, final String name) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setName(name);
        return processor;
    }

    private VersionedProcessGroup createGroup(final String identifier, final String name) {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setIdentifier(identifier);
        group.setName(name);
        return group;
    }

    private VersionedProcessGroup createRoot() {
        return createGroup(ROOT, ROOT);
    }
}
