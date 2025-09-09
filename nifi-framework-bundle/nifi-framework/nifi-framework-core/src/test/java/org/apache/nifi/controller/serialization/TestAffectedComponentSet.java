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

package org.apache.nifi.controller.serialization;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestAffectedComponentSet {

    private static final FlowController controller = Mockito.mock(FlowController.class);
    private static final FlowManager flowManager = Mockito.mock(FlowManager.class);
    static {
        when(controller.getFlowManager()).thenReturn(flowManager);
    }

    @Test
    public void testMinus() {
        final AffectedComponentSet setA = new AffectedComponentSet(controller);
        final AffectedComponentSet setB = new AffectedComponentSet(controller);

        for (int i = 0; i < 10; i++) {
            final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
            setA.addProcessor(processorNode);

            if (i < 8) {
                setB.addProcessor(processorNode);
            }
        }

        final AffectedComponentSet difference = setA.minus(setB);
        assertEquals(10, setA.getComponentCount());
        assertEquals(8, setB.getComponentCount());
        assertEquals(2, difference.getComponentCount());
    }

    @Test
    public void testRemoveComponents() {
        final AffectedComponentSet setA = new AffectedComponentSet(controller);
        final List<ProcessorNode> toRemove = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
            setA.addProcessor(processorNode);

            if (i < 8) {
                toRemove.add(processorNode);
            }
        }

        final ComponentSetFilter componentSetFilter = Mockito.mock(ComponentSetFilter.class);
        doAnswer(invocation -> {
            final ProcessorNode toTest = invocation.getArgument(0, ProcessorNode.class);
            return toRemove.contains(toTest);
        }).when(componentSetFilter).testProcessor(any(ProcessorNode.class));

        final AffectedComponentSet removed = setA.removeComponents(componentSetFilter);
        assertEquals(2, setA.getComponentCount());
        assertEquals(8, removed.getComponentCount());
    }

    @Test
    public void testAddAffectedComponentsWithNullComponentADoesNotNPE() {
        final AffectedComponentSet set = new AffectedComponentSet(controller);

        // Simulate a DEEP comparison config difference on an added Process Group
        final FlowDifference difference = Mockito.mock(FlowDifference.class);
        when(difference.getDifferenceType()).thenReturn(DifferenceType.EXECUTION_ENGINE_CHANGED);
        when(difference.getComponentA()).thenReturn(null); // Newly added PG => local component is null

        // Should not throw and should not add any local components
        set.addAffectedComponents(difference);
        assertEquals(0, set.getComponentCount());
    }
}
