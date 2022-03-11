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

package org.apache.nifi.flow.layout;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestComponentArrangement {

    private final ProcessGroup group = Mockito.mock(ProcessGroup.class);
    private ConnectableNodeRegistry registry;

    @BeforeEach
    public void setup() {
        registry = new ConnectableNodeRegistry();
    }


    @Test
    public void testAToBToCToACreatesBendPoint() {
        final ProcessorNode parent = LayoutTestUtils.createProcessor(group, 0, 0, "parent");
        final ProcessorNode child = LayoutTestUtils.createProcessor(group, 100, 500, "child");
        final ProcessorNode grandChild = LayoutTestUtils.createProcessor(group, 200, -10, "grandchild");

        LayoutTestUtils.connect(parent, child);
        LayoutTestUtils.connect(child, grandChild);
        final Connection grandChildToParent = LayoutTestUtils.connect(grandChild, parent);

        final ConnectableNode parentNode = registry.getOrCreate(parent, () -> new ConnectableNode(parent, group, 0, registry));
        final ConnectableNode childNode = registry.getOrCreate(child, () -> new ConnectableNode(child, group, 1, registry));
        final ConnectableNode grandChildNode = registry.getOrCreate(grandChild, () -> new ConnectableNode(grandChild, group, 1, registry));

        final GraphLine line0 = new GraphLine(0);
        line0.addNode(parentNode);

        final GraphLine line1 = new GraphLine(1);
        line1.addNode(childNode);

        final GraphLine line2 = new GraphLine(2);
        line2.addNode(grandChildNode);

        final List<GraphLine> graphLines = Arrays.asList(line0, line1, line2);
        final ComponentArrangement arrangement = new ComponentArrangement(graphLines);
        arrangement.arrangeComponents();

        // Apply the positions to the components
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(ConnectableNode::applyPosition);
        }

        // Ensure that components are aligned
        assertEquals(parent.getPosition().getX(), child.getPosition().getX(), 0.2D);
        assertEquals(parent.getPosition().getX(), grandChild.getPosition().getX(), 0.2D);
        assertTrue(parent.getPosition().getY() < child.getPosition().getY());
        assertTrue(child.getPosition().getY() < grandChild.getPosition().getY());

        // Ensure that two bendpoints were added in connection from grandchild back to parent
        Mockito.verify(grandChildToParent, Mockito.atLeastOnce()).setBendPoints(ArgumentMatchers.argThat(bendpoints -> {
            if (bendpoints.size() != 2) {
                return false;
            }

            return bendpoints.get(0).getX() == bendpoints.get(1).getX();
        }));
    }
}
