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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGraphLine {
    private static final Logger logger = LoggerFactory.getLogger(TestGraphLine.class);

    private final ProcessGroup group = Mockito.mock(ProcessGroup.class);
    private ConnectableNodeRegistry registry;

    @BeforeEach
    public void setup() {
        registry = new ConnectableNodeRegistry();
    }

    @Test
    public void testLayoutCoordinatesWithSingleParentChild() {
        final ProcessorNode parent = createProcessor(0, 0, "parent");
        final ProcessorNode child = createProcessor(500, -10, "child");
        connect(parent, child);

        final ConnectableNode parentNode = registry.getOrCreate(parent, () -> new ConnectableNode(parent, group, 0, registry));
        final ConnectableNode childNode = registry.getOrCreate(child, () -> new ConnectableNode(child, group, 1, registry));

        final GraphLine line0 = new GraphLine(0);
        line0.addNode(parentNode);

        final GraphLine line1 = new GraphLine(1);
        line1.addNode(childNode);

        line0.assignYCoordinates(0);
        line1.assignYCoordinates(250);

        final List<GraphLine> graphLines = Arrays.asList(line0, line1);
        final GraphTranslation translation = new GraphTranslation(graphLines);
        line0.assignXCoordinates(translation);
        line1.assignXCoordinates(translation);

        // Apply the positions to the components
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(ConnectableNode::applyPosition);
        }

        assertEquals(parent.getPosition().getX(), child.getPosition().getX(), 0.2D);
        assertTrue(parent.getPosition().getY() < child.getPosition().getY());
    }

    @Test
    public void testTwoParentsOneChild() {
        final ProcessorNode parentA = createProcessor(0, 0, "parentA");
        final ProcessorNode parentB = createProcessor(200, 4, "parentB");
        final ProcessorNode child = createProcessor(500, -10, "child");

        connect(parentA, child);
        connect(parentB, child);

        final ConnectableNode parentANode = registry.getOrCreate(parentA, () -> new ConnectableNode(parentA, group, 0, registry));
        final ConnectableNode parentBNode = registry.getOrCreate(parentB, () -> new ConnectableNode(parentB, group, 0, registry));
        final ConnectableNode childNode = registry.getOrCreate(child, () -> new ConnectableNode(child, group, 0, registry));

        final GraphLine line0 = new GraphLine(0);
        line0.addNode(parentANode);
        line0.addNode(parentBNode);
        final GraphLine line1 = new GraphLine(1);
        line1.addNode(childNode);

        line0.assignYCoordinates(0);
        line1.assignYCoordinates(250);

        final List<GraphLine> graphLines = Arrays.asList(line0, line1);
        final GraphTranslation translation = new GraphTranslation(graphLines);
        line0.assignXCoordinates(translation);
        line1.assignXCoordinates(translation);

        // Apply the positions to the components
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(ConnectableNode::applyPosition);
        }

        assertTrue(parentA.getPosition().getX() < child.getPosition().getX());
        assertTrue(parentB.getPosition().getX() > child.getPosition().getX());
        assertTrue(parentB.getPosition().getX() > parentA.getPosition().getX());

        assertEquals(parentA.getPosition().getY(), parentB.getPosition().getY(), 0.2D);
        assertTrue(parentA.getPosition().getY() < child.getPosition().getY());
    }

    @Test
    public void testSingleParentTwoChildren() {
        final ProcessorNode parent = createProcessor(0, 0, "parent");
        final ProcessorNode childA = createProcessor(100, -10, "childA");
        final ProcessorNode childB = createProcessor(200, -10, "childB");

        connect(parent, childA);
        connect(parent, childB);

        final ConnectableNode parentNode = registry.getOrCreate(parent, () -> new ConnectableNode(parent, group, 0, registry));
        final ConnectableNode childANode = registry.getOrCreate(childA, () -> new ConnectableNode(childA, group, 1, registry));
        final ConnectableNode childBNode = registry.getOrCreate(childB, () -> new ConnectableNode(childB, group, 1, registry));

        final GraphLine line0 = new GraphLine(0);
        line0.addNode(parentNode);

        final GraphLine line1 = new GraphLine(1);
        line1.addNode(childANode);
        line1.addNode(childBNode);

        line0.assignYCoordinates(0);
        line1.assignYCoordinates(250);

        final List<GraphLine> graphLines = Arrays.asList(line0, line1);
        final GraphTranslation translation = new GraphTranslation(graphLines);
        line0.assignXCoordinates(translation);
        line1.assignXCoordinates(translation);

        // Apply the positions to the components
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(ConnectableNode::applyPosition);
        }

        for (final GraphLine graphLine : graphLines) {
            logger.info(graphLine.toString());
            graphLine.getNodes().forEach(node -> logger.info(node.toString()));
            logger.info("\n\n");
        }

        assertTrue(parent.getPosition().getX() > childA.getPosition().getX());
        assertTrue(parent.getPosition().getX() < childB.getPosition().getX());
        assertTrue(childA.getPosition().getX() < childB.getPosition().getX());

        assertEquals(childA.getPosition().getY(), childB.getPosition().getY(), 0.2D);
        assertTrue(parent.getPosition().getY() < childA.getPosition().getY());
    }



    private ProcessorNode createProcessor(final double x, final double y, final String description) {
        return LayoutTestUtils.createProcessor(group, x,y, description);
    }

    private Connection connect(final ProcessorNode source, final ProcessorNode destination) {
        return LayoutTestUtils.connect(source, destination);
    }
}
