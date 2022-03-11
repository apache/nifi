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

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestConnectedComponentAlignment {
    private final ProcessGroup group = Mockito.mock(ProcessGroup.class);
    private final Set<ProcessorNode> processors = new HashSet<>();

    @BeforeEach
    public void setup() {
        when(group.getProcessors()).thenReturn(processors);
    }

    @Test
    public void testDisconnectedComponentsUnmoved() {
        final ConnectedComponentAlignment alignment = new ConnectedComponentAlignment();

        final ProcessorNode processorA = addProcessor(0, 0);
        final ProcessorNode processorB = addProcessor(10, 100);

        alignment.alignComponents(group);

        assertEquals(new Position(0, 0), processorA.getPosition());
        assertEquals(new Position(10, 100), processorB.getPosition());
    }

    @Test
    public void testXCoordinatesAligned() {
        final ConnectedComponentAlignment alignment = new ConnectedComponentAlignment();

        final ProcessorNode processorA = addProcessor(0, 0);
        final ProcessorNode processorB = addProcessor(10, 150);
        connect(processorA, processorB);

        alignment.alignComponents(group);

        assertEquals(new Position(0, 0), processorA.getPosition());
        assertEquals(new Position(0, 150), processorB.getPosition());
    }

    @Test
    public void testYCoordinatesAligned() {
        final ConnectedComponentAlignment alignment = new ConnectedComponentAlignment();

        final ProcessorNode processorA = addProcessor(0, 0);
        final ProcessorNode processorB = addProcessor(400, 20);
        connect(processorA, processorB);

        alignment.alignComponents(group);

        assertEquals(new Position(0, 0), processorA.getPosition());
        assertEquals(new Position(400, 0), processorB.getPosition());
    }

    @Test
    public void testFarAwayComponentsUnmoved() {
        final ConnectedComponentAlignment alignment = new ConnectedComponentAlignment();

        final ProcessorNode processorA = addProcessor(0, 0);
        final ProcessorNode processorB = addProcessor(500, 500);
        connect(processorA, processorB);

        alignment.alignComponents(group);

        assertEquals(new Position(0, 0), processorA.getPosition());
        assertEquals(new Position(500, 500), processorB.getPosition());
    }


    private ProcessorNode addProcessor(final double x, final double y) {
        final AtomicReference<Position> positionReference = new AtomicReference<>(new Position(x, y));

        final ProcessorNode processor = Mockito.mock(ProcessorNode.class);
        when(processor.getProcessGroup()).thenReturn(group);
        when(processor.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor.getPosition()).thenAnswer(invocation -> positionReference.get());
        doAnswer(invocation -> {
            positionReference.set(invocation.getArgument(0, Position.class));
            return null;
        }).when(processor).setPosition(any(Position.class));

        processors.add(processor);
        return processor;
    }

    private void connect(final ProcessorNode processorA, final ProcessorNode processorB) {
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getSource()).thenReturn(processorA);
        when(connection.getDestination()).thenReturn(processorB);

        final Set<Connection> currentOutboundConnections = processorA.getConnections();
        final Set<Connection> outboundConnections = currentOutboundConnections == null ? new HashSet<>() : currentOutboundConnections;
        outboundConnections.add(connection);
        when(processorA.getConnections()).thenReturn(outboundConnections);

        final List<Connection> currentIncoming = processorB.getIncomingConnections();
        final List<Connection> incomingConnections = currentIncoming == null ? new ArrayList<>() : currentIncoming;
        incomingConnections.add(connection);
        when(processorB.getIncomingConnections()).thenReturn(incomingConnections);
    }
}
