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
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class LayoutTestUtils {
    public static ProcessorNode createProcessor(final ProcessGroup group, final double x, final double y, final String description) {
        final AtomicReference<Position> positionReference = new AtomicReference<>(new Position(x, y));

        final ProcessorNode processor = Mockito.mock(ProcessorNode.class);
        when(processor.getProcessGroup()).thenReturn(group);
        when(processor.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor.getPosition()).thenAnswer(invocation -> positionReference.get());
        doAnswer(invocation -> {
            positionReference.set(invocation.getArgument(0, Position.class));
            return null;
        }).when(processor).setPosition(any(Position.class));

        when(processor.toString()).thenReturn(description);

        return processor;
    }

    public static Connection connect(final ProcessorNode source, final ProcessorNode destination) {
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getSource()).thenReturn(source);
        when(connection.getDestination()).thenReturn(destination);

        final Set<Connection> currentOutboundConnections = source.getConnections();
        final Set<Connection> outboundConnections = currentOutboundConnections == null ? new HashSet<>() : currentOutboundConnections;
        outboundConnections.add(connection);
        when(source.getConnections()).thenReturn(outboundConnections);

        final List<Connection> currentIncoming = destination.getIncomingConnections();
        final List<Connection> incomingConnections = currentIncoming == null ? new ArrayList<>() : currentIncoming;
        incomingConnections.add(connection);
        when(destination.getIncomingConnections()).thenReturn(incomingConnections);

        final String toString = source + " -> " + destination;
        when(connection.toString()).thenReturn(toString);

        return connection;
    }
}
