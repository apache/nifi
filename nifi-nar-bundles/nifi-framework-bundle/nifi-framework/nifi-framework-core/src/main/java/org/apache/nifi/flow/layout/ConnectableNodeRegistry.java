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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Positionable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ConnectableNodeRegistry {
    private final Map<Object, ConnectableNode> nodesByConnectable = new HashMap<>();
    private final Map<Connection, ConnectableEdge> edgesByConnection = new HashMap<>();


    public ConnectableEdge getOrCreate(final Connection connection, final Supplier<ConnectableEdge> factory) {
        final ConnectableEdge existing = edgesByConnection.get(connection);
        if (existing != null) {
            return existing;
        }

        // There's no existing edgefor the given connectable. Create a new ConnectableEdge
        final ConnectableEdge created = factory.get();
        edgesByConnection.put(connection, created);
        return created;
    }

    public ConnectableNode getOrCreate(final Connectable connectable, final Supplier<ConnectableNode> factory) {
        final ConnectableNode existing = nodesByConnectable.get(connectable);
        if (existing != null) {
            return existing;
        }

        // There's no existing node for the given connectable. Create a new ConnectableNode
        final ConnectableNode created = factory.get();

        // If the connectable is a port, we want to check if its Process Group/RPG is registered and if so return that Node.
        // Otherwise, just register the newly created node.
        switch (connectable.getConnectableType()) {
            case INPUT_PORT:
            case OUTPUT_PORT:
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT:
                final Positionable positionableComponent = created.getPositionableComponent();
                final ConnectableNode existingPositionableNode = nodesByConnectable.get(positionableComponent);
                if (existingPositionableNode != null) {
                    return existingPositionableNode;
                }

                nodesByConnectable.put(positionableComponent, created);
            default:
                nodesByConnectable.put(connectable, created);
        }

        return created;
    }
}
