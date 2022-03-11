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
import org.apache.nifi.connectable.Position;
import org.apache.nifi.groups.ProcessGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConnectableEdge {
    private final ConnectableNode source;
    private final ConnectableNode destination;
    private final Connection connection;
    private long zIndex;
    private List<Position> bendPoints;

    public ConnectableEdge(final ConnectableNode source, final ConnectableNode destination, final Connection connection) {
        this.source = source;
        this.destination = destination;
        this.connection = connection;

        this.zIndex = connection.getZIndex();
        this.bendPoints = new ArrayList<>(connection.getBendPoints());
    }

    public ConnectableNode getSource() {
        return source;
    }

    public ConnectableNode getDestination() {
        return destination;
    }

    public List<Position> getBendPoints() {
        return bendPoints;
    }

    public void setBendPoints(final List<Position> bendPoints) {
        this.bendPoints = new ArrayList<>(bendPoints);
    }

    public void addBendPoint(final Position bendPoint) {
        this.bendPoints.add(bendPoint);
    }

    public long getZIndex() {
        return zIndex;
    }

    public void setZIndex(final long zIndex) {
        this.zIndex = zIndex;
    }

    public ProcessGroup getProcessGroup() {
        return connection.getProcessGroup();
    }

    public boolean isSelfLoop() {
        return source.equals(destination);
    }

    public void applyUpdates() {
        connection.setZIndex(zIndex);
        connection.setBendPoints(bendPoints);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ConnectableEdge that = (ConnectableEdge) o;
        return connection.equals(that.connection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection);
    }
}
