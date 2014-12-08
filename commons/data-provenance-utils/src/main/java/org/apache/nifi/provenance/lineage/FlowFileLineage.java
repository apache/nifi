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
package org.apache.nifi.provenance.lineage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FlowFileLineage implements Lineage {

    private final List<LineageNode> nodes;
    private final List<LineageEdge> edges;

    public FlowFileLineage(final Collection<LineageNode> nodes, final Collection<LineageEdge> edges) {
        this.nodes = new ArrayList<>(requireNonNull(nodes));
        this.edges = new ArrayList<>(requireNonNull(edges));
    }

    @Override
    public List<LineageNode> getNodes() {
        return nodes;
    }

    @Override
    public List<LineageEdge> getEdges() {
        return edges;
    }

    @Override
    public int hashCode() {
        int sum = 923;
        for (final LineageNode node : nodes) {
            sum += node.hashCode();
        }

        for (final LineageEdge edge : edges) {
            sum += edge.hashCode();
        }

        return sum;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FlowFileLineage)) {
            return false;
        }

        final FlowFileLineage other = (FlowFileLineage) obj;
        return nodes.equals(other.nodes) && edges.equals(other.edges);
    }
}
