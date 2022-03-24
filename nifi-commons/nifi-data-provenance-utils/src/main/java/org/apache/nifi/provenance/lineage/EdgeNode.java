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

import static java.util.Objects.requireNonNull;

public class EdgeNode implements LineageEdge {

    private final String uuid;
    private final LineageNode source;
    private final LineageNode destination;

    public EdgeNode(final String uuid, final LineageNode source, final LineageNode destination) {
        this.uuid = uuid;
        this.source = requireNonNull(source);
        this.destination = requireNonNull(destination);
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public LineageNode getSource() {
        return source;
    }

    @Override
    public LineageNode getDestination() {
        return destination;
    }

    @Override
    public int hashCode() {
        return 43298293 + source.hashCode() + destination.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof EdgeNode)) {
            return false;
        }

        final EdgeNode other = (EdgeNode) obj;
        return (source.equals(other.source) && destination.equals(other.destination));
    }

    @Override
    public String toString() {
        return "Edge[Source=" + source + ", Destination=" + destination + "]";
    }
}
