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

public class FlowFileNode implements LineageNode {

    private final String flowFileUuid;
    private final long creationTime;
    private String clusterNodeIdentifier;

    public FlowFileNode(final String flowFileUuid, final long flowFileCreationTime) {
        this.flowFileUuid = requireNonNull(flowFileUuid);
        this.creationTime = flowFileCreationTime;
    }

    @Override
    public String getIdentifier() {
        return flowFileUuid;
    }

    @Override
    public long getTimestamp() {
        return creationTime;
    }

    @Deprecated
    @Override
    public String getClusterNodeIdentifier() {
        return clusterNodeIdentifier;
    }

    @Override
    public LineageNodeType getNodeType() {
        return LineageNodeType.FLOWFILE_NODE;
    }

    @Override
    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    @Override
    public int hashCode() {
        return 23498723 + flowFileUuid.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FlowFileNode)) {
            return false;
        }

        final FlowFileNode other = (FlowFileNode) obj;
        return flowFileUuid.equals(other.flowFileUuid);
    }

    @Override
    public String toString() {
        return "FlowFile[UUID=" + flowFileUuid + "]";
    }
}
