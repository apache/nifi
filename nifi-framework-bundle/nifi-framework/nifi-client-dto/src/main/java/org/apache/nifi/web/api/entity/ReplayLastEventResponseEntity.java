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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "replayLastEventResponseEntity")
public class ReplayLastEventResponseEntity extends Entity {

    private String componentId;
    private String nodes;
    private ReplayLastEventSnapshotDTO aggregateSnapshot;
    private List<NodeReplayLastEventSnapshotDTO> nodeSnapshots;

    @Schema(description = "The UUID of the component whose last event should be replayed.")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @Schema(description = "Which nodes were requested to replay their last provenance event.",
        allowableValues = {"ALL", "PRIMARY"}
    )
    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    @Schema(description = "The aggregate result of all nodes' responses")
    public ReplayLastEventSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(final ReplayLastEventSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @Schema(description = "The node-wise results")
    public List<NodeReplayLastEventSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(final List<NodeReplayLastEventSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }
}
