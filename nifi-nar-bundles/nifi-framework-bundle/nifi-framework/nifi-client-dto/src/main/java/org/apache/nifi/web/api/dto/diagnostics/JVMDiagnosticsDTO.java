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

package org.apache.nifi.web.api.dto.diagnostics;

import java.util.List;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "jvmDiagnostics")
public class JVMDiagnosticsDTO {
    private Boolean clustered;
    private Boolean connected;
    private JVMDiagnosticsSnapshotDTO aggregateSnapshot;
    private List<NodeJVMDiagnosticsSnapshotDTO> nodeSnapshots;

    @ApiModelProperty("Whether or not the NiFi instance is clustered")
    public Boolean getClustered() {
        return clustered;
    }

    public void setClustered(Boolean clustered) {
        this.clustered = clustered;
    }

    @ApiModelProperty("Whether or not the node is connected to the cluster")
    public Boolean getConnected() {
        return connected;
    }

    public void setConnected(Boolean connected) {
        this.connected = connected;
    }

    @ApiModelProperty("Aggregate JVM diagnostic information about the entire cluster")
    public JVMDiagnosticsSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(JVMDiagnosticsSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty("Node-wise breakdown of JVM diagnostic information")
    public List<NodeJVMDiagnosticsSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeJVMDiagnosticsSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }
}
