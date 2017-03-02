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

package org.apache.nifi.web.api.dto;

import java.util.List;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "systemDiagnostics")
public class SystemDiagnosticsDTO {
    private SystemDiagnosticsSnapshotDTO aggregateSnapshot;
    private List<NodeSystemDiagnosticsSnapshotDTO> nodeSnapshots;


    @ApiModelProperty("A systems diagnostic snapshot that represents the aggregate values of all nodes in the cluster. If the NiFi instance is "
        + "a standalone instance, rather than a cluster, this represents the stats of the single instance.")
    public SystemDiagnosticsSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(SystemDiagnosticsSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty("A systems diagnostics snapshot for each node in the cluster. If the NiFi instance is a standalone instance, rather than "
        + "a cluster, this may be null.")
    public List<NodeSystemDiagnosticsSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeSystemDiagnosticsSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }
}
