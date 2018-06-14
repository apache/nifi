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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ConnectionDTO;

import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType(name="connectionDiagnostics")
public class ConnectionDiagnosticsDTO {
    private ConnectionDTO connection;
    private ConnectionDiagnosticsSnapshotDTO aggregateSnapshot;
    private List<ConnectionDiagnosticsSnapshotDTO> nodeSnapshots;

    @ApiModelProperty(value = "Details about the connection", readOnly = true)
    public ConnectionDTO getConnection() {
        return connection;
    }

    public void setConnection(final ConnectionDTO connection) {
        this.connection = connection;
    }

    @ApiModelProperty(value = "Aggregate values for all nodes in the cluster, or for this instance if not clustered", readOnly = true)
    public ConnectionDiagnosticsSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(final ConnectionDiagnosticsSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty(value = "A list of values for each node in the cluster, if clustered.", readOnly = true)
    public List<ConnectionDiagnosticsSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(final List<ConnectionDiagnosticsSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }
}
