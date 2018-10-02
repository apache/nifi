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

import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType(name = "connectionDiagnosticsSnapshot")
public class ConnectionDiagnosticsSnapshotDTO {
    private int totalFlowFileCount;
    private long totalByteCount;
    private String nodeIdentifier;
    private LocalQueuePartitionDTO localQueuePartition;
    private List<RemoteQueuePartitionDTO> remoteQueuePartitions;

    @ApiModelProperty("Total number of FlowFiles owned by the Connection")
    public int getTotalFlowFileCount() {
        return totalFlowFileCount;
    }

    public void setTotalFlowFileCount(int totalFlowFileCount) {
        this.totalFlowFileCount = totalFlowFileCount;
    }

    @ApiModelProperty("Total number of bytes that make up the content for the FlowFiles owned by this Connection")
    public long getTotalByteCount() {
        return totalByteCount;
    }

    public void setTotalByteCount(long totalByteCount) {
        this.totalByteCount = totalByteCount;
    }

    @ApiModelProperty("The Node Identifier that this information pertains to")
    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(final String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    @ApiModelProperty("The local queue partition, from which components can pull FlowFiles on this node.")
    public LocalQueuePartitionDTO getLocalQueuePartition() {
        return localQueuePartition;
    }

    public void setLocalQueuePartition(LocalQueuePartitionDTO localQueuePartition) {
        this.localQueuePartition = localQueuePartition;
    }

    public List<RemoteQueuePartitionDTO> getRemoteQueuePartitions() {
        return remoteQueuePartitions;
    }

    public void setRemoteQueuePartitions(List<RemoteQueuePartitionDTO> remoteQueuePartitions) {
        this.remoteQueuePartitions = remoteQueuePartitions;
    }
}
