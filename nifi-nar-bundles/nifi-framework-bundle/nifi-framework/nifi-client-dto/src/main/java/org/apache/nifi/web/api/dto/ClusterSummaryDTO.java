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

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * Details for the controller configuration.
 */
@XmlType(name = "clusterConfiguration")
public class ClusterSummaryDTO {

    private Boolean isClustered;
    private Boolean isConnectedToCluster;

    private String connectedNodes;
    private Integer connectedNodeCount = 0;
    private Integer totalNodeCount = 0;

    /**
     * @return whether this NiFi instance is clustered
     */
    @Schema(description = "Whether this NiFi instance is clustered."
    )
    public Boolean getClustered() {
        return isClustered;
    }

    public void setClustered(Boolean clustered) {
        isClustered = clustered;
    }

    /**
     * @return whether this NiFi instance is connected to a cluster
     */
    @Schema(description = "Whether this NiFi instance is connected to a cluster."
    )
    public Boolean getConnectedToCluster() {
        return isConnectedToCluster;
    }

    public void setConnectedToCluster(Boolean connectedToCluster) {
        isConnectedToCluster = connectedToCluster;
    }

    @Schema(description = "The number of nodes that are currently connected to the cluster")
    public Integer getConnectedNodeCount() {
        return connectedNodeCount;
    }

    public void setConnectedNodeCount(Integer connectedNodeCount) {
        this.connectedNodeCount = connectedNodeCount;
    }

    @Schema(description = "The number of nodes in the cluster, regardless of whether or not they are connected")
    public Integer getTotalNodeCount() {
        return totalNodeCount;
    }

    public void setTotalNodeCount(Integer totalNodeCount) {
        this.totalNodeCount = totalNodeCount;
    }

    /**
     * @return Used in clustering, will report the number of nodes connected vs
     * the number of nodes in the cluster
     */
    @Schema(description = "When clustered, reports the number of nodes connected vs the number of nodes in the cluster.")
    public String getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String connectedNodes) {
        this.connectedNodes = connectedNodes;
    }
}
