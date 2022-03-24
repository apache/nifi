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

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "jvmControllerDiagnosticsSnapshot")
public class JVMControllerDiagnosticsSnapshotDTO implements Cloneable {
    private Boolean primaryNode;
    private Boolean clusterCoordinator;
    private Integer maxTimerDrivenThreads;
    private Integer maxEventDrivenThreads;

    @ApiModelProperty("Whether or not this node is primary node")
    public Boolean getPrimaryNode() {
        return primaryNode;
    }

    public void setPrimaryNode(Boolean primaryNode) {
        this.primaryNode = primaryNode;
    }

    @ApiModelProperty("Whether or not this node is cluster coordinator")
    public Boolean getClusterCoordinator() {
        return clusterCoordinator;
    }

    public void setClusterCoordinator(Boolean clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @ApiModelProperty("The maximum number of timer-driven threads")
    public Integer getMaxTimerDrivenThreads() {
        return maxTimerDrivenThreads;
    }

    public void setMaxTimerDrivenThreads(Integer maxTimerDrivenThreads) {
        this.maxTimerDrivenThreads = maxTimerDrivenThreads;
    }

    @ApiModelProperty("The maximum number of event-driven threads")
    public Integer getMaxEventDrivenThreads() {
        return maxEventDrivenThreads;
    }

    public void setMaxEventDrivenThreads(Integer maxEventDrivenThreads) {
        this.maxEventDrivenThreads = maxEventDrivenThreads;
    }

    @Override
    public JVMControllerDiagnosticsSnapshotDTO clone() {
        final JVMControllerDiagnosticsSnapshotDTO clone = new JVMControllerDiagnosticsSnapshotDTO();
        clone.clusterCoordinator = clusterCoordinator;
        clone.primaryNode = primaryNode;
        clone.maxEventDrivenThreads = maxEventDrivenThreads;
        clone.maxTimerDrivenThreads = maxTimerDrivenThreads;
        return clone;
    }

}
