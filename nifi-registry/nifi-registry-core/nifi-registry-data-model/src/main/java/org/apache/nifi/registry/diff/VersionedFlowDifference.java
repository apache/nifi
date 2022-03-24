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

package org.apache.nifi.registry.diff;

import io.swagger.annotations.ApiModelProperty;

import java.util.Set;

/**
 * Represents the result of a diff between 2 versions of the same flow.
 * A subset of the model classes in registry.flow.diff for exposing on the API
 * The differences are grouped by component
 */
public class VersionedFlowDifference {
    private String bucketId;
    private String flowId;
    private int versionA;
    private int versionB;
    private Set<ComponentDifferenceGroup> componentDifferenceGroups;

    public Set<ComponentDifferenceGroup> getComponentDifferenceGroups() {
        return componentDifferenceGroups;
    }

    public void setComponentDifferenceGroups(Set<ComponentDifferenceGroup> componentDifferenceGroups) {
        this.componentDifferenceGroups = componentDifferenceGroups;
    }

    @ApiModelProperty("The id of the bucket that the flow is stored in.")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @ApiModelProperty("The id of the flow that is being examined.")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    @ApiModelProperty("The earlier version from the diff operation.")
    public int getVersionA() {
        return versionA;
    }

    public void setVersionA(int versionA) {
        this.versionA = versionA;
    }

    @ApiModelProperty("The latter version from the diff operation.")
    public int getVersionB() {
        return versionB;
    }

    public void setVersionB(int versionB) {
        this.versionB = versionB;
    }
}
