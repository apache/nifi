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

package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModelProperty;

public class VersionedPort extends VersionedComponent {
    private PortType type;
    private Integer concurrentlySchedulableTaskCount;
    private ScheduledState scheduledState;
    private boolean allowRemoteAccess;

    @ApiModelProperty("The number of tasks that should be concurrently scheduled for the port.")
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    @ApiModelProperty("The type of port.")
    public PortType getType() {
        return type;
    }

    public void setType(PortType type) {
        this.type = type;
    }

    @ApiModelProperty("The scheduled state of the component")
    public ScheduledState getScheduledState() {
        return scheduledState;
    }

    public void setScheduledState(ScheduledState scheduledState) {
        this.scheduledState = scheduledState;
    }

    @ApiModelProperty("Whether or not this port allows remote access for site-to-site")
    public boolean isAllowRemoteAccess() {
        return allowRemoteAccess;
    }

    public void setAllowRemoteAccess(boolean allowRemoteAccess) {
        this.allowRemoteAccess = allowRemoteAccess;
    }

    @Override
    public ComponentType getComponentType() {
        if (type == PortType.OUTPUT_PORT) {
            return ComponentType.OUTPUT_PORT;
        }

        return ComponentType.INPUT_PORT;
    }
}
