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

import javax.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessGroupUploadDTO.
 */
@XmlRootElement(name = "processGroupUploadEntity")
public class ProcessGroupUploadEntity extends Entity {
    private String id;
    private String groupName;
    private Double positionX;
    private Double positionY;
    private String clientId;
    private Boolean disconnectedNodeAcknowledged;
    private VersionedFlowSnapshot versionedFlowSnapshot;

    /**
     * @return The group ID
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The process group name
     */
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * @return The process group X-position
     */
    public Double getPositionX() {
        return positionX;
    }

    public void setPositionX(Double positionX) {
        this.positionX = positionX;
    }

    /**
     * @return The process group Y-position
     */
    public Double getPositionY() {
        return positionY;
    }

    public void setPositionY(Double positionY) {
        this.positionY = positionY;
    }

    /**
     * @return The client ID
     */
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return Acknowledges if a node is disconnected from a cluster
     */
    public Boolean getDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }

    /**
     * @return The uploaded file of a VersionedFlowSnapshot
     */
    public VersionedFlowSnapshot getFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public void setFlowSnapshot(VersionedFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }
}