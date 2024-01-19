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
 * Details of a port in a remote process group.
 */
@XmlType(name = "remoteProcessGroupPort")
public class RemoteProcessGroupPortDTO {

    private String id;
    private String targetId;
    private String versionedComponentId;
    private String groupId;
    private String name;
    private String comments;
    private Integer concurrentlySchedulableTaskCount;
    private Boolean transmitting;
    private Boolean useCompression;
    private Boolean exists;
    private Boolean targetRunning;
    private Boolean connected;
    private BatchSettingsDTO batchSettings;

    /**
     * @return comments as configured in the target port
     */
    @Schema(description = "The comments as configured on the target port."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Schema(description = "The ID of the corresponding component that is under version control")
    public String getVersionedComponentId() {
        return versionedComponentId;
    }

    public void setVersionedComponentId(final String id) {
        this.versionedComponentId = id;
    }

    /**
     * @return number tasks that may transmit flow files to the target port concurrently
     */
    @Schema(description = "The number of task that may transmit flowfiles to the target port concurrently."
    )
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    /**
     * @return id of the target port
     */
    @Schema(description = "The id of the port."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Schema(description = "The id of the target port.")
    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    /**
     * @return id of the remote process group that this port resides in
     */
    @Schema(description = "The id of the remote process group that the port resides in."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of the target port
     */
    @Schema(description = "The name of the target port."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return whether or not this remote group port is configured for transmission
     */
    @Schema(description = "Whether the remote port is configured for transmission."
    )
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * @return whether or not flow file are compressed when sent to this target port
     */
    @Schema(description = "Whether the flowfiles are compressed when sent to the target port."
    )
    public Boolean getUseCompression() {
        return useCompression;
    }

    public void setUseCompression(Boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * @return whether or not the target port exists
     */
    @Schema(description = "Whether the target port exists."
    )
    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    /**
     * @return whether or not the target port is running
     */
    @Schema(description = "Whether the target port is running."
    )
    public Boolean isTargetRunning() {
        return targetRunning;
    }

    public void setTargetRunning(Boolean targetRunning) {
        this.targetRunning = targetRunning;
    }

    /**
     * @return whether or not this port has either an incoming or outgoing connection
     */
    @Schema(description = "Whether the port has either an incoming or outgoing connection."
    )
    public Boolean isConnected() {
        return connected;
    }

    public void setConnected(Boolean connected) {
        this.connected = connected;
    }

    /**
     * @return batch settings for data transmission
     */
    @Schema(description = "The batch settings for data transmission."
    )
    public BatchSettingsDTO getBatchSettings() {
        return batchSettings;
    }

    public void setBatchSettings(BatchSettingsDTO batchSettings) {
        this.batchSettings = batchSettings;
    }

    @Override
    public int hashCode() {
        return 923847 + String.valueOf(name).hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RemoteProcessGroupPortDTO)) {
            return false;
        }
        final RemoteProcessGroupPortDTO other = (RemoteProcessGroupPortDTO) obj;
        if (name == null && other.name == null) {
            return true;
        }

        if (name == null) {
            return false;
        }
        return name.equals(other.name);
    }
}
