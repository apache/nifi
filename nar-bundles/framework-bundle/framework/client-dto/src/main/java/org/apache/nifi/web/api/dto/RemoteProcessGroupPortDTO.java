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

import javax.xml.bind.annotation.XmlType;

/**
 * Details of a port in a remote process group.
 */
@XmlType(name = "remoteProcessGroupPort")
public class RemoteProcessGroupPortDTO {

    private String id;
    private String groupId;
    private String name;
    private String comments;
    private Integer concurrentlySchedulableTaskCount;
    private Boolean transmitting;
    private Boolean useCompression;
    private Boolean exists;
    private Boolean targetRunning;
    private Boolean connected;

    /**
     * The comments as configured in the target port.
     *
     * @return
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * The number tasks that may transmit flow files to the target port
     * concurrently.
     *
     * @return
     */
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    /**
     * The id of the target port.
     *
     * @return
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The id of the remote process group that this port resides in.
     *
     * @return
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * The name of the target port.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Whether or not this remote group port is configured for transmission.
     *
     * @return
     */
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * Whether or not flow file are compressed when sent to this target port.
     *
     * @return
     */
    public Boolean getUseCompression() {
        return useCompression;
    }

    public void setUseCompression(Boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * Whether or not the target port exists.
     *
     * @return
     */
    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    /**
     * Whether or not the target port is running.
     *
     * @return
     */
    public Boolean isTargetRunning() {
        return targetRunning;
    }

    public void setTargetRunning(Boolean targetRunning) {
        this.targetRunning = targetRunning;
    }

    /**
     * Whether or not this port has either an incoming or outgoing connection.
     *
     * @return
     */
    public Boolean isConnected() {
        return connected;
    }

    public void setConnected(Boolean connected) {
        this.connected = connected;
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
