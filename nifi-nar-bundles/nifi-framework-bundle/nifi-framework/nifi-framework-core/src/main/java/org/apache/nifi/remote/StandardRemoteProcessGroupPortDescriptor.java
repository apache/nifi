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
package org.apache.nifi.remote;

import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;

public class StandardRemoteProcessGroupPortDescriptor implements RemoteProcessGroupPortDescriptor {

    private String id;
    private String groupId;
    private String name;
    private String comments;
    private Integer concurrentlySchedulableTaskCount;
    private Boolean transmitting;
    private Boolean useCompression;
    private Integer batchCount;
    private String batchSize;
    private String batchDuration;
    private Boolean exists;
    private Boolean targetRunning;
    private Boolean connected;

    @Override
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    @Override
    public Boolean getUseCompression() {
        return useCompression;
    }

    public void setUseCompression(Boolean useCompression) {
        this.useCompression = useCompression;
    }

    @Override
    public Integer getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(Integer batchCount) {
        this.batchCount = batchCount;
    }

    @Override
    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public String getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(String batchDuration) {
        this.batchDuration = batchDuration;
    }

    @Override
    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    @Override
    public Boolean isTargetRunning() {
        return targetRunning;
    }

    public void setTargetRunning(Boolean targetRunning) {
        this.targetRunning = targetRunning;
    }

    @Override
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
        if (!(obj instanceof RemoteProcessGroupPortDescriptor)) {
            return false;
        }

        final RemoteProcessGroupPortDescriptor other = (RemoteProcessGroupPortDescriptor) obj;
        if (name == null && other.getName() == null) {
            return true;
        }

        if (name == null) {
            return false;
        }
        return name.equals(other.getName());
    }
}
