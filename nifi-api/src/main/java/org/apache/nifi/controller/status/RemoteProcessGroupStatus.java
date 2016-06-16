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
package org.apache.nifi.controller.status;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RemoteProcessGroupStatus implements Cloneable {

    private String id;
    private String groupId;
    private TransmissionStatus transmissionStatus;
    private String uri;
    private String name;
    private Integer activeThreadCount;
    private int sentCount;
    private long sentContentSize;
    private int receivedCount;
    private long receivedContentSize;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private long averageLineageDuration;

    public String getTargetUri() {
        return uri;
    }

    public void setTargetUri(String uri) {
        this.uri = uri;
    }

    public TransmissionStatus getTransmissionStatus() {
        return transmissionStatus;
    }

    public void setTransmissionStatus(TransmissionStatus transmissionStatus) {
        this.transmissionStatus = transmissionStatus;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    public Integer getSentCount() {
        return sentCount;
    }

    public void setSentCount(Integer sentCount) {
        this.sentCount = sentCount;
    }

    public Long getSentContentSize() {
        return sentContentSize;
    }

    public void setSentContentSize(Long sentContentSize) {
        this.sentContentSize = sentContentSize;
    }

    public Integer getReceivedCount() {
        return receivedCount;
    }

    public void setReceivedCount(Integer receivedCount) {
        this.receivedCount = receivedCount;
    }

    public Long getReceivedContentSize() {
        return receivedContentSize;
    }

    public void setReceivedContentSize(Long receivedContentSize) {
        this.receivedContentSize = receivedContentSize;
    }

    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    public long getAverageLineageDuration() {
        return averageLineageDuration;
    }

    public void setAverageLineageDuration(final long millis) {
        this.averageLineageDuration = millis;
    }

    public long getAverageLineageDuration(final TimeUnit timeUnit) {
        return TimeUnit.MILLISECONDS.convert(averageLineageDuration, timeUnit);
    }

    public void setAverageLineageDuration(final long duration, final TimeUnit timeUnit) {
        this.averageLineageDuration = timeUnit.toMillis(duration);
    }

    @Override
    public RemoteProcessGroupStatus clone() {
        final RemoteProcessGroupStatus clonedObj = new RemoteProcessGroupStatus();
        clonedObj.id = id;
        clonedObj.groupId = groupId;
        clonedObj.name = name;
        clonedObj.uri = uri;
        clonedObj.activeThreadCount = activeThreadCount;
        clonedObj.transmissionStatus = transmissionStatus;
        clonedObj.sentCount = sentCount;
        clonedObj.sentContentSize = sentContentSize;
        clonedObj.receivedCount = receivedCount;
        clonedObj.receivedContentSize = receivedContentSize;
        clonedObj.activeRemotePortCount = activeRemotePortCount;
        clonedObj.inactiveRemotePortCount = inactiveRemotePortCount;
        clonedObj.averageLineageDuration = averageLineageDuration;
        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RemoteProcessGroupStatus [id=");
        builder.append(id);
        builder.append(", groupId=");
        builder.append(groupId);
        builder.append(", name=");
        builder.append(name);
        builder.append(", uri=");
        builder.append(uri);
        builder.append(", activeThreadCount=");
        builder.append(activeThreadCount);
        builder.append(", transmissionStatus=");
        builder.append(transmissionStatus);
        builder.append(", sentCount=");
        builder.append(sentCount);
        builder.append(", sentContentSize=");
        builder.append(sentContentSize);
        builder.append(", receivedCount=");
        builder.append(receivedCount);
        builder.append(", receivedContentSize=");
        builder.append(receivedContentSize);
        builder.append(", activeRemotePortCount=");
        builder.append(activeRemotePortCount);
        builder.append(", inactiveRemotePortCount=");
        builder.append(inactiveRemotePortCount);
        builder.append("]");
        return builder.toString();
    }
}
