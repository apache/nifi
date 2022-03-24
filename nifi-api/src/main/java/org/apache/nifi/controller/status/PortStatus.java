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

/**
 * The status of a port.
 */
public class PortStatus implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private Integer activeThreadCount;
    private int inputCount;
    private long inputBytes;
    private int outputCount;
    private long outputBytes;
    private int flowFilesReceived;
    private long bytesReceived;
    private int flowFilesSent;
    private long bytesSent;
    private Boolean transmitting;
    private RunStatus runStatus;

    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    public int getInputCount() {
        return inputCount;
    }

    public void setInputCount(int inputCount) {
        this.inputCount = inputCount;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public int getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(final int outputCount) {
        this.outputCount = outputCount;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(final long outputBytes) {
        this.outputBytes = outputBytes;
    }

    public RunStatus getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(RunStatus runStatus) {
        this.runStatus = runStatus;
    }

    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(int flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(int flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(long bytesSent) {
        this.bytesSent = bytesSent;
    }

    public Boolean getTransmitting() {
        return transmitting;
    }

    @Override
    public PortStatus clone() {
        final PortStatus clonedObj = new PortStatus();
        clonedObj.id = id;
        clonedObj.groupId = groupId;
        clonedObj.name = name;
        clonedObj.activeThreadCount = activeThreadCount;
        clonedObj.inputBytes = inputBytes;
        clonedObj.inputCount = inputCount;
        clonedObj.outputBytes = outputBytes;
        clonedObj.outputCount = outputCount;
        clonedObj.flowFilesReceived = flowFilesReceived;
        clonedObj.bytesReceived = bytesReceived;
        clonedObj.flowFilesSent = flowFilesSent;
        clonedObj.bytesSent = bytesSent;
        clonedObj.transmitting = transmitting;
        clonedObj.runStatus = runStatus;
        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PortStatus [id=");
        builder.append(id);
        builder.append(", groupId=");
        builder.append(groupId);
        builder.append(", name=");
        builder.append(name);
        builder.append(", activeThreadCount=");
        builder.append(activeThreadCount);
        builder.append(", transmitting=");
        builder.append(transmitting);
        builder.append(", inputCount=");
        builder.append(inputCount);
        builder.append(", inputBytes=");
        builder.append(inputBytes);
        builder.append(", outputCount=");
        builder.append(outputCount);
        builder.append(", outputBytes=");
        builder.append(outputBytes);
        builder.append(", runStatus=");
        builder.append(runStatus);
        builder.append("]");
        return builder.toString();
    }
}
