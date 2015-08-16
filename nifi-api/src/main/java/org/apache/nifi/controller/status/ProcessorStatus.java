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
 */
public class ProcessorStatus implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private RunStatus runStatus;
    private int inputCount;
    private long inputBytes;
    private int outputCount;
    private long outputBytes;
    private long bytesRead;
    private long bytesWritten;
    private int invocations;
    private long processingNanos;
    private int flowFilesRemoved;
    private long averageLineageDuration;
    private int activeThreadCount;
    private int flowFilesReceived;
    private long bytesReceived;
    private int flowFilesSent;
    private long bytesSent;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public int getInputCount() {
        return inputCount;
    }

    public RunStatus getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(RunStatus runStatus) {
        this.runStatus = runStatus;
    }

    public void setInputCount(final int inputCount) {
        this.inputCount = inputCount;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(final long inputBytes) {
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

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(final long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(final long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public int getInvocations() {
        return invocations;
    }

    public void setInvocations(final int invocations) {
        this.invocations = invocations;
    }

    public long getProcessingNanos() {
        return processingNanos;
    }

    public void setProcessingNanos(final long processingNanos) {
        this.processingNanos = processingNanos;
    }

    public long getAverageLineageDuration(final TimeUnit timeUnit) {
        return TimeUnit.MILLISECONDS.convert(averageLineageDuration, timeUnit);
    }

    public void setAverageLineageDuration(final long duration, final TimeUnit timeUnit) {
        this.averageLineageDuration = timeUnit.toMillis(duration);
    }

    public long getAverageLineageDuration() {
        return averageLineageDuration;
    }

    public void setAverageLineageDuration(final long millis) {
        this.averageLineageDuration = millis;
    }

    public int getFlowFilesRemoved() {
        return flowFilesRemoved;
    }

    public void setFlowFilesRemoved(int flowFilesRemoved) {
        this.flowFilesRemoved = flowFilesRemoved;
    }

    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(final int activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
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

    @Override
    public ProcessorStatus clone() {
        final ProcessorStatus clonedObj = new ProcessorStatus();
        clonedObj.activeThreadCount = activeThreadCount;
        clonedObj.bytesRead = bytesRead;
        clonedObj.bytesWritten = bytesWritten;
        clonedObj.flowFilesReceived = flowFilesReceived;
        clonedObj.bytesReceived = bytesReceived;
        clonedObj.flowFilesSent = flowFilesSent;
        clonedObj.bytesSent = bytesSent;
        clonedObj.groupId = groupId;
        clonedObj.id = id;
        clonedObj.inputBytes = inputBytes;
        clonedObj.inputCount = inputCount;
        clonedObj.invocations = invocations;
        clonedObj.name = name;
        clonedObj.outputBytes = outputBytes;
        clonedObj.outputCount = outputCount;
        clonedObj.processingNanos = processingNanos;
        clonedObj.averageLineageDuration = averageLineageDuration;
        clonedObj.flowFilesRemoved = flowFilesRemoved;
        clonedObj.runStatus = runStatus;
        clonedObj.type = type;
        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProcessorStatus [id=");
        builder.append(id);
        builder.append(", groupId=");
        builder.append(groupId);
        builder.append(", name=");
        builder.append(name);
        builder.append(", type=");
        builder.append(type);
        builder.append(", runStatus=");
        builder.append(runStatus);
        builder.append(", inputCount=");
        builder.append(inputCount);
        builder.append(", inputBytes=");
        builder.append(inputBytes);
        builder.append(", outputCount=");
        builder.append(outputCount);
        builder.append(", outputBytes=");
        builder.append(outputBytes);
        builder.append(", bytesRead=");
        builder.append(bytesRead);
        builder.append(", bytesWritten=");
        builder.append(bytesWritten);
        builder.append(", invocations=");
        builder.append(invocations);
        builder.append(", processingNanos=");
        builder.append(processingNanos);
        builder.append(", activeThreadCount=");
        builder.append(activeThreadCount);
        builder.append("]");
        return builder.toString();
    }
}
