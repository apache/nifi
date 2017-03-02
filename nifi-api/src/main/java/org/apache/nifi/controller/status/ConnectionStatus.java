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

import org.apache.nifi.processor.DataUnit;

/**
 */
public class ConnectionStatus implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String sourceId;
    private String sourceName;
    private String destinationId;
    private String destinationName;
    private String backPressureDataSizeThreshold;
    private long backPressureBytesThreshold;
    private long backPressureObjectThreshold;
    private int inputCount;
    private long inputBytes;
    private int queuedCount;
    private long queuedBytes;
    private int outputCount;
    private long outputBytes;
    private int maxQueuedCount;
    private long maxQueuedBytes;

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

    public int getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(final int queuedCount) {
        this.queuedCount = queuedCount;
    }

    public long getQueuedBytes() {
        return queuedBytes;
    }

    public void setQueuedBytes(final long queuedBytes) {
        this.queuedBytes = queuedBytes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public String getBackPressureDataSizeThreshold() {
        return backPressureDataSizeThreshold;
    }

    public void setBackPressureDataSizeThreshold(String backPressureDataSizeThreshold) {
        this.backPressureDataSizeThreshold = backPressureDataSizeThreshold;
        setBackPressureBytesThreshold(DataUnit.parseDataSize(backPressureDataSizeThreshold, DataUnit.B).longValue());
    }

    public long getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    public void setBackPressureObjectThreshold(long backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public int getInputCount() {
        return inputCount;
    }

    public void setInputCount(int inputCount) {
        this.inputCount = inputCount;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
    }

    public int getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(int outputCount) {
        this.outputCount = outputCount;
    }

    public int getMaxQueuedCount() {
        return maxQueuedCount;
    }

    public void setMaxQueuedCount(int maxQueuedCount) {
        this.maxQueuedCount = maxQueuedCount;
    }

    public long getMaxQueuedBytes() {
        return maxQueuedBytes;
    }

    public void setMaxQueuedBytes(long maxQueueBytes) {
        this.maxQueuedBytes = maxQueueBytes;
    }

    public long getBackPressureBytesThreshold() {
        return backPressureBytesThreshold;
    }

    public void setBackPressureBytesThreshold(long backPressureBytesThreshold) {
        this.backPressureBytesThreshold = backPressureBytesThreshold;
    }

    @Override
    public ConnectionStatus clone() {
        final ConnectionStatus clonedObj = new ConnectionStatus();
        clonedObj.groupId = groupId;
        clonedObj.id = id;
        clonedObj.inputBytes = inputBytes;
        clonedObj.inputCount = inputCount;
        clonedObj.name = name;
        clonedObj.outputBytes = outputBytes;
        clonedObj.outputCount = outputCount;
        clonedObj.queuedBytes = queuedBytes;
        clonedObj.queuedCount = queuedCount;
        clonedObj.sourceId = sourceId;
        clonedObj.sourceName = sourceName;
        clonedObj.destinationId = destinationId;
        clonedObj.destinationName = destinationName;
        clonedObj.backPressureDataSizeThreshold = backPressureDataSizeThreshold;
        clonedObj.backPressureObjectThreshold = backPressureObjectThreshold;
        clonedObj.maxQueuedBytes = maxQueuedBytes;
        clonedObj.maxQueuedCount = maxQueuedCount;
        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ConnectionStatus [id=");
        builder.append(id);
        builder.append(", groupId=");
        builder.append(groupId);
        builder.append(", name=");
        builder.append(name);
        builder.append(", sourceId=");
        builder.append(sourceId);
        builder.append(", sourceName=");
        builder.append(sourceName);
        builder.append(", destinationId=");
        builder.append(destinationId);
        builder.append(", destinationName=");
        builder.append(destinationName);
        builder.append(", backPressureDataSizeThreshold=");
        builder.append(backPressureDataSizeThreshold);
        builder.append(", backPressureObjectThreshold=");
        builder.append(backPressureObjectThreshold);
        builder.append(", inputCount=");
        builder.append(inputCount);
        builder.append(", inputBytes=");
        builder.append(inputBytes);
        builder.append(", queuedCount=");
        builder.append(queuedCount);
        builder.append(", queuedBytes=");
        builder.append(queuedBytes);
        builder.append(", outputCount=");
        builder.append(outputCount);
        builder.append(", outputBytes=");
        builder.append(outputBytes);
        builder.append(", maxQueuedCount=");
        builder.append(maxQueuedCount);
        builder.append(", maxQueueBytes=");
        builder.append(maxQueuedBytes);
        builder.append("]");
        return builder.toString();
    }
}
