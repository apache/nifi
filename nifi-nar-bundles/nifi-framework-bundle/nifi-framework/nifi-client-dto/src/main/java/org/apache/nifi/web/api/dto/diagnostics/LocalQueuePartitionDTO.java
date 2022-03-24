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

package org.apache.nifi.web.api.dto.diagnostics;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "localQueuePartition")
public class LocalQueuePartitionDTO {
    private int totalFlowFileCount;
    private long totalByteCount;
    private int activeQueueFlowFileCount;
    private long activeQueueByteCount;
    private int swapFlowFileCount;
    private long swapByteCount;
    private int swapFiles;
    private int inFlightFlowFileCount;
    private long inFlightByteCount;
    private Boolean allActiveQueueFlowFilesPenalized;
    private Boolean anyActiveQueueFlowFilesPenalized;

    @ApiModelProperty("Total number of FlowFiles owned by the Connection")
    public int getTotalFlowFileCount() {
        return totalFlowFileCount;
    }

    public void setTotalFlowFileCount(int totalFlowFileCount) {
        this.totalFlowFileCount = totalFlowFileCount;
    }

    @ApiModelProperty("Total number of bytes that make up the content for the FlowFiles owned by this Connection")
    public long getTotalByteCount() {
        return totalByteCount;
    }

    public void setTotalByteCount(long totalByteCount) {
        this.totalByteCount = totalByteCount;
    }

    @ApiModelProperty("Total number of FlowFiles that exist in the Connection's Active Queue, immediately available to be offered up to a component")
    public int getActiveQueueFlowFileCount() {
        return activeQueueFlowFileCount;
    }

    public void setActiveQueueFlowFileCount(int activeQueueFlowFileCount) {
        this.activeQueueFlowFileCount = activeQueueFlowFileCount;
    }

    @ApiModelProperty("Total number of bytes that make up the content for the FlowFiles that are present in the Connection's Active Queue")
    public long getActiveQueueByteCount() {
        return activeQueueByteCount;
    }

    public void setActiveQueueByteCount(long activeQueueByteCount) {
        this.activeQueueByteCount = activeQueueByteCount;
    }

    @ApiModelProperty("The total number of FlowFiles that are swapped out for this Connection")
    public int getSwapFlowFileCount() {
        return swapFlowFileCount;
    }

    public void setSwapFlowFileCount(int swapFlowFileCount) {
        this.swapFlowFileCount = swapFlowFileCount;
    }

    @ApiModelProperty("Total number of bytes that make up the content for the FlowFiles that are swapped out to disk for the Connection")
    public long getSwapByteCount() {
        return swapByteCount;
    }

    public void setSwapByteCount(long swapByteCount) {
        this.swapByteCount = swapByteCount;
    }

    @ApiModelProperty("The number of Swap Files that exist for this Connection")
    public int getSwapFiles() {
        return swapFiles;
    }

    public void setSwapFiles(int swapFiles) {
        this.swapFiles = swapFiles;
    }

    @ApiModelProperty("The number of In-Flight FlowFiles for this Connection. These are FlowFiles that belong to the connection but are currently being operated on by a Processor, Port, etc.")
    public int getInFlightFlowFileCount() {
        return inFlightFlowFileCount;
    }

    public void setInFlightFlowFileCount(int inFlightFlowFileCount) {
        this.inFlightFlowFileCount = inFlightFlowFileCount;
    }

    @ApiModelProperty("The number bytes that make up the content of the FlowFiles that are In-Flight")
    public long getInFlightByteCount() {
        return inFlightByteCount;
    }

    public void setInFlightByteCount(long inFlightByteCount) {
        this.inFlightByteCount = inFlightByteCount;
    }

    @ApiModelProperty("Whether or not all of the FlowFiles in the Active Queue are penalized")
    public Boolean getAllActiveQueueFlowFilesPenalized() {
        return allActiveQueueFlowFilesPenalized;
    }

    public void setAllActiveQueueFlowFilesPenalized(Boolean allFlowFilesPenalized) {
        this.allActiveQueueFlowFilesPenalized = allFlowFilesPenalized;
    }

    @ApiModelProperty("Whether or not any of the FlowFiles in the Active Queue are penalized")
    public Boolean getAnyActiveQueueFlowFilesPenalized() {
        return anyActiveQueueFlowFilesPenalized;
    }

    public void setAnyActiveQueueFlowFilesPenalized(Boolean anyFlowFilesPenalized) {
        this.anyActiveQueueFlowFilesPenalized = anyFlowFilesPenalized;
    }
}
