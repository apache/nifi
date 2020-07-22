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

import java.util.ArrayList;
import java.util.List;

/**
 * The status of a NiFi node.
 */
public class NodeStatus implements Cloneable {
    private long createdAtInMs;

    private long freeHeap;
    private long usedHeap;
    private long heapUtilization;
    private long freeNonHeap;
    private long usedNonHeap;

    private long openFileHandlers;
    private double processorLoadAverage;

    private long totalThreads;
    private long eventDrivenThreads;
    private long timerDrivenThreads;

    private long flowFileRepositoryFreeSpace;
    private long flowFileRepositoryUsedSpace;

    private List<StorageStatus> contentRepositories = new ArrayList<>();
    private List<StorageStatus> provenanceRepositories = new ArrayList<>();

    public long getCreatedAtInMs() {
        return createdAtInMs;
    }

    public void setCreatedAtInMs(final long createdAtInMs) {
        this.createdAtInMs = createdAtInMs;
    }

    public long getFreeHeap() {
        return freeHeap;
    }

    public void setFreeHeap(final long freeHeap) {
        this.freeHeap = freeHeap;
    }

    public long getUsedHeap() {
        return usedHeap;
    }

    public void setUsedHeap(final long usedHeap) {
        this.usedHeap = usedHeap;
    }

    public long getHeapUtilization() {
        return heapUtilization;
    }

    public void setHeapUtilization(final long heapUtilization) {
        this.heapUtilization = heapUtilization;
    }

    public long getFreeNonHeap() {
        return freeNonHeap;
    }

    public void setFreeNonHeap(final long freeNonHeap) {
        this.freeNonHeap = freeNonHeap;
    }

    public long getUsedNonHeap() {
        return usedNonHeap;
    }

    public void setUsedNonHeap(final long usedNonHeap) {
        this.usedNonHeap = usedNonHeap;
    }

    public long getOpenFileHandlers() {
        return openFileHandlers;
    }

    public void setOpenFileHandlers(final long openFileHandlers) {
        this.openFileHandlers = openFileHandlers;
    }

    public double getProcessorLoadAverage() {
        return processorLoadAverage;
    }

    public void setProcessorLoadAverage(final double processorLoadAverage) {
        this.processorLoadAverage = processorLoadAverage;
    }

    public long getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(final long totalThreads) {
        this.totalThreads = totalThreads;
    }

    public long getEventDrivenThreads() {
        return eventDrivenThreads;
    }

    public void setEventDrivenThreads(final long eventDrivenThreads) {
        this.eventDrivenThreads = eventDrivenThreads;
    }

    public long getTimerDrivenThreads() {
        return timerDrivenThreads;
    }

    public void setTimerDrivenThreads(final long timerDrivenThreads) {
        this.timerDrivenThreads = timerDrivenThreads;
    }

    public long getFlowFileRepositoryFreeSpace() {
        return flowFileRepositoryFreeSpace;
    }

    public void setFlowFileRepositoryFreeSpace(final long flowFileRepositoryFreeSpace) {
        this.flowFileRepositoryFreeSpace = flowFileRepositoryFreeSpace;
    }

    public long getFlowFileRepositoryUsedSpace() {
        return flowFileRepositoryUsedSpace;
    }

    public void setFlowFileRepositoryUsedSpace(final long flowFileRepositoryUsedSpace) {
        this.flowFileRepositoryUsedSpace = flowFileRepositoryUsedSpace;
    }

    public List<StorageStatus> getContentRepositories() {
        return contentRepositories;
    }

    public void setContentRepositories(final List<StorageStatus> contentRepositories) {
        this.contentRepositories = new ArrayList<>();
        this.contentRepositories.addAll(contentRepositories);
    }

    public List<StorageStatus> getProvenanceRepositories() {
        return provenanceRepositories;
    }

    public void setProvenanceRepositories(final List<StorageStatus> provenanceRepositories) {
        this.provenanceRepositories = new ArrayList<>();
        this.provenanceRepositories.addAll(provenanceRepositories);
    }

    @Override
    protected NodeStatus clone() {
        final NodeStatus clonedObj = new NodeStatus();
        clonedObj.createdAtInMs = createdAtInMs;
        clonedObj.freeHeap = freeHeap;
        clonedObj.usedHeap = usedHeap;
        clonedObj.heapUtilization = heapUtilization;
        clonedObj.freeNonHeap = freeNonHeap;
        clonedObj.usedNonHeap = usedNonHeap;
        clonedObj.openFileHandlers = openFileHandlers;
        clonedObj.processorLoadAverage = processorLoadAverage;
        clonedObj.totalThreads = totalThreads;
        clonedObj.eventDrivenThreads = eventDrivenThreads;
        clonedObj.timerDrivenThreads = timerDrivenThreads;
        clonedObj.flowFileRepositoryFreeSpace = flowFileRepositoryFreeSpace;
        clonedObj.flowFileRepositoryUsedSpace = flowFileRepositoryUsedSpace;

        final List<StorageStatus> clonedContentRepositories = new ArrayList<>();
        contentRepositories.stream().map(r -> r.clone()).forEach(r -> clonedContentRepositories.add(r));
        clonedObj.contentRepositories = clonedContentRepositories;

        final List<StorageStatus> clonedProvenanceRepositories = new ArrayList<>();
        provenanceRepositories.stream().map(r -> r.clone()).forEach(r -> clonedProvenanceRepositories.add(r));
        clonedObj.provenanceRepositories = clonedProvenanceRepositories;

        return clonedObj;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NodeStatus{");
        sb.append("createdAtInMs=").append(createdAtInMs);
        sb.append(", freeHeap=").append(freeHeap);
        sb.append(", usedHeap=").append(usedHeap);
        sb.append(", heapUtilization=").append(heapUtilization);
        sb.append(", freeNonHeap=").append(freeNonHeap);
        sb.append(", usedNonHeap=").append(usedNonHeap);
        sb.append(", openFileHandlers=").append(openFileHandlers);
        sb.append(", processorLoadAverage=").append(processorLoadAverage);
        sb.append(", totalThreads=").append(totalThreads);
        sb.append(", eventDrivenThreads=").append(eventDrivenThreads);
        sb.append(", timerDrivenThreads=").append(timerDrivenThreads);
        sb.append(", flowFileRepositoryFreeSpace=").append(flowFileRepositoryFreeSpace);
        sb.append(", flowFileRepositoryUsedSpace=").append(flowFileRepositoryUsedSpace);
        sb.append(", contentRepositories=").append(contentRepositories);
        sb.append(", provenanceRepositories=").append(provenanceRepositories);
        sb.append('}');
        return sb.toString();
    }
}
