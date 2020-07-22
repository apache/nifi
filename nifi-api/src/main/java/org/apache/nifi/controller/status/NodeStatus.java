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
    private long uptime;
    private long totalThreads;

    private long flowFileRepositoryFreeSpace;
    private long flowFileRepositoryUsedSpace;

    private long contentRepositoryFreeSpace;
    private long contentRepositoryUsedSpace;

    private long provenanceRepositoryFreeSpace;
    private long provenanceRepositoryUsedSpace;

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

    public long getUptime() {
        return uptime;
    }

    public void setUptime(final long uptime) {
        this.uptime = uptime;
    }

    public long getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(final long totalThreads) {
        this.totalThreads = totalThreads;
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

    public long getContentRepositoryFreeSpace() {
        return contentRepositoryFreeSpace;
    }

    public void setContentRepositoryFreeSpace(final long contentRepositoryFreeSpace) {
        this.contentRepositoryFreeSpace = contentRepositoryFreeSpace;
    }

    public long getContentRepositoryUsedSpace() {
        return contentRepositoryUsedSpace;
    }

    public void setContentRepositoryUsedSpace(final long contentRepositoryUsedSpace) {
        this.contentRepositoryUsedSpace = contentRepositoryUsedSpace;
    }

    public long getProvenanceRepositoryFreeSpace() {
        return provenanceRepositoryFreeSpace;
    }

    public void setProvenanceRepositoryFreeSpace(final long provenanceRepositoryFreeSpace) {
        this.provenanceRepositoryFreeSpace = provenanceRepositoryFreeSpace;
    }

    public long getProvenanceRepositoryUsedSpace() {
        return provenanceRepositoryUsedSpace;
    }

    public void setProvenanceRepositoryUsedSpace(final long provenanceRepositoryUsedSpace) {
        this.provenanceRepositoryUsedSpace = provenanceRepositoryUsedSpace;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("NodeStatus{");
        builder.append("createdAtInMs=").append(createdAtInMs);
        builder.append(", freeHeap=").append(freeHeap);
        builder.append(", usedHeap=").append(usedHeap);
        builder.append(", heapUtilization=").append(heapUtilization);
        builder.append(", freeNonHeap=").append(freeNonHeap);
        builder.append(", usedNonHeap=").append(usedNonHeap);
        builder.append(", openFileHandlers=").append(openFileHandlers);
        builder.append(", processorLoadAverage=").append(processorLoadAverage);
        builder.append(", uptime=").append(uptime);
        builder.append(", totalThreads=").append(totalThreads);
        builder.append(", flowFileRepositoryFreeSpace=").append(flowFileRepositoryFreeSpace);
        builder.append(", flowFileRepositoryUsedSpace=").append(flowFileRepositoryUsedSpace);
        builder.append(", contentRepositoryFreeSpace=").append(contentRepositoryFreeSpace);
        builder.append(", contentRepositoryUsedSpace=").append(contentRepositoryUsedSpace);
        builder.append(", provenanceRepositoryFreeSpace=").append(provenanceRepositoryFreeSpace);
        builder.append(", provenanceRepositoryUsedSpace=").append(provenanceRepositoryUsedSpace);
        builder.append('}');
        return builder.toString();
    }
}
