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
package org.apache.nifi.diagnostics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Diagnostics for the JVM.
 *
 */
public class SystemDiagnostics implements Cloneable {

    private long totalNonHeap;
    private long usedNonHeap;
    private long maxNonHeap;

    private long totalHeap;
    private long usedHeap;
    private long maxHeap;

    private int availableProcessors;
    private Double processorLoadAverage;

    private int totalThreads;
    private int daemonThreads;

    private long uptime;

    private StorageUsage flowFileRepositoryStorageUsage;
    private Map<String, StorageUsage> contentRepositoryStorageUsage;
    private Map<String, StorageUsage> provenanceRepositoryStorageUsage;
    private Map<String, GarbageCollection> garbageCollection;

    private long creationTimestamp;

    public void setTotalNonHeap(final long totalNonHeap) {
        this.totalNonHeap = totalNonHeap;
    }

    public void setUsedNonHeap(final long usedNonHeap) {
        this.usedNonHeap = usedNonHeap;
    }

    public void setMaxNonHeap(final long maxNonHeap) {
        this.maxNonHeap = maxNonHeap;
    }

    public void setTotalHeap(final long totalHeap) {
        this.totalHeap = totalHeap;
    }

    public void setUsedHeap(final long usedHeap) {
        this.usedHeap = usedHeap;
    }

    public void setMaxHeap(final long maxHeap) {
        this.maxHeap = maxHeap;
    }

    public void setAvailableProcessors(final int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public void setProcessorLoadAverage(final Double processorLoadAverage) {
        this.processorLoadAverage = processorLoadAverage;
    }

    public void setTotalThreads(final int totalThreads) {
        this.totalThreads = totalThreads;
    }

    public void setDaemonThreads(final int daemonThreads) {
        this.daemonThreads = daemonThreads;
    }

    public void setFlowFileRepositoryStorageUsage(final StorageUsage flowFileRepositoryStorageUsage) {
        this.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage;
    }

    public void setContentRepositoryStorageUsage(final Map<String, StorageUsage> contentRepositoryStorageUsage) {
        this.contentRepositoryStorageUsage = contentRepositoryStorageUsage;
    }

    public void setProvenanceRepositoryStorageUsage(final Map<String, StorageUsage> provenanceRepositoryStorageUsage) {
        this.provenanceRepositoryStorageUsage = provenanceRepositoryStorageUsage;
    }

    public long getTotalNonHeap() {
        return totalNonHeap;
    }

    public long getUsedNonHeap() {
        return usedNonHeap;
    }

    public long getMaxNonHeap() {
        return maxNonHeap;
    }

    public long getTotalHeap() {
        return totalHeap;
    }

    public long getUsedHeap() {
        return usedHeap;
    }

    public long getMaxHeap() {
        return maxHeap;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public Double getProcessorLoadAverage() {
        return processorLoadAverage;
    }

    public int getTotalThreads() {
        return totalThreads;
    }

    public int getDaemonThreads() {
        return daemonThreads;
    }

    public StorageUsage getFlowFileRepositoryStorageUsage() {
        return flowFileRepositoryStorageUsage;
    }

    public Map<String, StorageUsage> getContentRepositoryStorageUsage() {
        return contentRepositoryStorageUsage;
    }

    public Map<String, StorageUsage> getProvenanceRepositoryStorageUsage() {
        return provenanceRepositoryStorageUsage;
    }

    public long getFreeNonHeap() {
        return totalNonHeap - usedNonHeap;
    }

    public long getFreeHeap() {
        return totalHeap - usedHeap;
    }

    public int getHeapUtilization() {
        if (maxHeap == -1) {
            return -1;
        } else {
            return DiagnosticUtils.getUtilization(usedHeap, maxHeap);
        }
    }

    public int getNonHeapUtilization() {
        if (maxNonHeap == -1) {
            return -1;
        } else {
            return DiagnosticUtils.getUtilization(usedNonHeap, maxNonHeap);
        }
    }

    public Map<String, GarbageCollection> getGarbageCollection() {
        return garbageCollection;
    }

    public void setGarbageCollection(Map<String, GarbageCollection> garbageCollection) {
        this.garbageCollection = garbageCollection;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    @Override
    public SystemDiagnostics clone() {
        final SystemDiagnostics clonedObj = new SystemDiagnostics();
        clonedObj.availableProcessors = availableProcessors;
        clonedObj.daemonThreads = daemonThreads;
        if (flowFileRepositoryStorageUsage != null) {
            clonedObj.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage.clone();
        }
        if (contentRepositoryStorageUsage != null) {
            final Map<String, StorageUsage> clonedMap = new LinkedHashMap<>();
            clonedObj.setContentRepositoryStorageUsage(clonedMap);
            for (final Map.Entry<String, StorageUsage> entry : contentRepositoryStorageUsage.entrySet()) {
                clonedMap.put(entry.getKey(), entry.getValue().clone());
            }
        }
        if(provenanceRepositoryStorageUsage != null) {
            final Map<String, StorageUsage> clonedMap = new LinkedHashMap<>();
            clonedObj.setProvenanceRepositoryStorageUsage(clonedMap);
            for (final Map.Entry<String, StorageUsage> entry : provenanceRepositoryStorageUsage.entrySet()) {
                clonedMap.put(entry.getKey(), entry.getValue().clone());
            }
        }
        if (garbageCollection != null) {
            final Map<String, GarbageCollection> clonedMap = new LinkedHashMap<>();
            clonedObj.setGarbageCollection(clonedMap);
            for (final Map.Entry<String, GarbageCollection> entry : garbageCollection.entrySet()) {
                clonedMap.put(entry.getKey(), entry.getValue().clone());
            }
        }
        clonedObj.maxHeap = maxHeap;
        clonedObj.maxNonHeap = maxNonHeap;
        clonedObj.processorLoadAverage = processorLoadAverage;
        clonedObj.totalHeap = totalHeap;
        clonedObj.totalNonHeap = totalNonHeap;
        clonedObj.totalThreads = totalThreads;
        clonedObj.usedHeap = usedHeap;
        clonedObj.usedNonHeap = usedNonHeap;
        clonedObj.creationTimestamp = creationTimestamp;
        clonedObj.uptime = uptime;

        return clonedObj;
    }

}
