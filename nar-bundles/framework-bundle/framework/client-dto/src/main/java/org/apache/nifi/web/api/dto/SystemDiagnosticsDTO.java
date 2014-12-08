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

import java.util.Date;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * The diagnostics of the system this NiFi is running on.
 */
@XmlType(name = "systemDiagnostics")
public class SystemDiagnosticsDTO {

    private String totalNonHeap;
    private String usedNonHeap;
    private String freeNonHeap;
    private String maxNonHeap;
    private String nonHeapUtilization;

    private String totalHeap;
    private String usedHeap;
    private String freeHeap;
    private String maxHeap;
    private String heapUtilization;

    private Integer availableProcessors;
    private Double processorLoadAverage;

    private Integer totalThreads;
    private Integer daemonThreads;

    private StorageUsageDTO flowFileRepositoryStorageUsage;
    private Set<StorageUsageDTO> contentRepositoryStorageUsage;
    private Set<GarbageCollectionDTO> garbageCollection;

    private Date statsLastRefreshed;

    /* getters / setters */
    /**
     * The number of available processors, if supported.
     *
     * @return
     */
    public Integer getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(Integer availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    /**
     * The number of daemon threads.
     *
     * @return
     */
    public Integer getDaemonThreads() {
        return daemonThreads;
    }

    public void setDaemonThreads(Integer daemonThreads) {
        this.daemonThreads = daemonThreads;
    }

    /**
     * The amount of free heap.
     *
     * @return
     */
    public String getFreeHeap() {
        return freeHeap;
    }

    public void setFreeHeap(String freeHeap) {
        this.freeHeap = freeHeap;
    }

    /**
     * The amount of free non-heap.
     *
     * @return
     */
    public String getFreeNonHeap() {
        return freeNonHeap;
    }

    public void setFreeNonHeap(String freeNonHeap) {
        this.freeNonHeap = freeNonHeap;
    }

    /**
     * The max size of the heap.
     *
     * @return
     */
    public String getMaxHeap() {
        return maxHeap;
    }

    public void setMaxHeap(String maxHeap) {
        this.maxHeap = maxHeap;
    }

    /**
     * The max size of the non-heap.
     *
     * @return
     */
    public String getMaxNonHeap() {
        return maxNonHeap;
    }

    public void setMaxNonHeap(String maxNonHeap) {
        this.maxNonHeap = maxNonHeap;
    }

    /**
     * The processor load average, if supported.
     *
     * @return
     */
    public Double getProcessorLoadAverage() {
        return processorLoadAverage;
    }

    public void setProcessorLoadAverage(Double processorLoadAverage) {
        this.processorLoadAverage = processorLoadAverage;
    }

    /**
     * The total size of the heap.
     *
     * @return
     */
    public String getTotalHeap() {
        return totalHeap;
    }

    public void setTotalHeap(String totalHeap) {
        this.totalHeap = totalHeap;
    }

    /**
     * The total size of non-heap.
     *
     * @return
     */
    public String getTotalNonHeap() {
        return totalNonHeap;
    }

    public void setTotalNonHeap(String totalNonHeap) {
        this.totalNonHeap = totalNonHeap;
    }

    /**
     * The total number of threads.
     *
     * @return
     */
    public Integer getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(Integer totalThreads) {
        this.totalThreads = totalThreads;
    }

    /**
     * The amount of used heap.
     *
     * @return
     */
    public String getUsedHeap() {
        return usedHeap;
    }

    public void setUsedHeap(String usedHeap) {
        this.usedHeap = usedHeap;
    }

    /**
     * The amount of used non-heap.
     *
     * @return
     */
    public String getUsedNonHeap() {
        return usedNonHeap;
    }

    public void setUsedNonHeap(String usedNonHeap) {
        this.usedNonHeap = usedNonHeap;
    }

    /**
     * The heap utilization.
     *
     * @return
     */
    public String getHeapUtilization() {
        return heapUtilization;
    }

    public void setHeapUtilization(String heapUtilization) {
        this.heapUtilization = heapUtilization;
    }

    /**
     * The non-heap utilization.
     *
     * @return
     */
    public String getNonHeapUtilization() {
        return nonHeapUtilization;
    }

    public void setNonHeapUtilization(String nonHeapUsage) {
        this.nonHeapUtilization = nonHeapUsage;
    }

    /**
     * The content repository storage usage.
     *
     * @return
     */
    public Set<StorageUsageDTO> getContentRepositoryStorageUsage() {
        return contentRepositoryStorageUsage;
    }

    public void setContentRepositoryStorageUsage(Set<StorageUsageDTO> contentRepositoryStorageUsage) {
        this.contentRepositoryStorageUsage = contentRepositoryStorageUsage;
    }

    /**
     * The flowfile repository storage usage.
     *
     * @return
     */
    public StorageUsageDTO getFlowFileRepositoryStorageUsage() {
        return flowFileRepositoryStorageUsage;
    }

    public void setFlowFileRepositoryStorageUsage(StorageUsageDTO flowFileRepositoryStorageUsage) {
        this.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage;
    }

    /**
     * Garbage collection details.
     *
     * @return
     */
    public Set<GarbageCollectionDTO> getGarbageCollection() {
        return garbageCollection;
    }

    public void setGarbageCollection(Set<GarbageCollectionDTO> garbageCollection) {
        this.garbageCollection = garbageCollection;
    }

    /**
     * When these diagnostics were generated.
     *
     * @return
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    /**
     * Details of storage usage.
     */
    @XmlType(name = "storageUsage")
    public static class StorageUsageDTO {

        private String identifier;
        private String freeSpace;
        private String totalSpace;
        private String usedSpace;
        private Long freeSpaceBytes;
        private Long totalSpaceBytes;
        private Long usedSpaceBytes;
        private String utilization;

        /**
         * The identifier for this storage location.
         *
         * @return
         */
        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        /**
         * The amount of free space.
         *
         * @return
         */
        public String getFreeSpace() {
            return freeSpace;
        }

        public void setFreeSpace(String freeSpace) {
            this.freeSpace = freeSpace;
        }

        /**
         * The amount of total space.
         *
         * @param freeSpace
         */
        public String getTotalSpace() {
            return totalSpace;
        }

        public void setTotalSpace(String totalSpace) {
            this.totalSpace = totalSpace;
        }

        /**
         * The amount of used space.
         *
         * @return
         */
        public String getUsedSpace() {
            return usedSpace;
        }

        public void setUsedSpace(String usedSpace) {
            this.usedSpace = usedSpace;
        }

        /**
         * The utilization of this storage location.
         *
         * @return
         */
        public String getUtilization() {
            return utilization;
        }

        public void setUtilization(String utilization) {
            this.utilization = utilization;
        }

        /**
         * The number of bytes of free space.
         *
         * @return
         */
        public Long getFreeSpaceBytes() {
            return freeSpaceBytes;
        }

        public void setFreeSpaceBytes(Long freeSpaceBytes) {
            this.freeSpaceBytes = freeSpaceBytes;
        }

        /**
         * The number of bytes of total space.
         *
         * @return
         */
        public Long getTotalSpaceBytes() {
            return totalSpaceBytes;
        }

        public void setTotalSpaceBytes(Long totalSpaceBytes) {
            this.totalSpaceBytes = totalSpaceBytes;
        }

        /**
         * The number of bytes of used space.
         *
         * @return
         */
        public Long getUsedSpaceBytes() {
            return usedSpaceBytes;
        }

        public void setUsedSpaceBytes(Long usedSpaceBytes) {
            this.usedSpaceBytes = usedSpaceBytes;
        }
    }

    /**
     * Details for garbage collection.
     */
    @XmlType(name = "garbageCollection")
    public static class GarbageCollectionDTO {

        private String name;
        private long collectionCount;
        private String collectionTime;

        /**
         * The name of the garbage collector.
         *
         * @return
         */
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getCollectionCount() {
            return collectionCount;
        }

        /**
         * The number of times garbage collection has run.
         *
         * @param collectionCount
         */
        public void setCollectionCount(long collectionCount) {
            this.collectionCount = collectionCount;
        }

        /**
         * The total amount of time spent garbage collecting.
         *
         * @return
         */
        public String getCollectionTime() {
            return collectionTime;
        }

        public void setCollectionTime(String collectionTime) {
            this.collectionTime = collectionTime;
        }

    }
}
