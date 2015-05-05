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

import com.wordnik.swagger.annotations.ApiModelProperty;
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
     * @return number of available processors, if supported
     */
    @ApiModelProperty(
            value = "Number of available processors if supported by the underlying system."
    )
    public Integer getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(Integer availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    /**
     * @return number of daemon threads
     */
    @ApiModelProperty(
            value = "Number of daemon threads."
    )
    public Integer getDaemonThreads() {
        return daemonThreads;
    }

    public void setDaemonThreads(Integer daemonThreads) {
        this.daemonThreads = daemonThreads;
    }

    /**
     * @return amount of free heap
     */
    @ApiModelProperty(
            value = "Amount of free heap."
    )
    public String getFreeHeap() {
        return freeHeap;
    }

    public void setFreeHeap(String freeHeap) {
        this.freeHeap = freeHeap;
    }

    /**
     * @return amount of free non-heap
     */
    @ApiModelProperty(
            value = "Amount of free non heap."
    )
    public String getFreeNonHeap() {
        return freeNonHeap;
    }

    public void setFreeNonHeap(String freeNonHeap) {
        this.freeNonHeap = freeNonHeap;
    }

    /**
     * @return max size of the heap
     */
    @ApiModelProperty(
            value = "Maximum size of heap."
    )
    public String getMaxHeap() {
        return maxHeap;
    }

    public void setMaxHeap(String maxHeap) {
        this.maxHeap = maxHeap;
    }

    /**
     * @return max size of the non-heap
     */
    @ApiModelProperty(
            value = "Maximum size of non heap."
    )
    public String getMaxNonHeap() {
        return maxNonHeap;
    }

    public void setMaxNonHeap(String maxNonHeap) {
        this.maxNonHeap = maxNonHeap;
    }

    /**
     * @return processor load average, if supported
     */
    @ApiModelProperty(
            value = "The processor load average if supported by the underlying system."
    )
    public Double getProcessorLoadAverage() {
        return processorLoadAverage;
    }

    public void setProcessorLoadAverage(Double processorLoadAverage) {
        this.processorLoadAverage = processorLoadAverage;
    }

    /**
     * @return total size of the heap
     */
    @ApiModelProperty(
            value = "Total size of heap."
    )
    public String getTotalHeap() {
        return totalHeap;
    }

    public void setTotalHeap(String totalHeap) {
        this.totalHeap = totalHeap;
    }

    /**
     * @return total size of non-heap
     */
    @ApiModelProperty(
            value = "Total size of non heap."
    )
    public String getTotalNonHeap() {
        return totalNonHeap;
    }

    public void setTotalNonHeap(String totalNonHeap) {
        this.totalNonHeap = totalNonHeap;
    }

    /**
     * @return total number of threads
     */
    @ApiModelProperty(
            value = "Total number of threads."
    )
    public Integer getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(Integer totalThreads) {
        this.totalThreads = totalThreads;
    }

    /**
     * @return amount of used heap
     */
    @ApiModelProperty(
            value = "Amount of used heap."
    )
    public String getUsedHeap() {
        return usedHeap;
    }

    public void setUsedHeap(String usedHeap) {
        this.usedHeap = usedHeap;
    }

    /**
     * @return amount of used non-heap
     */
    @ApiModelProperty(
            value = "Amount of use non heap."
    )
    public String getUsedNonHeap() {
        return usedNonHeap;
    }

    public void setUsedNonHeap(String usedNonHeap) {
        this.usedNonHeap = usedNonHeap;
    }

    /**
     * @return heap utilization
     */
    @ApiModelProperty(
            value = "Utilization of heap."
    )
    public String getHeapUtilization() {
        return heapUtilization;
    }

    public void setHeapUtilization(String heapUtilization) {
        this.heapUtilization = heapUtilization;
    }

    /**
     * @return non-heap utilization
     */
    @ApiModelProperty(
            value = "Utilization of non heap."
    )
    public String getNonHeapUtilization() {
        return nonHeapUtilization;
    }

    public void setNonHeapUtilization(String nonHeapUsage) {
        this.nonHeapUtilization = nonHeapUsage;
    }

    /**
     * @return content repository storage usage
     */
    @ApiModelProperty(
            value = "The content repository storage usage."
    )
    public Set<StorageUsageDTO> getContentRepositoryStorageUsage() {
        return contentRepositoryStorageUsage;
    }

    public void setContentRepositoryStorageUsage(Set<StorageUsageDTO> contentRepositoryStorageUsage) {
        this.contentRepositoryStorageUsage = contentRepositoryStorageUsage;
    }

    /**
     * @return flowfile repository storage usage
     */
    @ApiModelProperty(
            value = "The flowfile repository storage usage."
    )
    public StorageUsageDTO getFlowFileRepositoryStorageUsage() {
        return flowFileRepositoryStorageUsage;
    }

    public void setFlowFileRepositoryStorageUsage(StorageUsageDTO flowFileRepositoryStorageUsage) {
        this.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage;
    }

    /**
     * @return Garbage collection details
     */
    @ApiModelProperty(
            value = "The garbage collection details."
    )
    public Set<GarbageCollectionDTO> getGarbageCollection() {
        return garbageCollection;
    }

    public void setGarbageCollection(Set<GarbageCollectionDTO> garbageCollection) {
        this.garbageCollection = garbageCollection;
    }

    /**
     * @return When these diagnostics were generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "When the diagnostics were generated."
    )
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
         * @return identifier for this storage location
         */
        @ApiModelProperty(
                value = "The identifier of this storage location. The identifier will correspond to the identifier keyed in the storage configuration."
        )
        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        /**
         * @return amount of free space
         */
        @ApiModelProperty(
                value = "Amount of free space."
        )
        public String getFreeSpace() {
            return freeSpace;
        }

        public void setFreeSpace(String freeSpace) {
            this.freeSpace = freeSpace;
        }

        /**
         * @return freeSpace amount of total space
         */
        @ApiModelProperty(
                value = "Amount of total space."
        )
        public String getTotalSpace() {
            return totalSpace;
        }

        public void setTotalSpace(String totalSpace) {
            this.totalSpace = totalSpace;
        }

        /**
         * @return amount of used space
         */
        @ApiModelProperty(
                value = "Amount of used space."
        )
        public String getUsedSpace() {
            return usedSpace;
        }

        public void setUsedSpace(String usedSpace) {
            this.usedSpace = usedSpace;
        }

        /**
         * @return utilization of this storage location
         */
        @ApiModelProperty(
                value = "Utilization of this storage location."
        )
        public String getUtilization() {
            return utilization;
        }

        public void setUtilization(String utilization) {
            this.utilization = utilization;
        }

        /**
         * @return number of bytes of free space
         */
        @ApiModelProperty(
                value = "The number of bytes of free space."
        )
        public Long getFreeSpaceBytes() {
            return freeSpaceBytes;
        }

        public void setFreeSpaceBytes(Long freeSpaceBytes) {
            this.freeSpaceBytes = freeSpaceBytes;
        }

        /**
         * @return number of bytes of total space
         */
        @ApiModelProperty(
                value = "The number of bytes of total space."
        )
        public Long getTotalSpaceBytes() {
            return totalSpaceBytes;
        }

        public void setTotalSpaceBytes(Long totalSpaceBytes) {
            this.totalSpaceBytes = totalSpaceBytes;
        }

        /**
         * @return number of bytes of used space
         */
        @ApiModelProperty(
                value = "The number of bytes of used space."
        )
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
         * @return name of the garbage collector
         */
        @ApiModelProperty(
                value = "The name of the garbage collector."
        )
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @ApiModelProperty(
                value = "The number of times garbage collection has run."
        )
        public long getCollectionCount() {
            return collectionCount;
        }

        /**
         * @param collectionCount number of times garbage collection has run
         */
        public void setCollectionCount(long collectionCount) {
            this.collectionCount = collectionCount;
        }

        /**
         * @return total amount of time spent garbage collecting
         */
        @ApiModelProperty(
                value = "The total amount of time spent garbage collecting."
        )
        public String getCollectionTime() {
            return collectionTime;
        }

        public void setCollectionTime(String collectionTime) {
            this.collectionTime = collectionTime;
        }

    }
}
