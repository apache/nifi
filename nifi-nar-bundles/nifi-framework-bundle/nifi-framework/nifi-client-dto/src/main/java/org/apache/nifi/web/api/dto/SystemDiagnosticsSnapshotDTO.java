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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The diagnostics of the system this NiFi is running on.
 */
@XmlType(name = "systemDiagnosticsSnapshot")
public class SystemDiagnosticsSnapshotDTO implements Cloneable {

    private String totalNonHeap;
    private Long totalNonHeapBytes;
    private String usedNonHeap;
    private Long usedNonHeapBytes;
    private String freeNonHeap;
    private Long freeNonHeapBytes;
    private String maxNonHeap;
    private Long maxNonHeapBytes;
    private String nonHeapUtilization;

    private String totalHeap;
    private Long totalHeapBytes;
    private String usedHeap;
    private Long usedHeapBytes;
    private String freeHeap;
    private Long freeHeapBytes;
    private String maxHeap;
    private Long maxHeapBytes;
    private String heapUtilization;

    private Integer availableProcessors;
    private Double processorLoadAverage;

    private Integer totalThreads;
    private Integer daemonThreads;

    private String uptime;

    private StorageUsageDTO flowFileRepositoryStorageUsage;
    private Set<StorageUsageDTO> contentRepositoryStorageUsage;
    private Set<StorageUsageDTO> provenanceRepositoryStorageUsage;
    private Set<GarbageCollectionDTO> garbageCollection;
    private List<ResourceClaimDetailsDTO> resourceClaimDetails;

    private Date statsLastRefreshed;

    private VersionInfoDTO versionInfo;


    @Schema(description = "Number of available processors if supported by the underlying system.")
    public Integer getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(Integer availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    @Schema(description = "Number of daemon threads.")
    public Integer getDaemonThreads() {
        return daemonThreads;
    }

    public void setDaemonThreads(Integer daemonThreads) {
        this.daemonThreads = daemonThreads;
    }

    @Schema(description = "Amount of free heap.")
    public String getFreeHeap() {
        return freeHeap;
    }

    public void setFreeHeap(String freeHeap) {
        this.freeHeap = freeHeap;
    }

    @Schema(description = "Amount of free non heap.")
    public String getFreeNonHeap() {
        return freeNonHeap;
    }

    public void setFreeNonHeap(String freeNonHeap) {
        this.freeNonHeap = freeNonHeap;
    }

    @Schema(description = "Maximum size of heap.")
    public String getMaxHeap() {
        return maxHeap;
    }

    public void setMaxHeap(String maxHeap) {
        this.maxHeap = maxHeap;
    }

    @Schema(description = "Maximum size of non heap.")
    public String getMaxNonHeap() {
        return maxNonHeap;
    }

    public void setMaxNonHeap(String maxNonHeap) {
        this.maxNonHeap = maxNonHeap;
    }

    @Schema(description = "The processor load average if supported by the underlying system.")
    public Double getProcessorLoadAverage() {
        return processorLoadAverage;
    }

    public void setProcessorLoadAverage(Double processorLoadAverage) {
        this.processorLoadAverage = processorLoadAverage;
    }

    @Schema(description = "Total size of heap.")
    public String getTotalHeap() {
        return totalHeap;
    }

    public void setTotalHeap(String totalHeap) {
        this.totalHeap = totalHeap;
    }

    @Schema(description = "Total size of non heap.")
    public String getTotalNonHeap() {
        return totalNonHeap;
    }

    public void setTotalNonHeap(String totalNonHeap) {
        this.totalNonHeap = totalNonHeap;
    }

    @Schema(description = "Total number of threads.")
    public Integer getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(Integer totalThreads) {
        this.totalThreads = totalThreads;
    }

    @Schema(description = "Amount of used heap.")
    public String getUsedHeap() {
        return usedHeap;
    }

    public void setUsedHeap(String usedHeap) {
        this.usedHeap = usedHeap;
    }

    @Schema(description = "Amount of use non heap.")
    public String getUsedNonHeap() {
        return usedNonHeap;
    }

    public void setUsedNonHeap(String usedNonHeap) {
        this.usedNonHeap = usedNonHeap;
    }

    @Schema(description = "Utilization of heap.")
    public String getHeapUtilization() {
        return heapUtilization;
    }

    public void setHeapUtilization(String heapUtilization) {
        this.heapUtilization = heapUtilization;
    }

    @Schema(description = "Utilization of non heap.")
    public String getNonHeapUtilization() {
        return nonHeapUtilization;
    }

    public void setNonHeapUtilization(String nonHeapUsage) {
        this.nonHeapUtilization = nonHeapUsage;
    }

    @Schema(description = "The content repository storage usage.")
    public Set<StorageUsageDTO> getContentRepositoryStorageUsage() {
        return contentRepositoryStorageUsage;
    }

    public void setContentRepositoryStorageUsage(Set<StorageUsageDTO> contentRepositoryStorageUsage) {
        this.contentRepositoryStorageUsage = contentRepositoryStorageUsage;
    }

    @Schema(description = "The provenance repository storage usage.")
    public Set<StorageUsageDTO> getProvenanceRepositoryStorageUsage() {
        return provenanceRepositoryStorageUsage;
    }

    public void setProvenanceRepositoryStorageUsage(Set<StorageUsageDTO> provenanceRepositoryStorageUsage) {
        this.provenanceRepositoryStorageUsage = provenanceRepositoryStorageUsage;
    }

    @Schema(description = "The flowfile repository storage usage.")
    public StorageUsageDTO getFlowFileRepositoryStorageUsage() {
        return flowFileRepositoryStorageUsage;
    }

    public void setFlowFileRepositoryStorageUsage(StorageUsageDTO flowFileRepositoryStorageUsage) {
        this.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage;
    }

    @Schema(description = "The garbage collection details.")
    public Set<GarbageCollectionDTO> getGarbageCollection() {
        return garbageCollection;
    }

    public void setGarbageCollection(Set<GarbageCollectionDTO> garbageCollection) {
        this.garbageCollection = garbageCollection;
    }

    public List<ResourceClaimDetailsDTO> getResourceClaimDetails() {
        return resourceClaimDetails;
    }

    public void setResourceClaimDetails(final List<ResourceClaimDetailsDTO> resourceClaimDetails) {
        this.resourceClaimDetails = resourceClaimDetails;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @Schema(description = "When the diagnostics were generated.",
            type = "string"
    )
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }


    @Schema(description = "Total number of bytes allocated to the JVM not used for heap")
    public Long getTotalNonHeapBytes() {
        return totalNonHeapBytes;
    }

    public void setTotalNonHeapBytes(Long totalNonHeapBytes) {
        this.totalNonHeapBytes = totalNonHeapBytes;
    }

    @Schema(description = "Total number of bytes used by the JVM not in the heap space")
    public Long getUsedNonHeapBytes() {
        return usedNonHeapBytes;
    }

    public void setUsedNonHeapBytes(Long usedNonHeapBytes) {
        this.usedNonHeapBytes = usedNonHeapBytes;
    }

    @Schema(description = "Total number of free non-heap bytes available to the JVM")
    public Long getFreeNonHeapBytes() {
        return freeNonHeapBytes;
    }

    public void setFreeNonHeapBytes(Long freeNonHeapBytes) {
        this.freeNonHeapBytes = freeNonHeapBytes;
    }

    @Schema(description = "The maximum number of bytes that the JVM can use for non-heap purposes")
    public Long getMaxNonHeapBytes() {
        return maxNonHeapBytes;
    }

    public void setMaxNonHeapBytes(Long maxNonHeapBytes) {
        this.maxNonHeapBytes = maxNonHeapBytes;
    }

    @Schema(description = "The total number of bytes that are available for the JVM heap to use")
    public Long getTotalHeapBytes() {
        return totalHeapBytes;
    }

    public void setTotalHeapBytes(Long totalHeapBytes) {
        this.totalHeapBytes = totalHeapBytes;
    }

    @Schema(description = "The number of bytes of JVM heap that are currently being used")
    public Long getUsedHeapBytes() {
        return usedHeapBytes;
    }

    public void setUsedHeapBytes(Long usedHeapBytes) {
        this.usedHeapBytes = usedHeapBytes;
    }

    @Schema(description = "The number of bytes that are allocated to the JVM heap but not currently being used")
    public Long getFreeHeapBytes() {
        return freeHeapBytes;
    }

    public void setFreeHeapBytes(Long freeHeapBytes) {
        this.freeHeapBytes = freeHeapBytes;
    }

    @Schema(description = "The maximum number of bytes that can be used by the JVM")
    public Long getMaxHeapBytes() {
        return maxHeapBytes;
    }

    public void setMaxHeapBytes(Long maxHeapBytes) {
        this.maxHeapBytes = maxHeapBytes;
    }

    @Schema(description = "The nifi, os, java, and build version information")
    public VersionInfoDTO getVersionInfo() {
        return versionInfo;
    }

    public void setVersionInfo(VersionInfoDTO versionInfo) {
        this.versionInfo = versionInfo;
    }

    @Schema(description = "The uptime of the Java virtual machine")
    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    @Override
    public SystemDiagnosticsSnapshotDTO clone() {
        final SystemDiagnosticsSnapshotDTO other = new SystemDiagnosticsSnapshotDTO();
        other.setAvailableProcessors(getAvailableProcessors());
        other.setDaemonThreads(getDaemonThreads());
        other.setFreeHeap(getFreeHeap());
        other.setFreeHeapBytes(getFreeHeapBytes());
        other.setFreeNonHeap(getFreeNonHeap());
        other.setFreeNonHeapBytes(getFreeNonHeapBytes());
        other.setHeapUtilization(getHeapUtilization());
        other.setMaxHeap(getMaxHeap());
        other.setMaxHeapBytes(getMaxHeapBytes());
        other.setMaxNonHeap(getMaxNonHeap());
        other.setMaxNonHeapBytes(getMaxNonHeapBytes());
        other.setNonHeapUtilization(getNonHeapUtilization());
        other.setProcessorLoadAverage(getProcessorLoadAverage());
        other.setStatsLastRefreshed(getStatsLastRefreshed());
        other.setTotalHeap(getTotalHeap());
        other.setTotalHeapBytes(getTotalHeapBytes());
        other.setTotalNonHeap(getTotalNonHeap());
        other.setTotalNonHeapBytes(getTotalNonHeapBytes());
        other.setTotalThreads(getTotalThreads());
        other.setUsedHeap(getUsedHeap());
        other.setUsedHeapBytes(getUsedHeapBytes());
        other.setUsedNonHeap(getUsedNonHeap());
        other.setUsedNonHeapBytes(getUsedNonHeapBytes());
        other.setResourceClaimDetails(getResourceClaimDetails());

        other.setFlowFileRepositoryStorageUsage(getFlowFileRepositoryStorageUsage().clone());

        final Set<StorageUsageDTO> contentRepoStorageUsage = new LinkedHashSet<>();
        other.setContentRepositoryStorageUsage(contentRepoStorageUsage);
        if (getContentRepositoryStorageUsage() != null) {
            for (final StorageUsageDTO usage : getContentRepositoryStorageUsage()) {
                contentRepoStorageUsage.add(usage.clone());
            }
        }

        final Set<StorageUsageDTO> provenanceRepoStorageUsage = new LinkedHashSet<>();
        other.setProvenanceRepositoryStorageUsage(provenanceRepoStorageUsage);
        if (getProvenanceRepositoryStorageUsage() != null) {
            for (final StorageUsageDTO usage : getProvenanceRepositoryStorageUsage()) {
                provenanceRepoStorageUsage.add(usage.clone());
            }
        }

        final Set<GarbageCollectionDTO> gcUsage = new LinkedHashSet<>();
        other.setGarbageCollection(gcUsage);
        if (getGarbageCollection() != null) {
            for (final GarbageCollectionDTO gcDto : getGarbageCollection()) {
                gcUsage.add(gcDto.clone());
            }
        }

        other.setVersionInfo(getVersionInfo().clone());

        other.setUptime(getUptime());

        return other;
    }

    /**
     * Details of storage usage.
     */
    @XmlType(name = "storageUsage")
    public static class StorageUsageDTO implements Cloneable {

        private String identifier;
        private String freeSpace;
        private String totalSpace;
        private String usedSpace;
        private Long freeSpaceBytes;
        private Long totalSpaceBytes;
        private Long usedSpaceBytes;
        /**
         * @return identifier for this storage location
         */
        @Schema(description = "The identifier of this storage location. The identifier will correspond to the identifier keyed in the storage configuration.")
        public String getIdentifier() {
            return identifier;
        }

        private String utilization;

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        /**
         * @return amount of free space
         */
        @Schema(description = "Amount of free space.")
        public String getFreeSpace() {
            return freeSpace;
        }

        public void setFreeSpace(String freeSpace) {
            this.freeSpace = freeSpace;
        }

        /**
         * @return freeSpace amount of total space
         */
        @Schema(description = "Amount of total space.")
        public String getTotalSpace() {
            return totalSpace;
        }

        public void setTotalSpace(String totalSpace) {
            this.totalSpace = totalSpace;
        }

        /**
         * @return amount of used space
         */
        @Schema(description = "Amount of used space.")
        public String getUsedSpace() {
            return usedSpace;
        }

        public void setUsedSpace(String usedSpace) {
            this.usedSpace = usedSpace;
        }

        /**
         * @return utilization of this storage location
         */
        @Schema(description = "Utilization of this storage location.")
        public String getUtilization() {
            return utilization;
        }

        public void setUtilization(String utilization) {
            this.utilization = utilization;
        }

        /**
         * @return number of bytes of free space
         */
        @Schema(description = "The number of bytes of free space.")
        public Long getFreeSpaceBytes() {
            return freeSpaceBytes;
        }

        public void setFreeSpaceBytes(Long freeSpaceBytes) {
            this.freeSpaceBytes = freeSpaceBytes;
        }

        /**
         * @return number of bytes of total space
         */
        @Schema(description = "The number of bytes of total space.")
        public Long getTotalSpaceBytes() {
            return totalSpaceBytes;
        }

        public void setTotalSpaceBytes(Long totalSpaceBytes) {
            this.totalSpaceBytes = totalSpaceBytes;
        }

        /**
         * @return number of bytes of used space
         */
        @Schema(description = "The number of bytes of used space.")
        public Long getUsedSpaceBytes() {
            return usedSpaceBytes;
        }

        public void setUsedSpaceBytes(Long usedSpaceBytes) {
            this.usedSpaceBytes = usedSpaceBytes;
        }

        @Override
        public StorageUsageDTO clone() {
            final StorageUsageDTO other = new StorageUsageDTO();
            other.setIdentifier(getIdentifier());
            other.setFreeSpace(getFreeSpace());
            other.setTotalSpace(getTotalSpace());
            other.setUsedSpace(getUsedSpace());
            other.setFreeSpaceBytes(getFreeSpaceBytes());
            other.setTotalSpaceBytes(getTotalSpaceBytes());
            other.setUsedSpaceBytes(getUsedSpaceBytes());
            other.setUtilization(getUtilization());
            return other;
        }
    }

    @XmlType(name = "resourceClaimDetails")
    public static class ResourceClaimDetailsDTO implements Cloneable {
        private String container;
        private String section;
        private String identifier;
        private Boolean inUse;
        private Boolean awaitingDestruction;
        private Boolean writable;
        private Integer claimantCount;


        @Schema(description = "The container of the Content Repository in which the Resource Claim exists")
        public String getContainer() {
            return container;
        }

        public void setContainer(final String container) {
            this.container = container;
        }

        @Schema(description = "The section of the Content Repository in which the Resource Claim exists")
        public String getSection() {
            return section;
        }

        public void setSection(final String section) {
            this.section = section;
        }

        @Schema(description = "The identifier of the Resource Claim")
        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(final String identifier) {
            this.identifier = identifier;
        }

        @Schema(description = "Whether or not the Resource Claim is in use")
        public Boolean getInUse() {
            return inUse;
        }

        public void setInUse(final Boolean inUse) {
            this.inUse = inUse;
        }

        @Schema(description = "Whether or not the Resource Claim is awaiting destruction")
        public Boolean getAwaitingDestruction() {
            return awaitingDestruction;
        }

        public void setAwaitingDestruction(final Boolean awaitingDestruction) {
            this.awaitingDestruction = awaitingDestruction;
        }

        @Schema(description = "The number of FlowFiles that have a claim to the Resource")
        public Integer getClaimantCount() {
            return claimantCount;
        }

        public void setClaimantCount(final Integer claimantCount) {
            this.claimantCount = claimantCount;
        }

        @Schema(description = "Whether or not the Resource Claim can still have more data written to it")
        public Boolean getWritable() {
            return writable;
        }

        public void setWritable(final Boolean writable) {
            this.writable = writable;
        }

        public ResourceClaimDetailsDTO clone() {
            final ResourceClaimDetailsDTO other = new ResourceClaimDetailsDTO();
            other.setContainer(getContainer());
            other.setSection(getSection());
            other.setIdentifier(getIdentifier());
            other.setInUse(getInUse());
            other.setAwaitingDestruction(getAwaitingDestruction());
            other.setClaimantCount(getClaimantCount());
            other.setWritable(getWritable());
            return other;
        }
    }

    /**
     * Details for garbage collection.
     */
    @XmlType(name = "garbageCollection")
    public static class GarbageCollectionDTO implements Cloneable {

        private String name;
        private long collectionCount;
        private String collectionTime;
        private Long collectionMillis;

        @Schema(description = "The name of the garbage collector.")
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Schema(description = "The number of times garbage collection has run.")
        public long getCollectionCount() {
            return collectionCount;
        }

        public void setCollectionCount(long collectionCount) {
            this.collectionCount = collectionCount;
        }

        @Schema(description = "The total amount of time spent garbage collecting.")
        public String getCollectionTime() {
            return collectionTime;
        }

        public void setCollectionTime(String collectionTime) {
            this.collectionTime = collectionTime;
        }

        @Schema(description = "The total number of milliseconds spent garbage collecting.")
        public Long getCollectionMillis() {
            return collectionMillis;
        }

        public void setCollectionMillis(Long collectionMillis) {
            this.collectionMillis = collectionMillis;
        }

        @Override
        public GarbageCollectionDTO clone() {
            final GarbageCollectionDTO other = new GarbageCollectionDTO();
            other.setName(getName());
            other.setCollectionCount(getCollectionCount());
            other.setCollectionTime(getCollectionTime());
            other.setCollectionMillis(getCollectionMillis());
            return other;
        }
    }

    /**
     * Details for version information.
     */
    @XmlType(name = "versionInfo")
    public static class VersionInfoDTO implements Cloneable {

        private String niFiVersion;
        private String javaVendor;
        private String javaVersion;
        private String osName;
        private String osVersion;
        private String osArchitecture;
        private String buildTag;
        private String buildRevision;
        private String buildBranch;
        private Date buildTimestamp;

        @Schema(description = "The version of this NiFi.")
        public String getNiFiVersion() {
            return niFiVersion;
        }

        public void setNiFiVersion(String niFiVersion) {
            this.niFiVersion = niFiVersion;
        }

        @Schema(description = "Java JVM vendor")
        public String getJavaVendor() {
            return javaVendor;
        }

        public void setJavaVendor(String javaVendor) {
            this.javaVendor = javaVendor;
        }

        @Schema(description = "Java version")
        public String getJavaVersion() {
            return javaVersion;
        }

        public void setJavaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
        }

        @Schema(description = "Host operating system name")
        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        @Schema(description = "Host operating system version")
        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Schema(description = "Host operating system architecture")
        public String getOsArchitecture() {
            return osArchitecture;
        }

        public void setOsArchitecture(String osArchitecture) {
            this.osArchitecture = osArchitecture;
        }

        @Schema(description = "Build tag")
        public String getBuildTag() {
            return buildTag;
        }

        public void setBuildTag(String buildTag) {
            this.buildTag = buildTag;
        }

        @Schema(description = "Build revision or commit hash")
        public String getBuildRevision() {
            return buildRevision;
        }

        public void setBuildRevision(String buildRevision) {
            this.buildRevision = buildRevision;
        }

        @Schema(description = "Build branch")
        public String getBuildBranch() {
            return buildBranch;
        }

        public void setBuildBranch(String buildBranch) {
            this.buildBranch = buildBranch;
        }

        @XmlJavaTypeAdapter(DateTimeAdapter.class)
        @Schema(description = "Build timestamp")
        public Date getBuildTimestamp() {
            return buildTimestamp;
        }

        public void setBuildTimestamp(Date buildTimestamp) {
            this.buildTimestamp = buildTimestamp;
        }

        @Override
        public VersionInfoDTO clone() {
            final VersionInfoDTO other = new VersionInfoDTO();
            other.setNiFiVersion(getNiFiVersion());
            other.setJavaVendor(getJavaVendor());
            other.setJavaVersion(getJavaVersion());
            other.setOsName(getOsName());
            other.setOsVersion(getOsVersion());
            other.setOsArchitecture(getOsArchitecture());
            other.setBuildTag(getBuildTag());
            other.setBuildTimestamp(getBuildTimestamp());
            other.setBuildBranch(getBuildBranch());
            other.setBuildRevision(getBuildRevision());
            return other;
        }
    }
}
