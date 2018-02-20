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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlType;

import org.apache.nifi.web.api.dto.BundleDTO;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "jvmDiagnosticsSnapshot")
public class JVMDiagnosticsSnapshotDTO implements Cloneable {
    // JVM/NiFi instance specific
    private Boolean primaryNode;
    private Boolean clusterCoordinator;
    private String uptime;
    private String timeZone;
    private Integer maxTimerDrivenThreads;
    private Integer maxEventDrivenThreads;
    private Integer activeTimerDrivenThreads;
    private Integer activeEventDrivenThreads;
    private Set<BundleDTO> bundlesLoaded;
    private RepositoryUsageDTO flowFileRepositoryStorageUsage;
    private Set<RepositoryUsageDTO> contentRepositoryStorageUsage;
    private Set<RepositoryUsageDTO> provenanceRepositoryStorageUsage;
    private Long maxHeapBytes;
    private String maxHeap;
    private List<GarbageCollectionDiagnosticsDTO> garbageCollectionDiagnostics;

    // System Specific
    private Integer cpuCores;
    private Double cpuLoadAverage;
    private Long physicalMemoryBytes;
    private String physicalMemory;

    // Only available if we get OS MXBean and can create class com.sun.management.UnixOperatingSystemMXBean and the OS MXBean
    // is of this type.
    private Long openFileDescriptors;
    private Long maxOpenFileDescriptors;

    @ApiModelProperty("Whether or not this node is primary node")
    public Boolean getPrimaryNode() {
        return primaryNode;
    }

    public void setPrimaryNode(Boolean primaryNode) {
        this.primaryNode = primaryNode;
    }

    @ApiModelProperty("Whether or not this node is cluster coordinator")
    public Boolean getClusterCoordinator() {
        return clusterCoordinator;
    }

    public void setClusterCoordinator(Boolean clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @ApiModelProperty("How long this node has been running, formatted as hours:minutes:seconds.milliseconds")
    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    @ApiModelProperty("The name of the Time Zone that is configured, if available")
    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    @ApiModelProperty("The maximum number of timer-driven threads")
    public Integer getMaxTimerDrivenThreads() {
        return maxTimerDrivenThreads;
    }

    public void setMaxTimerDrivenThreads(Integer maxTimerDrivenThreads) {
        this.maxTimerDrivenThreads = maxTimerDrivenThreads;
    }

    @ApiModelProperty("The maximum number of event-driven threads")
    public Integer getMaxEventDrivenThreads() {
        return maxEventDrivenThreads;
    }

    public void setMaxEventDrivenThreads(Integer maxEventDrivenThreads) {
        this.maxEventDrivenThreads = maxEventDrivenThreads;
    }

    @ApiModelProperty("The number of timer-driven threads that are active")
    public Integer getActiveTimerDrivenThreads() {
        return activeTimerDrivenThreads;
    }

    public void setActiveTimerDrivenThreads(Integer activeTimerDrivenThreads) {
        this.activeTimerDrivenThreads = activeTimerDrivenThreads;
    }

    @ApiModelProperty("The number of event-driven threads that are active")
    public Integer getActiveEventDrivenThreads() {
        return activeEventDrivenThreads;
    }

    public void setActiveEventDrivenThreads(Integer activeEventDrivenThreads) {
        this.activeEventDrivenThreads = activeEventDrivenThreads;
    }

    @ApiModelProperty("The NiFi Bundles (NARs) that are loaded by NiFi")
    public Set<BundleDTO> getBundlesLoaded() {
        return bundlesLoaded;
    }

    public void setBundlesLoaded(Set<BundleDTO> bundlesLoaded) {
        this.bundlesLoaded = bundlesLoaded;
    }

    @ApiModelProperty("Information about the FlowFile Repository's usage")
    public RepositoryUsageDTO getFlowFileRepositoryStorageUsage() {
        return flowFileRepositoryStorageUsage;
    }

    public void setFlowFileRepositoryStorageUsage(RepositoryUsageDTO flowFileRepositoryStorageUsage) {
        this.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage;
    }

    @ApiModelProperty("Information about the Content Repository's usage")
    public Set<RepositoryUsageDTO> getContentRepositoryStorageUsage() {
        return contentRepositoryStorageUsage;
    }

    public void setContentRepositoryStorageUsage(Set<RepositoryUsageDTO> contentRepositoryStorageUsage) {
        this.contentRepositoryStorageUsage = contentRepositoryStorageUsage;
    }

    @ApiModelProperty("Information about the Provenance Repository's usage")
    public Set<RepositoryUsageDTO> getProvenanceRepositoryStorageUsage() {
        return provenanceRepositoryStorageUsage;
    }

    public void setProvenanceRepositoryStorageUsage(Set<RepositoryUsageDTO> provenanceRepositoryStorageUsage) {
        this.provenanceRepositoryStorageUsage = provenanceRepositoryStorageUsage;
    }

    @ApiModelProperty("The maximum number of bytes that the JVM heap is configured to use for heap")
    public Long getMaxHeapBytes() {
        return maxHeapBytes;
    }

    public void setMaxHeapBytes(Long heapBytes) {
        this.maxHeapBytes = heapBytes;
    }

    @ApiModelProperty("The maximum number of bytes that the JVM heap is configured to use, as a human-readable value")
    public String getMaxHeap() {
        return maxHeap;
    }

    public void setMaxHeap(String maxHeap) {
        this.maxHeap = maxHeap;
    }

    @ApiModelProperty("The number of CPU Cores available on the system")
    public Integer getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(Integer cpuCores) {
        this.cpuCores = cpuCores;
    }

    @ApiModelProperty("The 1-minute CPU Load Average")
    public Double getCpuLoadAverage() {
        return cpuLoadAverage;
    }

    public void setCpuLoadAverage(Double cpuLoadAverage) {
        this.cpuLoadAverage = cpuLoadAverage;
    }

    @ApiModelProperty("The number of bytes of RAM available on the system")
    public Long getPhysicalMemoryBytes() {
        return physicalMemoryBytes;
    }

    public void setPhysicalMemoryBytes(Long memoryBytes) {
        this.physicalMemoryBytes = memoryBytes;
    }

    @ApiModelProperty("The number of bytes of RAM available on the system as a human-readable value")
    public String getPhysicalMemory() {
        return physicalMemory;
    }

    public void setPhysicalMemory(String memory) {
        this.physicalMemory = memory;
    }

    @ApiModelProperty("The number of files that are open by the NiFi process")
    public Long getOpenFileDescriptors() {
        return openFileDescriptors;
    }

    public void setOpenFileDescriptors(Long openFileDescriptors) {
        this.openFileDescriptors = openFileDescriptors;
    }

    @ApiModelProperty("The maximum number of open file descriptors that are available to each process")
    public Long getMaxOpenFileDescriptors() {
        return maxOpenFileDescriptors;
    }

    public void setMaxOpenFileDescriptors(Long maxOpenFileDescriptors) {
        this.maxOpenFileDescriptors = maxOpenFileDescriptors;
    }

    @ApiModelProperty("Diagnostic information about the JVM's garbage collections")
    public List<GarbageCollectionDiagnosticsDTO> getGarbageCollectionDiagnostics() {
        return garbageCollectionDiagnostics;
    }

    public void setGarbageCollectionDiagnostics(List<GarbageCollectionDiagnosticsDTO> garbageCollectionDiagnostics) {
        this.garbageCollectionDiagnostics = garbageCollectionDiagnostics;
    }


    @Override
    public JVMDiagnosticsSnapshotDTO clone() {
        final JVMDiagnosticsSnapshotDTO clone = new JVMDiagnosticsSnapshotDTO();
        clone.activeEventDrivenThreads = activeEventDrivenThreads;
        clone.activeTimerDrivenThreads = activeTimerDrivenThreads;
        clone.bundlesLoaded = bundlesLoaded == null ? null : new HashSet<>(bundlesLoaded);
        clone.clusterCoordinator = clusterCoordinator;
        clone.contentRepositoryStorageUsage = cloneRepoUsage(contentRepositoryStorageUsage);
        clone.cpuCores = cpuCores;
        clone.cpuLoadAverage = cpuLoadAverage;
        clone.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage == null ? null : flowFileRepositoryStorageUsage.clone();
        clone.maxEventDrivenThreads = maxEventDrivenThreads;
        clone.maxHeap = maxHeap;
        clone.maxHeapBytes = maxHeapBytes;
        clone.maxOpenFileDescriptors = maxOpenFileDescriptors;
        clone.maxTimerDrivenThreads = maxTimerDrivenThreads;
        clone.openFileDescriptors = openFileDescriptors;
        clone.physicalMemory = physicalMemory;
        clone.physicalMemoryBytes = physicalMemoryBytes;
        clone.primaryNode = primaryNode;
        clone.provenanceRepositoryStorageUsage = cloneRepoUsage(provenanceRepositoryStorageUsage);
        clone.timeZone = timeZone;
        clone.uptime = uptime;

        if (garbageCollectionDiagnostics != null) {
            clone.garbageCollectionDiagnostics = garbageCollectionDiagnostics.stream()
                .map(gcDiag -> gcDiag.clone())
                .collect(Collectors.toList());
        }

        return clone;
    }

    private static Set<RepositoryUsageDTO> cloneRepoUsage(final Set<RepositoryUsageDTO> repoUsage) {
        if (repoUsage == null) {
            return null;
        }

        return repoUsage.stream()
            .map(usage -> usage.clone())
            .collect(Collectors.toSet());
    }


    @XmlType(name = "versionInfo")
    public static class VersionInfoDTO implements Cloneable {

        private String niFiVersion;
        private String javaVendor;
        private String javaVersion;
        private String javaVmVendor;
        private String osName;
        private String osVersion;
        private String osArchitecture;

        @ApiModelProperty("The version of this NiFi.")
        public String getNiFiVersion() {
            return niFiVersion;
        }

        public void setNiFiVersion(String niFiVersion) {
            this.niFiVersion = niFiVersion;
        }

        @ApiModelProperty("Java vendor")
        public String getJavaVendor() {
            return javaVendor;
        }

        public void setJavaVendor(String javaVendor) {
            this.javaVendor = javaVendor;
        }

        @ApiModelProperty("Java VM Vendor")
        public String getJavaVmVendor() {
            return javaVmVendor;
        }

        public void setJavaVmVendor(String javaVmVendor) {
            this.javaVmVendor = javaVmVendor;
        }

        @ApiModelProperty("Java version")
        public String getJavaVersion() {
            return javaVersion;
        }

        public void setJavaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
        }

        @ApiModelProperty("Host operating system name")
        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        @ApiModelProperty("Host operating system version")
        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @ApiModelProperty("Host operating system architecture")
        public String getOsArchitecture() {
            return osArchitecture;
        }

        public void setOsArchitecture(String osArchitecture) {
            this.osArchitecture = osArchitecture;
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
            return other;
        }
    }
}
