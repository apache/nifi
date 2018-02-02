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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "jvmSystemDiagnosticsSnapshot")
public class JVMSystemDiagnosticsSnapshotDTO implements Cloneable {
    private RepositoryUsageDTO flowFileRepositoryStorageUsage;
    private Set<RepositoryUsageDTO> contentRepositoryStorageUsage;
    private Set<RepositoryUsageDTO> provenanceRepositoryStorageUsage;
    private Long maxHeapBytes;
    private String maxHeap;
    private List<GarbageCollectionDiagnosticsDTO> garbageCollectionDiagnostics;

    private Integer cpuCores;
    private Double cpuLoadAverage;
    private Long physicalMemoryBytes;
    private String physicalMemory;

    // Only available if we get OS MXBean and can create class com.sun.management.UnixOperatingSystemMXBean and the OS MXBean
    // is of this type.
    private Long openFileDescriptors;
    private Long maxOpenFileDescriptors;


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
    public JVMSystemDiagnosticsSnapshotDTO clone() {
        final JVMSystemDiagnosticsSnapshotDTO clone = new JVMSystemDiagnosticsSnapshotDTO();
        clone.contentRepositoryStorageUsage = cloneRepoUsage(contentRepositoryStorageUsage);
        clone.cpuCores = cpuCores;
        clone.cpuLoadAverage = cpuLoadAverage;
        clone.flowFileRepositoryStorageUsage = flowFileRepositoryStorageUsage == null ? null : flowFileRepositoryStorageUsage.clone();
        clone.maxHeap = maxHeap;
        clone.maxHeapBytes = maxHeapBytes;
        clone.maxOpenFileDescriptors = maxOpenFileDescriptors;
        clone.openFileDescriptors = openFileDescriptors;
        clone.physicalMemory = physicalMemory;
        clone.physicalMemoryBytes = physicalMemoryBytes;
        clone.provenanceRepositoryStorageUsage = cloneRepoUsage(provenanceRepositoryStorageUsage);

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
}
