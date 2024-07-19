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
package org.apache.nifi.web.api.dto.status;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the performance status of a processor.
 */
@XmlType(name = "processingPerformanceStatus")
public class ProcessingPerformanceStatusDTO implements Cloneable {
    private String identifier;
    private long cpuDuration;
    private long contentReadDuration;
    private long contentWriteDuration;
    private long sessionCommitDuration;
    private long garbageCollectionDuration;

    @Schema(description = "The unique ID of the process group that the Processor belongs to")
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Schema(description = "The number of nanoseconds has spent on CPU usage in the last 5 minutes.")
    public long getCpuDuration() {
        return cpuDuration;
    }

    public void setCpuDuration(long cpuDuration) {
        this.cpuDuration = cpuDuration;
    }

    @Schema(description = "The number of nanoseconds has spent to read content in the last 5 minutes.")
    public long getContentReadDuration() {
        return contentReadDuration;
    }

    public void setContentReadDuration(long contentReadDuration) {
        this.contentReadDuration = contentReadDuration;
    }

    @Schema(description = "The number of nanoseconds has spent to write content in the last 5 minutes.")
    public long getContentWriteDuration() {
        return contentWriteDuration;
    }

    public void setContentWriteDuration(long contentWriteDuration) {
        this.contentWriteDuration = contentWriteDuration;
    }

    @Schema(description = "The number of nanoseconds has spent running to commit sessions the last 5 minutes.")
    public long getSessionCommitDuration() {
        return sessionCommitDuration;
    }

    public void setSessionCommitDuration(long sessionCommitDuration) {
        this.sessionCommitDuration = sessionCommitDuration;
    }

    @Schema(description = "The number of nanoseconds has spent running garbage collection in the last 5 minutes.")
    public long getGarbageCollectionDuration() {
        return garbageCollectionDuration;
    }

    public void setGarbageCollectionDuration(long garbageCollectionDuration) {
        this.garbageCollectionDuration = garbageCollectionDuration;
    }

    @Override
    public ProcessingPerformanceStatusDTO clone() {
        final ProcessingPerformanceStatusDTO clonedObj = new ProcessingPerformanceStatusDTO();

        clonedObj.identifier = identifier;
        clonedObj.cpuDuration = cpuDuration;
        clonedObj.contentReadDuration = contentReadDuration;
        clonedObj.contentWriteDuration = contentWriteDuration;
        clonedObj.sessionCommitDuration = sessionCommitDuration;
        clonedObj.garbageCollectionDuration = garbageCollectionDuration;
        return clonedObj;
    }
}
