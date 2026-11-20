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
import jakarta.xml.bind.annotation.XmlType;

/**
 * A summary of the active drain of a Process Group, so that a client can show that the group is being drained without requesting the drain itself.
 */
@XmlType(name = "processGroupDrainSummary")
public class ProcessGroupDrainSummaryDTO {

    private String id;
    private Boolean prepared;

    private Integer originalCount;
    private Long originalSize;
    private Integer currentCount;
    private Long currentSize;

    @Schema(description = "The identifier of the drain request.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "Whether the components that bring data into the Process Group have stopped and the other components have been started.")
    public Boolean getPrepared() {
        return prepared;
    }

    public void setPrepared(final Boolean prepared) {
        this.prepared = prepared;
    }

    @Schema(description = "The number of FlowFiles that were queued in the Process Group once the components that bring data into it stopped.")
    public Integer getOriginalCount() {
        return originalCount;
    }

    public void setOriginalCount(final Integer originalCount) {
        this.originalCount = originalCount;
    }

    @Schema(description = "The size in bytes of the FlowFiles that were queued in the Process Group once the components that bring data into it stopped.")
    public Long getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(final Long originalSize) {
        this.originalSize = originalSize;
    }

    @Schema(description = "The number of FlowFiles that are queued in the Process Group.")
    public Integer getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(final Integer currentCount) {
        this.currentCount = currentCount;
    }

    @Schema(description = "The size in bytes of the FlowFiles that are queued in the Process Group.")
    public Long getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(final Long currentSize) {
        this.currentSize = currentSize;
    }
}
