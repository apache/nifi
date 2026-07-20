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
 * Snapshot of how much data remains on the source system for a Processor or Connector. Each numeric
 * dimension is reported only when the component can determine it; null fields are omitted from the
 * JSON representation rather than serialized as explicit nulls, in keeping with the framework-wide
 * null-omission policy applied to REST entities.
 */
@XmlType(name = "backlog")
public class BacklogDTO {

    private Long flowFileCount;
    private String formattedFlowFileCount;
    private Long byteCount;
    private String formattedByteCount;
    private Long recordCount;
    private String formattedRecordCount;
    private String lastCaughtUp;
    private String formattedLastCaughtUp;
    private String precision;

    @Schema(description = "The number of FlowFiles remaining on the source, if the component can determine it.")
    public Long getFlowFileCount() {
        return flowFileCount;
    }

    public void setFlowFileCount(final Long flowFileCount) {
        this.flowFileCount = flowFileCount;
    }

    @Schema(description = "The FlowFile count formatted for display with locale-appropriate grouping (for example, '1,025'). Populated whenever flowFileCount is populated.")
    public String getFormattedFlowFileCount() {
        return formattedFlowFileCount;
    }

    public void setFormattedFlowFileCount(final String formattedFlowFileCount) {
        this.formattedFlowFileCount = formattedFlowFileCount;
    }

    @Schema(description = "The total number of bytes remaining on the source, if the component can determine it.")
    public Long getByteCount() {
        return byteCount;
    }

    public void setByteCount(final Long byteCount) {
        this.byteCount = byteCount;
    }

    @Schema(description = "The byte count formatted for display in human-readable units (for example, '4.89 GB'). Populated whenever byteCount is populated.")
    public String getFormattedByteCount() {
        return formattedByteCount;
    }

    public void setFormattedByteCount(final String formattedByteCount) {
        this.formattedByteCount = formattedByteCount;
    }

    @Schema(description = "The number of records remaining on the source, if the component can determine it.")
    public Long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(final Long recordCount) {
        this.recordCount = recordCount;
    }

    @Schema(description = "The record count formatted for display with locale-appropriate grouping (for example, '1,025'). Populated whenever recordCount is populated.")
    public String getFormattedRecordCount() {
        return formattedRecordCount;
    }

    public void setFormattedRecordCount(final String formattedRecordCount) {
        this.formattedRecordCount = formattedRecordCount;
    }

    @Schema(description = "The most recent moment the component observed itself as fully caught up with the source, formatted as an ISO-8601 UTC timestamp.")
    public String getLastCaughtUp() {
        return lastCaughtUp;
    }

    public void setLastCaughtUp(final String lastCaughtUp) {
        this.lastCaughtUp = lastCaughtUp;
    }

    @Schema(description = "Human-readable description of how long ago the component was last caught up, "
            + "for example '5 mins ago', 'in 2 hours', or 'now'. Populated whenever lastCaughtUp is populated.")
    public String getFormattedLastCaughtUp() {
        return formattedLastCaughtUp;
    }

    public void setFormattedLastCaughtUp(final String formattedLastCaughtUp) {
        this.formattedLastCaughtUp = formattedLastCaughtUp;
    }

    @Schema(description = "The precision of the numeric dimensions of this backlog.",
            allowableValues = {"EXACT", "AT_LEAST"})
    public String getPrecision() {
        return precision;
    }

    public void setPrecision(final String precision) {
        this.precision = precision;
    }
}
