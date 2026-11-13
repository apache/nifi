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

import java.util.List;

/**
 * A component that keeps a Process Group drain from finishing because it has FlowFiles queued but cannot process them.
 */
@XmlType(name = "drainBlockingComponent")
public class DrainBlockingComponentDTO {

    private String id;
    private String groupId;
    private String remoteProcessGroupId;
    private String type;
    private String name;
    private String reason;
    private List<String> validationErrors;
    private Integer queuedCount;

    @Schema(description = "The identifier of the component.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "The identifier of the Process Group that the component belongs to. For a Remote Process Group Input Port, this is the Process Group "
        + "that contains the Remote Process Group.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The identifier of the Remote Process Group that the component belongs to. Only present when the component is a Remote Process Group Input Port.")
    public String getRemoteProcessGroupId() {
        return remoteProcessGroupId;
    }

    public void setRemoteProcessGroupId(final String remoteProcessGroupId) {
        this.remoteProcessGroupId = remoteProcessGroupId;
    }

    @Schema(description = "The type of the component.", allowableValues = {"PROCESSOR", "INPUT_PORT", "OUTPUT_PORT", "REMOTE_INPUT_PORT"})
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Schema(description = "The name of the component. Only present when the user is allowed to read the component.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The reason that the component cannot process its queued FlowFiles.", allowableValues = {"INVALID", "DISABLED"})
    public String getReason() {
        return reason;
    }

    public void setReason(final String reason) {
        this.reason = reason;
    }

    @Schema(description = "The validation errors of the component. Only present when the user is allowed to read the component.")
    public List<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(final List<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "The number of FlowFiles queued in the incoming connections of the component.")
    public Integer getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(final Integer queuedCount) {
        this.queuedCount = queuedCount;
    }
}
