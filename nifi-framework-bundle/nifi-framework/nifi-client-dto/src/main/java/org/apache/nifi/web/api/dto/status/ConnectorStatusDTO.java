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
 * DTO for serializing the status of a connector.
 */
@XmlType(name = "connectorStatus")
public class ConnectorStatusDTO extends ComponentStatusDTO {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private String runStatus;
    private String validationStatus;
    private Integer activeThreadCount;

    /**
     * The id of the connector.
     *
     * @return The connector id
     */
    @Schema(description = "The id of the connector."
    )
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The id of the group this connector belongs to.
     *
     * @return The group id
     */
    @Schema(description = "The id of the group this connector belongs to."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * The name of the connector.
     *
     * @return The connector name
     */
    @Schema(description = "The name of the connector."
    )
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * The type of the connector.
     *
     * @return The connector type
     */
    @Schema(description = "The type of the connector."
    )
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    /**
     * The run status of the connector.
     *
     * @return The run status
     */
    @Override
    @Schema(description = "The run status of the connector."
    )
    public String getRunStatus() {
        return runStatus;
    }

    @Override
    public void setRunStatus(final String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * The validation status of the connector.
     *
     * @return The validation status
     */
    @Override
    @Schema(description = "The validation status of the connector."
    )
    public String getValidationStatus() {
        return validationStatus;
    }

    @Override
    public void setValidationStatus(final String validationStatus) {
        this.validationStatus = validationStatus;
    }

    /**
     * The number of active threads for the connector.
     *
     * @return The active thread count
     */
    @Override
    @Schema(description = "The number of active threads for the connector."
    )
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    @Override
    public void setActiveThreadCount(final Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }
}
