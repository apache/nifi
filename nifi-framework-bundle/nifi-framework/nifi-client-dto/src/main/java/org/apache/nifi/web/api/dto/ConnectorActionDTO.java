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
 * Represents an action that can be performed on a Connector.
 */
@XmlType(name = "connectorAction")
public class ConnectorActionDTO {

    private String name;
    private String description;
    private Boolean allowed;
    private String reasonNotAllowed;

    @Schema(description = "The name of the action.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "A description of what this action does.")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Schema(description = "Whether this action is currently allowed to be performed, based on the state of the Connector. " +
                          "Note that a value of 'true' does not imply that the user has permission to perform the action.")
    public Boolean getAllowed() {
        return allowed;
    }

    public void setAllowed(final Boolean allowed) {
        this.allowed = allowed;
    }

    @Schema(description = "The reason why this action is not allowed, or null if the action is allowed.")
    public String getReasonNotAllowed() {
        return reasonNotAllowed;
    }

    public void setReasonNotAllowed(final String reasonNotAllowed) {
        this.reasonNotAllowed = reasonNotAllowed;
    }
}

