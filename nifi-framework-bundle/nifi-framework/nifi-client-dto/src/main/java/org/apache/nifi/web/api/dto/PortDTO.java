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
import java.util.Collection;

/**
 * The details for a port within this NiFi flow.
 */
@XmlType(name = "port")
public class PortDTO extends ComponentDTO {

    private String name;
    private String comments;
    private String state;
    private String type;
    private Boolean transmitting;
    private Integer concurrentlySchedulableTaskCount;
    private Boolean allowRemoteAccess;
    private String portFunction;

    private Collection<String> validationErrors;

    /**
     * @return name of this port
     */
    @Schema(description = "The name of the port."
    )
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return The state of this port. Possible states are 'RUNNING', 'STOPPED', and 'DISABLED'
     */
    @Schema(description = "The state of the port.",
            allowableValues = {"RUNNING", "STOPPED", "DISABLED"}
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    /**
     * The type of port. Possible values are 'INPUT_PORT' or 'OUTPUT_PORT'.
     *
     * @return The type of port
     */
    @Schema(description = "The type of port.",
            allowableValues = {"INPUT_PORT", "OUTPUT_PORT"}
    )
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return number of tasks that should be concurrently scheduled for this port
     */
    @Schema(description = "The number of tasks that should be concurrently scheduled for the port."
    )
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    /**
     * @return comments for this port
     */
    @Schema(description = "The comments for the port."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return whether this port has incoming or outgoing connections to a remote NiFi. This is only applicable when the port is allowed to be accessed remotely.
     */
    @Schema(description = "Whether the port has incoming or output connections to a remote NiFi. This is only applicable when the port is allowed to be accessed remotely."
    )
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * Gets the validation errors from this port. These validation errors represent the problems with the port that must be resolved before it can be started.
     *
     * @return The validation errors
     */
    @Schema(description = "Gets the validation errors from this port. These validation errors represent the problems with the port that must be resolved before it can be started."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }


    /**
     * @return whether this port can be accessed remotely via Site-to-Site protocol.
     */
    @Schema(description = "Whether this port can be accessed remotely via Site-to-Site protocol."
    )
    public Boolean getAllowRemoteAccess() {
        return allowRemoteAccess;
    }

    public void setAllowRemoteAccess(Boolean allowRemoteAccess) {
        this.allowRemoteAccess = allowRemoteAccess;
    }

    @Schema(description = "Specifies how the Port functions",
        allowableValues = {"STANDARD", "FAILURE"}
    )
    public String getPortFunction() {
        return portFunction;
    }

    public void setPortFunction(final String portFunction) {
        this.portFunction = portFunction;
    }
}
