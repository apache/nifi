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

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Collection;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

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
    private Set<String> userAccessControl;
    private Set<String> groupAccessControl;

    private Collection<String> validationErrors;

    /**
     * @return name of this port
     */
    @ApiModelProperty(
            value = "The name of the port."
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
    @ApiModelProperty(
            value = "The state of the port.",
            allowableValues = "RUNNING, STOPPED, DISABLED"
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
    @ApiModelProperty(
            value = "The type of port.",
            allowableValues = "INPUT_PORT, OUTPUT_PORT"
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
    @ApiModelProperty(
            value = "The number of tasks that should be concurrently scheduled for the port."
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
    @ApiModelProperty(
            value = "The comments for the port."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return whether this port has incoming or outgoing connections to a remote NiFi. This is only applicable when the port is running on the root group
     */
    @ApiModelProperty(
            value = "Whether the port has incoming or output connections to a remote NiFi. This is only applicable when the port is running in the root group."
    )
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * @return groups that are allowed to access this port
     */
    @ApiModelProperty(
            value = "The user groups that are allowed to access the port."
    )
    public Set<String> getGroupAccessControl() {
        return groupAccessControl;
    }

    public void setGroupAccessControl(Set<String> groupAccessControl) {
        this.groupAccessControl = groupAccessControl;
    }

    /**
     * @return users that are allowed to access this port
     */
    @ApiModelProperty(
            value = "The users that are allowed to access the port."
    )
    public Set<String> getUserAccessControl() {
        return userAccessControl;
    }

    public void setUserAccessControl(Set<String> userAccessControl) {
        this.userAccessControl = userAccessControl;
    }

    /**
     * Gets the validation errors from this port. These validation errors represent the problems with the port that must be resolved before it can be started.
     *
     * @return The validation errors
     */
    @ApiModelProperty(
            value = "Gets the validation errors from this port. These validation errors represent the problems with the port that must be resolved before it can be started."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

}
