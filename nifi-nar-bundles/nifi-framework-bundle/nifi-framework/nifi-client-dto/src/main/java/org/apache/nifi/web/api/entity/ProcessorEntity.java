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
package org.apache.nifi.web.api.entity;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessorDTO.
 */
@XmlRootElement(name = "processorEntity")
public class ProcessorEntity extends ComponentEntity implements Permissible<ProcessorDTO>, OperationPermissible {

    private ProcessorDTO component;
    private String inputRequirement;
    private ProcessorStatusDTO status;
    private PermissionsDTO operatePermissions;

    /**
     * The ProcessorDTO that is being serialized.
     *
     * @return The ProcessorDTO object
     */
    public ProcessorDTO getComponent() {
        return component;
    }

    public void setComponent(ProcessorDTO component) {
        this.component = component;
    }

    /**
     * The Processor status.
     *
     * @return status
     */
    public ProcessorStatusDTO getStatus() {
        return status;
    }

    public void setStatus(ProcessorStatusDTO status) {
        this.status = status;
    }

    /**
     * @return the input requirement of this processor
     */
    @ApiModelProperty(
            value = "The input requirement for this processor."
    )
    public String getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(String inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    /**
     * @return The permissions for this component operations
     */
    @ApiModelProperty(
            value = "The permissions for this component operations."
    )
    @Override
    public PermissionsDTO getOperatePermissions() {
        return operatePermissions;
    }

    @Override
    public void setOperatePermissions(PermissionsDTO operatePermissions) {
        this.operatePermissions = operatePermissions;
    }
}
