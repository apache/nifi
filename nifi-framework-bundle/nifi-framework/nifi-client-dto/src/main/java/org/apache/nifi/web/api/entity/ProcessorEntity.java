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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessorDTO.
 */
@XmlRootElement(name = "processorEntity")
public class ProcessorEntity extends ComponentEntity implements Permissible<ProcessorDTO>, OperationPermissible {

    private ProcessorDTO component;
    private String inputRequirement;
    private String physicalState;
    private ProcessorStatusDTO status;
    private PermissionsDTO operatePermissions;

    /**
     * The ProcessorDTO that is being serialized.
     *
     * @return The ProcessorDTO object
     */
    @Override
    public ProcessorDTO getComponent() {
        return component;
    }

    @Override
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
    @Schema(description = "The input requirement for this processor."
    )
    public String getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(String inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    /**
     * @return the physical state of this processor
     */
    @Schema(description = "The physical state of the processor, including transition states",
            allowableValues = {"RUNNING", "STOPPED", "DISABLED", "STARTING", "STOPPING", "RUN_ONCE"})
    public String getPhysicalState() {
        return physicalState;
    }

    public void setPhysicalState(String physicalState) {
        this.physicalState = physicalState;
    }


    /**
     * @return The permissions for this component operations
     */
    @Schema(description = "The permissions for this component operations."
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
