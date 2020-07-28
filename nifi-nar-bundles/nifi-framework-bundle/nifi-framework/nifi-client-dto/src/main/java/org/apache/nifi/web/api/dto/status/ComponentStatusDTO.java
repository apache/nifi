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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * General DTO for serializing the status of a component.
 * This DTO is used to expose component status to API clients.
 * This DTO must NOT contain any sensitive information as it can be returned
 * to API clients regardless of having READ privilege to the component.
 */
@XmlType(name = "componentStatus")
public class ComponentStatusDTO{

    public static final String VALID = "VALID";
    public static final String INVALID = "INVALID";
    public static final String VALIDATING = "VALIDATING";

    private String runStatus;
    private String validationStatus;
    private Integer activeThreadCount;

    /**
     * Sub-classes should override this method to provide API documentation using ApiModelProperty annotation with allowable values.
     * @return the run status of the component
     */
    @ApiModelProperty(value = "The run status of this component",
            readOnly = true,
            allowableValues = "ENABLED, ENABLING, DISABLED, DISABLING")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    @ApiModelProperty(value = "Indicates whether the component is valid, invalid, or still in the process of validating" +
            " (i.e., it is unknown whether or not the component is valid)",
            readOnly = true,
            allowableValues = VALID + ", " + INVALID + ", " + VALIDATING)
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    /**
     * @return number of active threads for this component
     */
    @ApiModelProperty(
            value = "The number of active threads for the component."
    )
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

}
