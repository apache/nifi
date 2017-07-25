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

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "varaibleRegistryUpdateStep")
public class VariableRegistryUpdateStepDTO {
    private String description;
    private boolean complete;
    private String failureReason;

    public VariableRegistryUpdateStepDTO() {
    }

    @ApiModelProperty(value = "Explanation of what happens in this step", readOnly = true)
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "Whether or not this step has completed", readOnly = true)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    @ApiModelProperty(value = "An explanation of why this step failed, or null if this step did not fail", readOnly = true)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }
}
