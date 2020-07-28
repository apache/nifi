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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "runStatusDetails")
public class ProcessorRunStatusDetailsDTO {
    public static final String RUNNING = "Running";
    public static final String STOPPED = "Stopped";
    public static final String INVALID = "Invalid";
    public static final String VALIDATING = "Validating";
    public static final String DISABLED = "Disabled";


    private String id;
    private String name;
    private String runStatus;
    private int activeThreads;
    private Set<String> validationErrors;

    @ApiModelProperty("The ID of the processor")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @ApiModelProperty("The name of the processor")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @ApiModelProperty(
        value = "The run status of the processor",
        allowableValues = RUNNING + ", " + STOPPED + ", " + INVALID + ", " + VALIDATING + ", " + DISABLED
    )
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(final String runStatus) {
        this.runStatus = runStatus;
    }

    @ApiModelProperty("The current number of threads that the processor is currently using")
    public int getActiveThreadCount() {
        return activeThreads;
    }

    public void setActiveThreadCount(final int activeThreads) {
        this.activeThreads = activeThreads;
    }


    @ApiModelProperty("The processor's validation errors")
    public Set<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(final Set<String> validationErrors) {
        this.validationErrors = validationErrors;
    }
}
