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

public abstract class UpdateStepDTO {
    private String description;
    private boolean complete;
    private String failureReason;

    @ApiModelProperty(value = "Explanation of what happens in this step", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "Whether or not this step has completed", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    @ApiModelProperty(value = "An explanation of why this step failed, or null if this step did not fail", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }
}
