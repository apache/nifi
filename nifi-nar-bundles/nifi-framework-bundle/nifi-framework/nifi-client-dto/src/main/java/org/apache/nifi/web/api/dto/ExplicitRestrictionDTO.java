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

/**
 * Class used for providing details about a components usage restriction.
 */
@XmlType(name = "explicitRestriction")
public class ExplicitRestrictionDTO {

    private RequiredPermissionDTO requiredPermission;
    private String explanation;

    /**
     * @return The required permission necessary for this restriction.
     */
    @ApiModelProperty(
            value = "The required permission necessary for this restriction."
    )
    public RequiredPermissionDTO getRequiredPermission() {
        return requiredPermission;
    }

    public void setRequiredPermission(RequiredPermissionDTO requiredPermission) {
        this.requiredPermission = requiredPermission;
    }

    /**
     * @return The description of why the usage of this component is restricted for this required permission.
     */
    @ApiModelProperty(
            value = "The description of why the usage of this component is restricted for this required permission."
    )
    public String getExplanation() {
        return explanation;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }
}
