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
package org.apache.nifi.registry;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@ApiModel
public class RegistryConfiguration {

    private Boolean supportsManagedAuthorizer;
    private Boolean supportsConfigurableAuthorizer;
    private Boolean supportsConfigurableUsersAndGroups;

    /**
     * @return whether this NiFi Registry supports a managed authorizer. Managed authorizers can visualize users, groups,
     * and policies in the UI. This value is read only
     */
    @ApiModelProperty(
            value = "Whether this NiFi Registry supports a managed authorizer. Managed authorizers can visualize users, groups, and policies in the UI.",
            readOnly = true
    )
    public Boolean getSupportsManagedAuthorizer() {
        return supportsManagedAuthorizer;
    }

    public void setSupportsManagedAuthorizer(Boolean supportsManagedAuthorizer) {
        this.supportsManagedAuthorizer = supportsManagedAuthorizer;
    }

    /**
     * @return whether this NiFi Registry supports configurable users and groups. This value is read only
     */
    @ApiModelProperty(
            value = "Whether this NiFi Registry supports configurable users and groups.",
            readOnly = true
    )
    public Boolean getSupportsConfigurableUsersAndGroups() {
        return supportsConfigurableUsersAndGroups;
    }

    public void setSupportsConfigurableUsersAndGroups(Boolean supportsConfigurableUsersAndGroups) {
        this.supportsConfigurableUsersAndGroups = supportsConfigurableUsersAndGroups;
    }

    /**
     * @return whether this NiFi Registry supports a configurable authorizer. This value is read only
     */
    @ApiModelProperty(
            value = "Whether this NiFi Registry supports a configurable authorizer.",
            readOnly = true
    )
    public Boolean getSupportsConfigurableAuthorizer() {
        return supportsConfigurableAuthorizer;
    }

    public void setSupportsConfigurableAuthorizer(Boolean supportsConfigurableAuthorizer) {
        this.supportsConfigurableAuthorizer = supportsConfigurableAuthorizer;
    }

}
