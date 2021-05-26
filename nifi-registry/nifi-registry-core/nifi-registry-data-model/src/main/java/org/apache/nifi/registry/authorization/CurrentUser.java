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
package org.apache.nifi.registry.authorization;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
public class CurrentUser {

    private String identity;
    private boolean anonymous;
    private boolean loginSupported;
    private boolean oidcLoginSupported;
    private ResourcePermissions resourcePermissions;

    @ApiModelProperty(value = "The identity of the current user", readOnly = true)
    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    @ApiModelProperty(value = "Indicates if the current user is anonymous", readOnly = true)
    public boolean isAnonymous() {
        return anonymous;
    }

    public void setAnonymous(boolean anonymous) {
        this.anonymous = anonymous;
    }

    @ApiModelProperty(value = "Indicates if the NiFi Registry instance supports logging in")
    public boolean isLoginSupported() {
        return loginSupported;
    }

    @ApiModelProperty(value = "Indicates if the NiFi Registry instance supports logging in with an OIDC provider")
    public boolean isOIDCLoginSupported() {
        return oidcLoginSupported;
    }

    public void setLoginSupported(boolean loginSupported) {
        this.loginSupported = loginSupported;
    }

    public void setOIDCLoginSupported(boolean oidcLoginSupported) {
        this.oidcLoginSupported = oidcLoginSupported;
    }

    @ApiModelProperty(value = "The access that the current user has to top level resources", readOnly = true)
    public ResourcePermissions getResourcePermissions() {
        return resourcePermissions;
    }

    public void setResourcePermissions(ResourcePermissions resourcePermissions) {
        this.resourcePermissions = resourcePermissions;
    }
}
