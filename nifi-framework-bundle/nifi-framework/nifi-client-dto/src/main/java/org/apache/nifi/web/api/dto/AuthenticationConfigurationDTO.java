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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

/**
 * Authentication Configuration endpoint and status information
 */
@XmlType(name = "authenticationConfiguration")
public class AuthenticationConfigurationDTO {

    private boolean externalLoginRequired;

    private boolean loginSupported;

    private String loginUri;

    private String logoutUri;

    @Schema(
            description = "Whether the system requires login through an external Identity Provider",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public boolean isExternalLoginRequired() {
        return externalLoginRequired;
    }

    public void setExternalLoginRequired(final boolean externalLoginRequired) {
        this.externalLoginRequired = externalLoginRequired;
    }

    @Schema(
            description = "Whether the system is configured to support login operations",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public boolean isLoginSupported() {
        return loginSupported;
    }

    public void setLoginSupported(final boolean loginSupported) {
        this.loginSupported = loginSupported;
    }

    @Schema(
            description = "Location for initiating login processing",
            accessMode = Schema.AccessMode.READ_ONLY,
            nullable = true
    )
    public String getLoginUri() {
        return loginUri;
    }

    public void setLoginUri(final String loginUri) {
        this.loginUri = loginUri;
    }

    @Schema(
            description = "Location for initiating logout processing",
            accessMode = Schema.AccessMode.READ_ONLY,
            nullable = true
    )
    public String getLogoutUri() {
        return logoutUri;
    }

    public void setLogoutUri(final String logoutUri) {
        this.logoutUri = logoutUri;
    }
}
