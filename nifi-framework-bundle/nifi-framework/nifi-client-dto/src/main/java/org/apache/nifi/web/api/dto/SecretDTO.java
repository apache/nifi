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
 * DTO representing a secret's metadata. Note: The actual secret value is never exposed via the REST API.
 */
@XmlType(name = "secret")
public class SecretDTO {

    private String providerId;
    private String providerName;
    private String groupName;
    private String name;
    private String fullyQualifiedName;
    private String description;

    @Schema(description = "The identifier of the secret provider that manages this secret.")
    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(final String providerId) {
        this.providerId = providerId;
    }

    @Schema(description = "The name of the secret provider that manages this secret.")
    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(final String providerName) {
        this.providerName = providerName;
    }

    @Schema(description = "The name of the group this secret belongs to.")
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
    }

    @Schema(description = "The name of the secret.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The fully qualified name of the secret, including the group name.")
    public String getFullyQualifiedName() {
        return fullyQualifiedName;
    }

    public void setFullyQualifiedName(final String fullyQualifiedName) {
        this.fullyQualifiedName = fullyQualifiedName;
    }

    @Schema(description = "A description of the secret.")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }
}

