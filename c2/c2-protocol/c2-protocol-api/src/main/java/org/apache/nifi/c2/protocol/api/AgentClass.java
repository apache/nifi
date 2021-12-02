/*
 * Apache NiFi - MiNiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

@ApiModel
public class AgentClass implements Serializable {
    private static final long serialVersionUID = 6513853015430858587L;

    public static final String NAME_ALLOWED_PATTERN = "^[^{}]{1,200}$";
    public static final int DESCRIPTION_MAX_SIZE = 8000;

    @NotNull
    @Pattern(regexp = NAME_ALLOWED_PATTERN)
    private String name;
    @Size(max = DESCRIPTION_MAX_SIZE)
    private String description;

    // TODO add support for one-to-many class-to-manifests.
    // Temporarily, until we can reliably support one-to-many, only allow at most 1 manifest per class.
    // See Swagger annotation below for how this restriction is conveyed in the API docs.
    @Size(max = 1)
    private Set<String> agentManifests;

    @ApiModelProperty(value = "A unique class name for the agent", required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("An optional description of this agent class")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "A list of agent manifest ids belonging to this class",
        notes = "Note that although this type is a list, currently it supports at most one agent manifest entry. " +
            "This restriction will be removed in future versions of the service.")
    public Set<String> getAgentManifests() {
        return agentManifests;
    }

    public void setAgentManifests(Set<String> agentManifests) {
        this.agentManifests = agentManifests;
    }

}
