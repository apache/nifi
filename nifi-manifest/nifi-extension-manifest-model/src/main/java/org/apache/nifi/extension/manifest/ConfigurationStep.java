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
package org.apache.nifi.extension.manifest;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;

import java.util.List;

/**
 * Represents a configuration step for a Connector.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ConfigurationStep {

    private String name;
    private String description;

    @XmlElementWrapper
    @XmlElement(name = "stepDependency")
    private List<ConfigurationStepDependency> stepDependencies;

    @XmlElementWrapper
    @XmlElement(name = "propertyGroup")
    private List<ConnectorPropertyGroup> propertyGroups;

    @Schema(description = "The name of the configuration step")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The description of the configuration step")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The dependencies that this step has on other steps")
    public List<ConfigurationStepDependency> getStepDependencies() {
        return stepDependencies;
    }

    public void setStepDependencies(List<ConfigurationStepDependency> stepDependencies) {
        this.stepDependencies = stepDependencies;
    }

    @Schema(description = "The property groups in this configuration step")
    public List<ConnectorPropertyGroup> getPropertyGroups() {
        return propertyGroups;
    }

    public void setPropertyGroups(List<ConnectorPropertyGroup> propertyGroups) {
        this.propertyGroups = propertyGroups;
    }
}

