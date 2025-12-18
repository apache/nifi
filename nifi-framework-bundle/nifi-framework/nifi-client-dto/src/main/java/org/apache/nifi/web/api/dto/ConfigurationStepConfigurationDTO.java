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

import java.util.List;

/**
 * The configuration for a configuration step.
 */
@XmlType(name = "configurationStepConfiguration")
public class ConfigurationStepConfigurationDTO {

    private String configurationStepName;
    private String configurationStepDescription;
    private boolean documented;
    private List<PropertyGroupConfigurationDTO> propertyGroupConfigurations;

    /**
     * @return the configuration step name
     */
    @Schema(description = "The name of the configuration step.")
    public String getConfigurationStepName() {
        return configurationStepName;
    }

    public void setConfigurationStepName(final String configurationStepName) {
        this.configurationStepName = configurationStepName;
    }

    /**
     * @return the configuration step description
     */
    @Schema(description = "The description of the configuration step.")
    public String getConfigurationStepDescription() {
        return configurationStepDescription;
    }

    public void setConfigurationStepDescription(final String configurationStepDescription) {
        this.configurationStepDescription = configurationStepDescription;
    }

    /**
     * @return whether this step has extended documentation available
     */
    @Schema(description = "Whether extended documentation is available for this configuration step.")
    public boolean isDocumented() {
        return documented;
    }

    public void setDocumented(final boolean documented) {
        this.documented = documented;
    }

    /**
     * @return the property group configurations
     */
    @Schema(description = "The list of property group configurations for this configuration step.")
    public List<PropertyGroupConfigurationDTO> getPropertyGroupConfigurations() {
        return propertyGroupConfigurations;
    }

    public void setPropertyGroupConfigurations(final List<PropertyGroupConfigurationDTO> propertyGroupConfigurations) {
        this.propertyGroupConfigurations = propertyGroupConfigurations;
    }
}