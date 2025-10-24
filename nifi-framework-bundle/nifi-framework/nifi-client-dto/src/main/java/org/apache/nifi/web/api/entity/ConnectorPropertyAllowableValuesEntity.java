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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;

/**
 * Entity for holding allowable values for a connector property.
 */
@XmlRootElement(name = "connectorPropertyAllowableValuesEntity")
public class ConnectorPropertyAllowableValuesEntity extends Entity {

    private String configurationStepName;
    private String propertyGroupName;
    private String propertyName;
    private List<AllowableValueEntity> allowableValues;

    @Schema(description = "The name of the configuration step.")
    public String getConfigurationStepName() {
        return configurationStepName;
    }

    public void setConfigurationStepName(final String configurationStepName) {
        this.configurationStepName = configurationStepName;
    }

    @Schema(description = "The name of the property group.")
    public String getPropertyGroupName() {
        return propertyGroupName;
    }

    public void setPropertyGroupName(final String propertyGroupName) {
        this.propertyGroupName = propertyGroupName;
    }

    @Schema(description = "The name of the property.")
    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    @Schema(description = "The allowable values for the property.")
    public List<AllowableValueEntity> getAllowableValues() {
        return allowableValues;
    }

    public void setAllowableValues(final List<AllowableValueEntity> allowableValues) {
        this.allowableValues = allowableValues;
    }
}

