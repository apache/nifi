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

import java.util.Map;

/**
 * The configuration for a property group.
 */
@XmlType(name = "propertyGroupConfiguration")
public class PropertyGroupConfigurationDTO {

    private String propertyGroupName;
    private String propertyGroupDescription;
    private Map<String, ConnectorPropertyDescriptorDTO> propertyDescriptors;
    private Map<String, ConnectorValueReferenceDTO> propertyValues;

    /**
     * @return the property group name
     */
    @Schema(description = "The name of the property group.")
    public String getPropertyGroupName() {
        return propertyGroupName;
    }

    public void setPropertyGroupName(final String propertyGroupName) {
        this.propertyGroupName = propertyGroupName;
    }

    /**
     * @return the property group description
     */
    @Schema(description = "The description of the property group.")
    public String getPropertyGroupDescription() {
        return propertyGroupDescription;
    }

    public void setPropertyGroupDescription(final String propertyGroupDescription) {
        this.propertyGroupDescription = propertyGroupDescription;
    }

    /**
     * @return the property descriptors keyed by property name
     */
    @Schema(description = "The property descriptors for this property group, keyed by property name.")
    public Map<String, ConnectorPropertyDescriptorDTO> getPropertyDescriptors() {
        return propertyDescriptors;
    }

    public void setPropertyDescriptors(final Map<String, ConnectorPropertyDescriptorDTO> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
    }

    /**
     * @return the property values
     */
    @Schema(description = "The property values for this property group.")
    public Map<String, ConnectorValueReferenceDTO> getPropertyValues() {
        return propertyValues;
    }

    public void setPropertyValues(final Map<String, ConnectorValueReferenceDTO> propertyValues) {
        this.propertyValues = propertyValues;
    }
}
