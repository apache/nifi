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
 * Represents a property descriptor for a Connector.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectorProperty {

    private String name;
    private String description;
    private String defaultValue;
    private boolean required;
    private ConnectorPropertyType propertyType;
    private boolean allowableValuesFetchable;

    @XmlElementWrapper
    @XmlElement(name = "allowableValue")
    private List<AllowableValue> allowableValues;

    @XmlElementWrapper
    @XmlElement(name = "dependency")
    private List<ConnectorPropertyDependency> dependencies;

    @Schema(description = "The name of the property")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The description of the property")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The default value of the property")
    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Schema(description = "Whether or not the property is required")
    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    @Schema(description = "The type of the property")
    public ConnectorPropertyType getPropertyType() {
        return propertyType;
    }

    public void setPropertyType(ConnectorPropertyType propertyType) {
        this.propertyType = propertyType;
    }

    @Schema(description = "Whether or not the allowable values can be fetched dynamically")
    public boolean isAllowableValuesFetchable() {
        return allowableValuesFetchable;
    }

    public void setAllowableValuesFetchable(boolean allowableValuesFetchable) {
        this.allowableValuesFetchable = allowableValuesFetchable;
    }

    @Schema(description = "The allowable values for this property")
    public List<AllowableValue> getAllowableValues() {
        return allowableValues;
    }

    public void setAllowableValues(List<AllowableValue> allowableValues) {
        this.allowableValues = allowableValues;
    }

    @Schema(description = "The properties that this property depends on")
    public List<ConnectorPropertyDependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<ConnectorPropertyDependency> dependencies) {
        this.dependencies = dependencies;
    }
}

