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
import org.apache.nifi.web.api.entity.AllowableValueEntity;

import java.util.List;
import java.util.Set;

/**
 * A description of a connector property.
 */
@XmlType(name = "connectorPropertyDescriptor")
public class ConnectorPropertyDescriptorDTO {

    private String name;
    private String description;
    private String defaultValue;
    private Boolean required;
    private String type;
    private List<AllowableValueEntity> allowableValues;
    private Boolean allowableValuesFetchable;
    private Set<ConnectorPropertyDependencyDTO> dependencies;

    /**
     * @return property name
     */
    @Schema(description = "The name of the property.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return An explanation of the meaning of the given property
     */
    @Schema(description = "The description of the property.")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * @return default value for this property
     */
    @Schema(description = "The default value for the property.")
    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(final String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * @return whether this property is required
     */
    @Schema(description = "Whether the property is required.")
    public Boolean getRequired() {
        return required;
    }

    public void setRequired(final Boolean required) {
        this.required = required;
    }

    /**
     * @return the property type (STRING, INTEGER, BOOLEAN, DOUBLE, STRING_LIST, etc.)
     */
    @Schema(description = "The type of the property (STRING, INTEGER, BOOLEAN, DOUBLE, STRING_LIST).")
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    /**
     * @return set of allowable values for this property. If empty then the allowable values are not constrained
     */
    @Schema(description = "Allowable values for the property. If empty then the allowed values are not constrained.")
    public List<AllowableValueEntity> getAllowableValues() {
        return allowableValues;
    }

    public void setAllowableValues(final List<AllowableValueEntity> allowableValues) {
        this.allowableValues = allowableValues;
    }

    /**
     * @return whether the allowable values can be fetched dynamically
     */
    @Schema(description = "Whether the allowable values are dynamically fetchable based on other property values.")
    public Boolean getAllowableValuesFetchable() {
        return allowableValuesFetchable;
    }

    public void setAllowableValuesFetchable(final Boolean allowableValuesFetchable) {
        this.allowableValuesFetchable = allowableValuesFetchable;
    }

    /**
     * @return the dependencies this property has on other properties
     */
    @Schema(description = "The dependencies this property has on other properties.")
    public Set<ConnectorPropertyDependencyDTO> getDependencies() {
        return dependencies;
    }

    public void setDependencies(final Set<ConnectorPropertyDependencyDTO> dependencies) {
        this.dependencies = dependencies;
    }
}

