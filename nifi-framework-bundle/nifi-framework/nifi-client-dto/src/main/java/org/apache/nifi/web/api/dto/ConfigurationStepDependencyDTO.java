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

import java.util.Set;

/**
 * A dependency that a configuration step has on another configuration step's property.
 */
@XmlType(name = "configurationStepDependency")
public class ConfigurationStepDependencyDTO {

    private String stepName;
    private String propertyName;
    private Set<String> dependentValues;

    /**
     * @return the name of the configuration step that this step depends on
     */
    @Schema(description = "The name of the configuration step that this step depends on.")
    public String getStepName() {
        return stepName;
    }

    public void setStepName(final String stepName) {
        this.stepName = stepName;
    }

    /**
     * @return the name of the property within the dependent step that must have a value
     */
    @Schema(description = "The name of the property within the dependent step that must have a value.")
    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    /**
     * @return the values of the dependent property that satisfy this dependency, or null if any non-null value satisfies the dependency
     */
    @Schema(description = "The values of the dependent property that satisfy this dependency. If null, any non-null value satisfies the dependency.")
    public Set<String> getDependentValues() {
        return dependentValues;
    }

    public void setDependentValues(final Set<String> dependentValues) {
        this.dependentValues = dependentValues;
    }
}

