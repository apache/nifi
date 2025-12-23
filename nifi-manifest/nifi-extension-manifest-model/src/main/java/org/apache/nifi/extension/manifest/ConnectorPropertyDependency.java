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
 * Represents a dependency that a connector property has on another property.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectorPropertyDependency {

    private String propertyName;

    @XmlElementWrapper
    @XmlElement(name = "dependentValue")
    private List<String> dependentValues;

    @Schema(description = "The name of the property that this property depends on")
    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    @Schema(description = "The values of the dependent property that enable this property")
    public List<String> getDependentValues() {
        return dependentValues;
    }

    public void setDependentValues(List<String> dependentValues) {
        this.dependentValues = dependentValues;
    }
}

