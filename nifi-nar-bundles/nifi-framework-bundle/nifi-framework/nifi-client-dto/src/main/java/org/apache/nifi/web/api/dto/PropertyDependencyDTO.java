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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "propertyDependency")
public class PropertyDependencyDTO {
    private String propertyName;
    private Set<String> dependentValues;

    @ApiModelProperty("The name of the property that is being depended upon")
    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    @ApiModelProperty("The values for the property that satisfies the dependency, or null if the dependency is satisfied by the presence of any value for the associated property name")
    public Set<String> getDependentValues() {
        return dependentValues;
    }

    public void setDependentValues(final Set<String> dependentValues) {
        this.dependentValues = dependentValues;
    }
}
