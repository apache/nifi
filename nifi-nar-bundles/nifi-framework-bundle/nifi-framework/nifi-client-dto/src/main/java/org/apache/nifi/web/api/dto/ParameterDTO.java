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
import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "parameter")
public class ParameterDTO {
    private String name;
    private String description;
    private Boolean sensitive;
    private String value;
    private Set<AffectedComponentEntity> referencingComponents;

    @ApiModelProperty("The name of the Parameter")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @ApiModelProperty("The description of the Parameter")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @ApiModelProperty("Whether or not the Parameter is sensitive")
    public Boolean getSensitive() {
        return sensitive;
    }

    public void setSensitive(final Boolean sensitive) {
        this.sensitive = sensitive;
    }

    @ApiModelProperty("The value of the Parameter")
    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    @ApiModelProperty("The set of all components in the flow that are referencing this Parameter")
    public Set<AffectedComponentEntity> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(final Set<AffectedComponentEntity> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }

    @Override
    public String toString() {
        return "ParameterDTO[name=" + name + ", sensitive=" + sensitive + ", value=" + (sensitive ? "********" : value) + "]";
    }
}
