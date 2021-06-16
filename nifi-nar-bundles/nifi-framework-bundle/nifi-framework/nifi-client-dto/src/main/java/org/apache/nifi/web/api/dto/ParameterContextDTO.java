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
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "parameterContext")
public class ParameterContextDTO {
    private String identifier;
    private String name;
    private String description;
    private Set<ParameterEntity> parameters;
    private Set<ProcessGroupEntity> boundProcessGroups;

    public void setId(String id) {
        this.identifier = id;
    }

    @ApiModelProperty(value = "The ID the Parameter Context.", readOnly = true)
    public String getId() {
        return identifier;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The Name of the Parameter Context.")
    public String getName() {
        return name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty("The Description of the Parameter Context.")
    public String getDescription() {
        return description;
    }

    public void setParameters(final Set<ParameterEntity> parameters) {
        this.parameters = parameters;
    }

    @ApiModelProperty("The Parameters for the Parameter Context")
    public Set<ParameterEntity> getParameters() {
        return parameters;
    }

    public void setBoundProcessGroups(final Set<ProcessGroupEntity> boundProcessGroups) {
        this.boundProcessGroups = boundProcessGroups;
    }

    @ApiModelProperty(value = "The Process Groups that are bound to this Parameter Context", readOnly = true)
    public Set<ProcessGroupEntity> getBoundProcessGroups() {
        return boundProcessGroups;
    }

    @Override
    public String toString() {
        return "ParameterContext[id=" + identifier + ", name=" + name + ", parameters=" + parameters + "]";
    }
}
