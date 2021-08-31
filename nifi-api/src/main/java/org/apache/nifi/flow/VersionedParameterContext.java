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
package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Set;

public class VersionedParameterContext extends VersionedComponent {
    private Set<VersionedParameter> parameters;
    private List<String> inheritedParameterContexts;
    private String description;
    private String parameterProvider;
    private String parameterGroupName;
    private Boolean isSynchronized;

    @ApiModelProperty("The description of the parameter context")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty("The parameters in the context")
    public Set<VersionedParameter> getParameters() {
        return parameters;
    }

    public void setParameters(Set<VersionedParameter> parameters) {
        this.parameters = parameters;
    }

    @ApiModelProperty("The names of additional parameter contexts from which to inherit parameters")
    public List<String> getInheritedParameterContexts() {
        return inheritedParameterContexts;
    }

    public void setInheritedParameterContexts(List<String> parameterContextNames) {
        this.inheritedParameterContexts = parameterContextNames;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.PARAMETER_CONTEXT;
    }

    @ApiModelProperty("The identifier of an optional parameter provider")
    public String getParameterProvider() {
        return parameterProvider;
    }

    public void setParameterProvider(String parameterProvider) {
        this.parameterProvider = parameterProvider;
    }

    @ApiModelProperty("The corresponding parameter group name fetched from the parameter provider, if applicable")
    public String getParameterGroupName() {
        return parameterGroupName;
    }

    public void setParameterGroupName(String parameterGroupName) {
        this.parameterGroupName = parameterGroupName;
    }

    @ApiModelProperty("True if the parameter provider is set and the context should receive updates when its parameters are next fetched")
    public Boolean isSynchronized() {
        return isSynchronized;
    }

    public void setSynchronized(Boolean aSynchronized) {
        isSynchronized = aSynchronized;
    }
}
