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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApiModel
public class ProcessorDefinition extends ExtensionComponent implements ConfigurableComponentDefinition {
    private static final long serialVersionUID = 1L;

    private Map<String, PropertyDescriptor> propertyDescriptors;
    private boolean supportsDynamicProperties;
    private InputRequirement.Requirement inputRequirement;

    private List<Relationship> supportedRelationships;
    private boolean supportsDynamicRelationships;

    @Override
    @ApiModelProperty("Descriptions of configuration properties applicable to this reporting task")
    public Map<String, PropertyDescriptor> getPropertyDescriptors() {
        return (propertyDescriptors != null ? Collections.unmodifiableMap(propertyDescriptors) : null);
    }

    @Override
    public void setPropertyDescriptors(LinkedHashMap<String, PropertyDescriptor> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
    }

    @Override
    @ApiModelProperty("Whether or not this processor makes use of dynamic (user-set) properties")
    public boolean getSupportsDynamicProperties() {
        return supportsDynamicProperties;
    }

    @Override
    public void setSupportsDynamicProperties(boolean supportsDynamicProperties) {
        this.supportsDynamicProperties = supportsDynamicProperties;
    }

    @ApiModelProperty("Any input requirements this processor has")
    public InputRequirement.Requirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement.Requirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @ApiModelProperty("The supported relationships for this processor")
    public List<Relationship> getSupportedRelationships() {
        return (supportedRelationships == null ? Collections.emptyList() : Collections.unmodifiableList(supportedRelationships));
    }

    public void setSupportedRelationships(List<Relationship> supportedRelationships) {
        this.supportedRelationships = supportedRelationships;
    }

    @ApiModelProperty("Whether or not this processor supports dynamic relationships")
    public boolean getSupportsDynamicRelationships() {
        return supportsDynamicRelationships;
    }

    public void setSupportsDynamicRelationships(boolean supportsDynamicRelationships) {
        this.supportsDynamicRelationships = supportsDynamicRelationships;
    }
}
