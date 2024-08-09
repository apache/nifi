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
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;

import jakarta.xml.bind.annotation.XmlType;

import java.util.List;
import java.util.Set;

@XmlType(name = "parameter")
public class ParameterDTO {
    private String name;
    private String description;
    private Boolean sensitive;
    private String value;
    private Boolean valueRemoved;
    private Boolean provided;
    private List<AssetReferenceDTO> referencedAssets;
    private Set<AffectedComponentEntity> referencingComponents;
    private ParameterContextReferenceEntity parameterContext;
    private Boolean inherited;

    @Schema(description = "The name of the Parameter")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "Whether or not the Parameter is inherited from another context", accessMode = Schema.AccessMode.READ_ONLY)
    public Boolean getInherited() {
        return inherited;
    }

    public void setInherited(final Boolean inherited) {
        this.inherited = inherited;
    }

    @Schema(description = "The description of the Parameter")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Schema(description = "Whether or not the Parameter is sensitive")
    public Boolean getSensitive() {
        return sensitive;
    }

    public void setSensitive(final Boolean sensitive) {
        this.sensitive = sensitive;
    }

    @Schema(description = "Whether or not the Parameter is provided by a ParameterProvider")
    public Boolean getProvided() {
        return provided;
    }

    public void setProvided(final Boolean provided) {
        this.provided = provided;
    }

    @Schema(description = "The value of the Parameter")
    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    @Schema(description = """
        Whether or not the value of the Parameter was removed.
        When a request is made to change a parameter, the value may be null.
        The absence of the value may be used either to indicate that the value is not to be changed, or that the value is to be set to null (i.e., removed).
        This denotes which of the two scenarios is being encountered.
        """
    )
    public Boolean getValueRemoved() {
        return valueRemoved;
    }

    public void setValueRemoved(final Boolean valueRemoved) {
        this.valueRemoved = valueRemoved;
    }

    @Schema(description = "The set of all components in the flow that are referencing this Parameter")
    public Set<AffectedComponentEntity> getReferencingComponents() {
        return referencingComponents;
    }

    public void setParameterContext(final ParameterContextReferenceEntity parameterContext) {
        this.parameterContext = parameterContext;
    }

    @Schema(description = "A reference to the Parameter Context that contains this one")
    public ParameterContextReferenceEntity getParameterContext() {
        return parameterContext;
    }

    public void setReferencingComponents(final Set<AffectedComponentEntity> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }

    @Schema(description = "A list of identifiers of the assets that are referenced by the parameter")
    public List<AssetReferenceDTO> getReferencedAssets() {
        return referencedAssets;
    }

    public void setReferencedAssets(final List<AssetReferenceDTO> referencedAssets) {
        this.referencedAssets = referencedAssets;
    }

    @Override
    public String toString() {
        return "ParameterDTO[name=" + name + ", sensitive=" + sensitive + ", value=" + (sensitive == Boolean.TRUE ? "********" : value) + (provided == Boolean.TRUE ? " (provided)" : "") + "]";
    }
}
