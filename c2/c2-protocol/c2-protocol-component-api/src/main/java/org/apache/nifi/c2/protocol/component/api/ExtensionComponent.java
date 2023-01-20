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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A component provided by an extension bundle
 */
@ApiModel
public class ExtensionComponent extends DefinedType {
    private static final long serialVersionUID = 1L;

    private BuildInfo buildInfo;

    private List<DefinedType> providedApiImplementations;

    private Set<String> tags;
    private Set<String> seeAlso;

    private Boolean deprecated;
    private String deprecationReason;
    private Set<String> deprecationAlternatives;

    private Boolean restricted;
    private String restrictedExplanation;
    private Set<Restriction> explicitRestrictions;

    private Stateful stateful;
    private List<SystemResourceConsideration> systemResourceConsiderations;

    private boolean additionalDetails;

    @ApiModelProperty("The build metadata for this component")
    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    public void setBuildInfo(BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
    }

    @ApiModelProperty("If this type represents a provider for an interface, this lists the APIs it implements")
    public List<DefinedType> getProvidedApiImplementations() {
        return (providedApiImplementations != null ? Collections.unmodifiableList(providedApiImplementations) : null);

    }

    public void setProvidedApiImplementations(List<DefinedType> providedApiImplementations) {
        this.providedApiImplementations = providedApiImplementations;
    }

    @ApiModelProperty("The tags associated with this type")
    public Set<String> getTags() {
        return (tags != null ? Collections.unmodifiableSet(tags) : null);
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    @ApiModelProperty("The names of other component types that may be related")
    public Set<String> getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(Set<String> seeAlso) {
        this.seeAlso = seeAlso;
    }

    @ApiModelProperty("Whether or not the component has been deprecated")
    public Boolean getDeprecated() {
        return deprecated;
    }

    public void setDeprecated(Boolean deprecated) {
        this.deprecated = deprecated;
    }

    @ApiModelProperty("If this component has been deprecated, this optional field can be used to provide an explanation")
    public String getDeprecationReason() {
        return deprecationReason;
    }

    public void setDeprecationReason(String deprecationReason) {
        this.deprecationReason = deprecationReason;
    }

    @ApiModelProperty("If this component has been deprecated, this optional field provides alternatives to use")
    public Set<String> getDeprecationAlternatives() {
        return deprecationAlternatives;
    }

    public void setDeprecationAlternatives(Set<String> deprecationAlternatives) {
        this.deprecationAlternatives = deprecationAlternatives;
    }

    @ApiModelProperty("Whether or not the component has a general restriction")
    public Boolean isRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    @ApiModelProperty("An optional description of the general restriction")
    public String getRestrictedExplanation() {
        return restrictedExplanation;
    }

    public void setRestrictedExplanation(String restrictedExplanation) {
        this.restrictedExplanation = restrictedExplanation;
    }

    @ApiModelProperty("Explicit restrictions that indicate a require permission to use the component")
    public Set<Restriction> getExplicitRestrictions() {
        return explicitRestrictions;
    }

    public void setExplicitRestrictions(Set<Restriction> explicitRestrictions) {
        this.explicitRestrictions = explicitRestrictions;
    }

    @ApiModelProperty("Indicates if the component stores state")
    public Stateful getStateful() {
        return stateful;
    }

    public void setStateful(Stateful stateful) {
        this.stateful = stateful;
    }

    @ApiModelProperty("The system resource considerations for the given component")
    public List<SystemResourceConsideration> getSystemResourceConsiderations() {
        return systemResourceConsiderations;
    }

    public void setSystemResourceConsiderations(List<SystemResourceConsideration> systemResourceConsiderations) {
        this.systemResourceConsiderations = systemResourceConsiderations;
    }

    @ApiModelProperty("Indicates if the component has additional details documentation")
    public boolean isAdditionalDetails() {
        return additionalDetails;
    }

    public void setAdditionalDetails(boolean additionalDetails) {
        this.additionalDetails = additionalDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ExtensionComponent that = (ExtensionComponent) o;
        return super.equals(o) && Objects.equals(buildInfo, that.buildInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buildInfo);
    }

}
