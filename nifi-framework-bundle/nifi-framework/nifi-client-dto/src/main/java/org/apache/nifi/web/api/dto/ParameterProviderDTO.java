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
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;

import jakarta.xml.bind.annotation.XmlType;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Component that is capable of providing parameters to NiFi from an external source
 */
@XmlType(name = "parameterProvider")
public class ParameterProviderDTO extends ComponentDTO {
    public static final String VALID = "VALID";
    public static final String INVALID = "INVALID";
    public static final String VALIDATING = "VALIDATING";

    private String name;
    private String type;
    private BundleDTO bundle;
    private String comments;
    private Boolean persistsState;
    private Boolean restricted;
    private Boolean deprecated;
    private Boolean isExtensionMissing;
    private Boolean multipleVersionsAvailable;

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
    private Collection<ParameterGroupConfigurationEntity> parameterGroupConfigurations;
    private Set<AffectedComponentEntity> affectedComponents;
    private Set<ParameterStatusDTO> parameterStatus;
    private Set<ParameterProviderReferencingComponentEntity> referencingParameterContexts;

    private String customUiUrl;
    private String annotationData;

    private Collection<String> validationErrors;
    private String validationStatus;

    /**
     * @return user-defined name of the parameter provider
     */
    @Schema(description = "The name of the parameter provider."
    )
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The set of all components in the flow that are referencing Parameters provided by this provider")
    public Set<AffectedComponentEntity> getAffectedComponents() {
        return affectedComponents;
    }

    public void setAffectedComponents(final Set<AffectedComponentEntity> affectedComponents) {
        this.affectedComponents = affectedComponents;
    }

    public void setReferencingParameterContexts(final Set<ParameterProviderReferencingComponentEntity> referencingParameterContexts) {
        this.referencingParameterContexts = referencingParameterContexts;
    }

    @Schema(description = "The status of all provided parameters for this parameter provider")
    public Set<ParameterStatusDTO> getParameterStatus() {
        return parameterStatus;
    }

    public void setParameterStatus(Set<ParameterStatusDTO> parameterStatus) {
        this.parameterStatus = parameterStatus;
    }

    @Schema(description = "The Parameter Contexts that reference this Parameter Provider", accessMode = Schema.AccessMode.READ_ONLY)
    public Set<ParameterProviderReferencingComponentEntity> getReferencingParameterContexts() {
        return referencingParameterContexts;
    }

    @Schema(description = "Configuration for any fetched parameter groups."
    )
    public Collection<ParameterGroupConfigurationEntity> getParameterGroupConfigurations() {
        return parameterGroupConfigurations;
    }

    public void setParameterGroupConfigurations(final Collection<ParameterGroupConfigurationEntity> parameterGroupConfigurations) {
        this.parameterGroupConfigurations = parameterGroupConfigurations;
    }

    /**
     * @return user-defined comments for the parameter provider
     */
    @Schema(description = "The comments of the parameter provider."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(final String comments) {
        this.comments = comments;
    }

    /**
     * @return type of parameter provider
     */
    @Schema(description = "The fully qualified type of the parameter provider."
    )
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    /**
     * The details of the artifact that bundled this parameter provider type.
     *
     * @return The bundle details
     */
    @Schema(description = "The details of the artifact that bundled this parameter provider type."
    )
    public BundleDTO getBundle() {
        return bundle;
    }

    public void setBundle(final BundleDTO bundle) {
        this.bundle = bundle;
    }

    /**
     * @return whether this parameter provider persists state
     */
    @Schema(description = "Whether the parameter provider persists state."
    )
    public Boolean getPersistsState() {
        return persistsState;
    }

    public void setPersistsState(final Boolean persistsState) {
        this.persistsState = persistsState;
    }

    /**
     * @return whether this parameter provider requires elevated privileges
     */
    @Schema(description = "Whether the parameter provider requires elevated privileges."
    )
    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(final Boolean restricted) {
        this.restricted = restricted;
    }

    /**
     * @return Whether the parameter provider has been deprecated.
     */
    @Schema(description = "Whether the parameter provider has been deprecated."
    )
    public Boolean getDeprecated() {
        return deprecated;
    }

    public void setDeprecated(final Boolean deprecated) {
        this.deprecated = deprecated;
    }

    /**
     * @return whether the underlying extension is missing
     */
    @Schema(description = "Whether the underlying extension is missing."
    )
    public Boolean getExtensionMissing() {
        return isExtensionMissing;
    }

    public void setExtensionMissing(final Boolean extensionMissing) {
        isExtensionMissing = extensionMissing;
    }

    /**
     * @return whether this parameter provider has multiple versions available
     */
    @Schema(description = "Whether the parameter provider has multiple versions available."
    )
    public Boolean getMultipleVersionsAvailable() {
        return multipleVersionsAvailable;
    }

    public void setMultipleVersionsAvailable(final Boolean multipleVersionsAvailable) {
        this.multipleVersionsAvailable = multipleVersionsAvailable;
    }

    /**
     * @return parameter provider's properties
     */
    @Schema(description = "The properties of the parameter provider."
    )
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * @return Map of property name to descriptor
     */
    @Schema(description = "The descriptors for the parameter providers properties."
    )
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * @return the URL for this parameter provider custom configuration UI if applicable. Null otherwise
     */
    @Schema(description = "The URL for the custom configuration UI for the parameter provider."
    )
    public String getCustomUiUrl() {
        return customUiUrl;
    }

    public void setCustomUiUrl(String customUiUrl) {
        this.customUiUrl = customUiUrl;
    }

    /**
     * @return currently configured annotation data for the parameter provider
     */
    @Schema(description = "The annotation data for the parameter provider. This is how the custom UI relays configuration to the parameter provider."
    )
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    /**
     * Gets the validation errors from this parameter provider. These validation errors represent the problems with the parameter provider that must be resolved before it can be scheduled to run.
     *
     * @return The validation errors
     */
    @Schema(description = "Gets the validation errors from the parameter provider. These validation errors represent the problems with the parameter provider that must be resolved before "
                    + "it can be scheduled to run."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "Indicates whether the Parameter Provider is valid, invalid, or still in the process of validating (i.e., it is unknown whether or not the Parameter Provider is valid)",
        accessMode = Schema.AccessMode.READ_ONLY,
        allowableValues = {VALID, INVALID, VALIDATING})
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

}
