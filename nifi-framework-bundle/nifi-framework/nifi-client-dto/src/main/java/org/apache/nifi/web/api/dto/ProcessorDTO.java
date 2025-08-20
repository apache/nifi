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
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Details for a processor within this NiFi.
 */
@XmlType(name = "processor")
public class ProcessorDTO extends ComponentDTO {
    public static final String VALID = "VALID";
    public static final String INVALID = "INVALID";
    public static final String VALIDATING = "VALIDATING";

    private String name;
    private String type;
    private BundleDTO bundle;
    private String state;
    private Map<String, String> style;
    private List<RelationshipDTO> relationships;
    private String description;
    private Boolean supportsParallelProcessing;
    private Boolean supportsBatching;
    private Boolean supportsSensitiveDynamicProperties;
    private Boolean persistsState;
    private Boolean restricted;
    private Boolean deprecated;
    private Boolean isExtensionMissing;
    private Boolean executionNodeRestricted;
    private Boolean multipleVersionsAvailable;
    private String inputRequirement;

    private ProcessorConfigDTO config;

    private Collection<String> validationErrors;
    private String validationStatus;

    /**
     * The name of this processor.
     *
     * @return This processors name
     */
    @Schema(description = "The name of the processor."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The type of this processor.
     *
     * @return This processors type
     */
    @Schema(description = "The type of the processor."
    )
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The details of the artifact that bundled this processor type.
     *
     * @return The bundle details
     */
    @Schema(description = "The details of the artifact that bundled this processor type."
    )
    public BundleDTO getBundle() {
        return bundle;
    }

    public void setBundle(BundleDTO bundle) {
        this.bundle = bundle;
    }

    /**
     * @return The state of this processor. Possible states are 'RUNNING', 'STOPPED', and 'DISABLED'
     */
    @Schema(description = "The state of the processor",
            allowableValues = {"RUNNING", "STOPPED", "DISABLED"}
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return The styles for this processor. (Currently only supports color)
     */
    @Schema(description = "Styles for the processor (background-color : #eee)."
    )
    public Map<String, String> getStyle() {
        return style;
    }

    public void setStyle(Map<String, String> style) {
        this.style = style;
    }

    /**
     * @return whether this processor supports parallel processing
     */
    @Schema(description = "Whether the processor supports parallel processing."
    )
    public Boolean getSupportsParallelProcessing() {
        return supportsParallelProcessing;
    }

    public void setSupportsParallelProcessing(Boolean supportsParallelProcessing) {
        this.supportsParallelProcessing = supportsParallelProcessing;
    }

    /**
     * @return whether this processor supports sensitive dynamic properties
     */
    @Schema(description = "Whether the processor supports sensitive dynamic properties."
    )
    public Boolean getSupportsSensitiveDynamicProperties() {
        return supportsSensitiveDynamicProperties;
    }

    public void setSupportsSensitiveDynamicProperties(final Boolean supportsSensitiveDynamicProperties) {
        this.supportsSensitiveDynamicProperties = supportsSensitiveDynamicProperties;
    }

    /**
     * @return whether this processor persists state
     */
    @Schema(description = "Whether the processor persists state."
    )
    public Boolean getPersistsState() {
        return persistsState;
    }

    public void setPersistsState(Boolean persistsState) {
        this.persistsState = persistsState;
    }

    /**
     * @return whether this processor has multiple versions available
     */
    @Schema(description = "Whether the processor has multiple versions available."
    )
    public Boolean getMultipleVersionsAvailable() {
        return multipleVersionsAvailable;
    }

    public void setMultipleVersionsAvailable(Boolean multipleVersionsAvailable) {
        this.multipleVersionsAvailable = multipleVersionsAvailable;
    }

    /**
     * @return whether the underlying extension is missing
     */
    @Schema(description = "Whether the underlying extension is missing."
    )
    public Boolean getExtensionMissing() {
        return isExtensionMissing;
    }

    public void setExtensionMissing(Boolean extensionMissing) {
        isExtensionMissing = extensionMissing;
    }

    /**
     * @return whether this processor requires elevated privileges
     */
    @Schema(description = "Whether the processor requires elevated privileges."
    )
    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    /**
     * @return Whether the processor has been deprecated.
     */
    @Schema(description = "Whether the processor has been deprecated."
    )
    public Boolean getDeprecated() {
        return deprecated;
    }

    public void setDeprecated(Boolean deprecated) {
        this.deprecated = deprecated;
    }

    /**
     * @return the input requirement of this processor
     */
    @Schema(description = "The input requirement for this processor."
    )
    public String getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(String inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    /**
     * @return whether this processor supports batching
     */
    @Schema(description = "Whether the processor supports batching. This makes the run duration settings available."
    )
    public Boolean getSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(Boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    /**
     * Gets the available relationships that this processor currently supports.
     *
     * @return The available relationships
     */
    @Schema(description = "The available relationships that the processor currently supports.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public List<RelationshipDTO> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<RelationshipDTO> relationships) {
        this.relationships = relationships;
    }

    /**
     * The configuration details for this processor. These details will be included in a response if the verbose flag is set to true.
     *
     * @return The processor configuration details
     */
    @Schema(description = "The configuration details for the processor. These details will be included in a response if the verbose flag is included in a request."
    )
    public ProcessorConfigDTO getConfig() {
        return config;
    }

    public void setConfig(ProcessorConfigDTO config) {
        this.config = config;
    }

    /**
     * Gets the validation errors from this processor. These validation errors represent the problems with the processor that must be resolved before it can be started.
     *
     * @return The validation errors
     */
    @Schema(description = "The validation errors for the processor. These validation errors represent the problems with the processor that must be resolved before it can be started."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "Indicates whether the Processor is valid, invalid, or still in the process of validating (i.e., it is unknown whether or not the Processor is valid)",
        accessMode = Schema.AccessMode.READ_ONLY,
        allowableValues = {VALID, INVALID, VALIDATING})
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    /**
     * @return the description for this processor
     */
    @Schema(description = "The description of the processor."
    )
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * @return whether or not this processor is restricted to run only in primary node
     */
    @Schema(description = "Indicates if the execution node of a processor is restricted to run only on the primary node"
    )
    public Boolean isExecutionNodeRestricted() {
        return executionNodeRestricted;
    }

    public void setExecutionNodeRestricted(Boolean executionNodeRestricted) {
        this.executionNodeRestricted = executionNodeRestricted;
    }
}
