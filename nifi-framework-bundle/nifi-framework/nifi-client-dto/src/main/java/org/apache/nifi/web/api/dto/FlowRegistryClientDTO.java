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
import java.util.Map;
import java.util.Set;

/**
 * Details about a configured registry.
 */
@XmlType(name = "registry")
public class FlowRegistryClientDTO {
    public static final String VALID = "VALID";
    public static final String INVALID = "INVALID";
    public static final String VALIDATING = "VALIDATING";

    private String id;
    private String name;
    private String description;

    private String type;
    private BundleDTO bundle;

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
    private Set<String> sensitiveDynamicPropertyNames;
    private Boolean supportsSensitiveDynamicProperties;
    private Boolean supportsBranching;

    private Boolean restricted;
    private Boolean deprecated;
    private Boolean isExtensionMissing;
    private Boolean setMultipleVersionsAvailable;

    private Collection<String> validationErrors;
    private String validationStatus;
    private String annotationData;

    @Schema(description = "The registry identifier")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Schema(description = "The registry name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The registry description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The type of the registry client.")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Schema(description = "The details of the artifact that bundled this registry client type.")
    public BundleDTO getBundle() {
        return bundle;
    }

    public void setBundle(BundleDTO bundle) {
        this.bundle = bundle;
    }

    @Schema(description = "The properties of the registry client.")
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Schema(description = "The descriptors for the registry client properties.")
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    @Schema(description = "Set of sensitive dynamic property names")
    public Set<String> getSensitiveDynamicPropertyNames() {
        return sensitiveDynamicPropertyNames;
    }

    public void setSensitiveDynamicPropertyNames(final Set<String> sensitiveDynamicPropertyNames) {
        this.sensitiveDynamicPropertyNames = sensitiveDynamicPropertyNames;
    }

    @Schema(description = "Whether the registry client supports sensitive dynamic properties.")
    public Boolean getSupportsSensitiveDynamicProperties() {
        return supportsSensitiveDynamicProperties;
    }

    public void setSupportsSensitiveDynamicProperties(final Boolean supportsSensitiveDynamicProperties) {
        this.supportsSensitiveDynamicProperties = supportsSensitiveDynamicProperties;
    }

    @Schema(description = "Whether the registry client supports branching.")
    public Boolean getSupportsBranching() {
        return supportsBranching;
    }

    public void setSupportsBranching(final Boolean supportsBranching) {
        this.supportsBranching = supportsBranching;
    }

    @Schema(description = "Whether the registry client requires elevated privileges."
    )
    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    @Schema(description = "Whether the registry client has been deprecated."
    )
    public Boolean getDeprecated() {
        return deprecated;
    }

    public void setDeprecated(Boolean deprecated) {
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

    public void setExtensionMissing(Boolean extensionMissing) {
        isExtensionMissing = extensionMissing;
    }

    /**
     * @return whether this flow registry client has multiple versions available
     */
    @Schema(description = "Whether the flow registry client has multiple versions available."
    )
    public Boolean getMultipleVersionsAvailable() {
        return setMultipleVersionsAvailable;
    }

    public void setMultipleVersionsAvailable(Boolean setMultipleVersionsAvailable) {
        this.setMultipleVersionsAvailable = setMultipleVersionsAvailable;
    }

    @Schema(description = "The annotation data for the registry client. This is how the custom UI relays configuration to the registry client."
    )
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    @Schema(description = "Gets the validation errors from the registry client. These validation errors represent the problems with the registry client that must be resolved before "
            + "it can be used for interacting with the flow registry."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "Indicates whether the Registry Client is valid, invalid, or still in the process of validating (i.e., it is unknown whether or not the Registry Client is valid)",
            accessMode = Schema.AccessMode.READ_ONLY,
            allowableValues = {VALID, INVALID, VALIDATING})
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }
}
