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
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;

import jakarta.xml.bind.annotation.XmlType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Controller Service that can be shared by other components
 */
@XmlType(name = "controllerService")
public class ControllerServiceDTO extends ComponentDTO {
    public static final String VALID = "VALID";
    public static final String INVALID = "INVALID";
    public static final String VALIDATING = "VALIDATING";

    private String name;
    private String type;
    private BundleDTO bundle;
    private List<ControllerServiceApiDTO> controllerServiceApis;
    private String comments;
    private String state;
    private Boolean persistsState;
    private Boolean restricted;
    private Boolean deprecated;
    private Boolean isExtensionMissing;
    private Boolean multipleVersionsAvailable;
    private Boolean supportsSensitiveDynamicProperties;

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
    private Set<String> sensitiveDynamicPropertyNames;

    private String customUiUrl;
    private String annotationData;

    private Set<ControllerServiceReferencingComponentEntity> referencingComponents;

    private Collection<String> validationErrors;
    private String validationStatus;
    private String bulletinLevel;

    /**
     * @return controller service name
     */
    @Schema(description = "The name of the controller service."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the controller service type
     */
    @Schema(description = "The type of the controller service."
    )
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The details of the artifact that bundled this controller service type.
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
     * Lists the APIs this Controller Service implements.
     *
     * @return The listing of implemented APIs
     */
    @Schema(description = "Lists the APIs this Controller Service implements."
    )
    public List<ControllerServiceApiDTO> getControllerServiceApis() {
        return controllerServiceApis;
    }

    public void setControllerServiceApis(List<ControllerServiceApiDTO> controllerServiceApis) {
        this.controllerServiceApis = controllerServiceApis;
    }

    /**
     * @return the comment for the Controller Service
     */
    @Schema(description = "The comments for the controller service."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return the level at which this controller service will report bulletins
     */
    @Schema(description = "The level at which the controller service will report bulletins."
    )
    public String getBulletinLevel() {
        return bulletinLevel;
    }

    public void setBulletinLevel(String bulletinLevel) {
        this.bulletinLevel = bulletinLevel;
    }

    /**
     * @return whether this controller service persists state
     */
    @Schema(description = "Whether the controller service persists state."
    )
    public Boolean getPersistsState() {
        return persistsState;
    }

    public void setPersistsState(Boolean persistsState) {
        this.persistsState = persistsState;
    }

    /**
     * @return whether this controller service requires elevated privileges
     */
    @Schema(description = "Whether the controller service requires elevated privileges."
    )
    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    /**
     * @return Whether the controller service has been deprecated.
     */
    @Schema(description = "Whether the ontroller service has been deprecated."
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
     * @return whether this controller service has multiple versions available
     */
    @Schema(description = "Whether the controller service has multiple versions available."
    )
    public Boolean getMultipleVersionsAvailable() {
        return multipleVersionsAvailable;
    }

    public void setMultipleVersionsAvailable(Boolean multipleVersionsAvailable) {
        this.multipleVersionsAvailable = multipleVersionsAvailable;
    }

    /**
     * @return whether this controller service supports sensitive dynamic properties
     */
    @Schema(description = "Whether the controller service supports sensitive dynamic properties."
    )
    public Boolean getSupportsSensitiveDynamicProperties() {
        return supportsSensitiveDynamicProperties;
    }

    public void setSupportsSensitiveDynamicProperties(final Boolean supportsSensitiveDynamicProperties) {
        this.supportsSensitiveDynamicProperties = supportsSensitiveDynamicProperties;
    }

    /**
     * @return The state of this controller service. Possible values are ENABLED, ENABLING, DISABLED, DISABLING
     */
    @Schema(description = "The state of the controller service.",
            allowableValues = {"ENABLED", "ENABLING", "DISABLED", "DISABLING"}
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return controller service properties
     */
    @Schema(description = "The properties of the controller service."
    )
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * @return descriptors for the controller service properties
     */
    @Schema(description = "The descriptors for the controller service properties."
    )
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * @return Set of sensitive dynamic property names
     */
    @Schema(description = "Set of sensitive dynamic property names"
    )
    public Set<String> getSensitiveDynamicPropertyNames() {
        return sensitiveDynamicPropertyNames;
    }

    public void setSensitiveDynamicPropertyNames(final Set<String> sensitiveDynamicPropertyNames) {
        this.sensitiveDynamicPropertyNames = sensitiveDynamicPropertyNames;
    }

    /**
     * @return the URL for this controller services custom configuration UI if applicable. Null otherwise
     */
    @Schema(description = "The URL for the controller services custom configuration UI if applicable."
    )
    public String getCustomUiUrl() {
        return customUiUrl;
    }

    public void setCustomUiUrl(String customUiUrl) {
        this.customUiUrl = customUiUrl;
    }

    /**
     * @return annotation data for this controller service
     */
    @Schema(description = "The annotation for the controller service. This is how the custom UI relays configuration to the controller service."
    )
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    /**
     * @return all components referencing this controller service
     */
    @Schema(description = "All components referencing this controller service."
    )
    public Set<ControllerServiceReferencingComponentEntity> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(Set<ControllerServiceReferencingComponentEntity> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }

    /**
     * Gets the validation errors from this controller service. These validation errors represent the problems with the controller service that must be resolved before it can be enabled.
     *
     * @return The validation errors
     */
    @Schema(description = """
                          The validation errors from the controller service.
                          These validation errors represent the problems with the controller service that must be resolved before it can be enabled.
                          """
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "Indicates whether the ControllerService is valid, invalid, or still in the process of validating (i.e., it is unknown whether or not the ControllerService is valid)",
        accessMode = Schema.AccessMode.READ_ONLY,
        allowableValues = {VALID, INVALID, VALIDATING})
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    @Override
    public int hashCode() {
        final String id = getId();
        return 37 + 3 * ((id == null) ? 0 : id.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (obj.getClass() != ControllerServiceDTO.class) {
            return false;
        }

        final ControllerServiceDTO other = (ControllerServiceDTO) obj;
        if (getId() == null || other.getId() == null) {
            return false;
        }

        return getId().equals(other.getId());
    }
}
