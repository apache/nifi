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

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * A Controller Service that can be shared by other components
 */
@XmlType(name = "controllerService")
public class ControllerServiceDTO extends NiFiComponentDTO {

    private String name;
    private String type;
    private String comments;
    private String availability;
    private String state;
    private Boolean persistsState;

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;

    private String customUiUrl;
    private String annotationData;

    private Set<ControllerServiceReferencingComponentDTO> referencingComponents;

    private Collection<String> validationErrors;

    /**
     * @return controller service name
     */
    @ApiModelProperty(
            value = "The name of the controller service."
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
    @ApiModelProperty(
            value = "The type of the controller service."
    )
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return the comment for the Controller Service
     */
    @ApiModelProperty(
            value = "The comments for the controller service."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return Where this service is available. Possible values are NCM, NODE
     */
    @ApiModelProperty(
            value = "Where the servcie is available.",
            allowableValues = "NCM, NODE"
    )
    public String getAvailability() {
        return availability;
    }

    public void setAvailability(String availability) {
        this.availability = availability;
    }

    /**
     * @return whether this controller service persists state
     */
    @ApiModelProperty(
        value = "Whether the controller service persists state."
    )
    public Boolean getPersistsState() {
        return persistsState;
    }

    public void setPersistsState(Boolean persistsState) {
        this.persistsState = persistsState;
    }

    /**
     * @return The state of this controller service. Possible values are ENABLED, ENABLING, DISABLED, DISABLING
     */
    @ApiModelProperty(
            value = "The state of the controller service.",
            allowableValues = "ENABLED, ENABLING, DISABLED, DISABLING"
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
    @ApiModelProperty(
            value = "The properties of the controller service."
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
    @ApiModelProperty(
            value = "The descriptors for the controller service properties."
    )
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * @return the URL for this controller services custom configuration UI if applicable. Null otherwise
     */
    @ApiModelProperty(
            value = "The URL for the controller services custom configuration UI if applicable."
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
    @ApiModelProperty(
            value = "The annontation for the controller service. This is how the custom UI relays configuration to the controller service."
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
    @ApiModelProperty(
            value = "All components referencing this controller service."
    )
    public Set<ControllerServiceReferencingComponentDTO> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(Set<ControllerServiceReferencingComponentDTO> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }

    /**
     * Gets the validation errors from this controller service. These validation errors represent the problems with the controller service that must be resolved before it can be enabled.
     *
     * @return The validation errors
     */
    @ApiModelProperty(
            value = "The validation errors from the controller service. These validation errors represent the problems with the controller service that must be resolved before it can be enabled."
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
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
