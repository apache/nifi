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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * A component referencing a controller service. This can either be another
 * controller service or a processor. Depending on the type of component
 * different properties may be set.
 */
@XmlType(name = "controllerServiceReferencingComponent")
public class ControllerServiceReferencingComponentDTO {
    private String groupId;
    private String id;
    private String name;
    private String type;
    private String state;

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;
    
    private Collection<String> validationErrors;
    
    private String referenceType;
    private Integer activeThreadCount;
    
    private Boolean referenceCycle;
    private Set<ControllerServiceReferencingComponentDTO> referencingComponents;

    /**
     * Group id for this component referencing a controller service. If this
     * component is another service, this field is blank.
     * 
     * @return 
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * The id for this component referencing a controller service.
     * 
     * @return 
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The name for this component referencing a controller service.
     * 
     * @return 
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The type for this component referencing a controller service.
     * 
     * @return 
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The state of the processor referencing a controller service. If this
     * component is another service, this field is blank.
     * 
     * @return 
     */
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    /**
     * The type of reference this is (Processor, ControllerService, or ReportingTask).
     * @return 
     */
    public String getReferenceType() {
        return referenceType;
    }

    public void setReferenceType(String referenceType) {
        this.referenceType = referenceType;
    }

    /**
     * The component properties.
     * 
     * @return 
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * The descriptors for the components properties.
     * 
     * @return 
     */
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }
    
    /**
     * Any validation error associated with this component.
     * 
     * @return 
     */
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }
    
    /**
     * The active thread count for the referencing component.
     * 
     * @return 
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * If this referencing component represents a ControllerService, these
     * are the components that reference it.
     * 
     * @return 
     */
    public Set<ControllerServiceReferencingComponentDTO> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(Set<ControllerServiceReferencingComponentDTO> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }

    /**
     * If this referencing component represents a ControllerService, this indicates
     * whether it has already been represented in this hierarchy.
     * 
     * @return 
     */
    public Boolean getReferenceCycle() {
        return referenceCycle;
    }

    public void setReferenceCycle(Boolean referenceCycle) {
        this.referenceCycle = referenceCycle;
    }

}