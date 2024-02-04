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
package org.apache.nifi.extension.manifest;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.Valid;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import java.util.List;
import java.util.Objects;

@XmlAccessorType(XmlAccessType.FIELD)
public class Extension {

    @Valid
    @XmlElement(required = true)
    private String name;

    @Valid
    @XmlElement(required = true)
    private ExtensionType type;

    private DeprecationNotice deprecationNotice;

    private String description;

    @XmlElementWrapper
    @XmlElement(name = "tag")
    private List<String> tags;

    @XmlElementWrapper
    @XmlElement(name = "property")
    private List<Property> properties;

    private boolean supportsSensitiveDynamicProperties;

    @XmlElementWrapper
    @XmlElement(name = "dynamicProperty")
    private List<DynamicProperty> dynamicProperties;

    @XmlElementWrapper
    @XmlElement(name = "relationship")
    private List<Relationship> relationships;

    private DynamicRelationship dynamicRelationship;

    @XmlElementWrapper
    @XmlElement(name = "readsAttribute")
    private List<Attribute> readsAttributes;

    @XmlElementWrapper
    @XmlElement(name = "writesAttribute")
    private List<Attribute> writesAttributes;

    private Stateful stateful;

    @Valid
    private Restricted restricted;

    private InputRequirement inputRequirement;

    @XmlElementWrapper
    @XmlElement(name = "systemResourceConsideration")
    private List<SystemResourceConsideration> systemResourceConsiderations;

    @XmlElementWrapper
    @XmlElement(name = "see")
    private List<String> seeAlso;

    @Valid
    @XmlElementWrapper
    @XmlElement(name = "providedServiceAPI")
    private List<ProvidedServiceAPI> providedServiceAPIs;

    private DefaultSettings defaultSettings;
    private DefaultSchedule defaultSchedule;

    private boolean triggerSerially;
    private boolean triggerWhenEmpty;
    private boolean triggerWhenAnyDestinationAvailable;
    private boolean supportsBatching;
    private boolean primaryNodeOnly;
    private boolean sideEffectFree;

    @XmlElementWrapper
    @XmlElement(name = "useCase")
    private List<UseCase> useCases;

    @XmlElementWrapper
    @XmlElement(name = "multiProcessorUseCase")
    private List<MultiProcessorUseCase> multiProcessorUseCases;

    @Schema(description = "The name of the extension")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The type of the extension")
    public ExtensionType getType() {
        return type;
    }

    public void setType(ExtensionType type) {
        this.type = type;
    }

    @Schema(description = "The deprecation notice of the extension")
    public DeprecationNotice getDeprecationNotice() {
        return deprecationNotice;
    }

    public void setDeprecationNotice(DeprecationNotice deprecationNotice) {
        this.deprecationNotice = deprecationNotice;
    }

    @Schema(description = "The description of the extension")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The tags of the extension")
    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Schema(description = "The properties of the extension")
    public List<Property> getProperties() {
        return properties;
    }

    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }

    public boolean getSupportsSensitiveDynamicProperties() {
        return supportsSensitiveDynamicProperties;
    }

    public void setSupportsSensitiveDynamicProperties(boolean supportsSensitiveDynamicProperties) {
        this.supportsSensitiveDynamicProperties = supportsSensitiveDynamicProperties;
    }

    @Schema(description = "The dynamic properties of the extension")
    public List<DynamicProperty> getDynamicProperties() {
        return dynamicProperties;
    }

    public void setDynamicProperties(List<DynamicProperty> dynamicProperties) {
        this.dynamicProperties = dynamicProperties;
    }

    @Schema(description = "The relationships of the extension")
    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
        this.relationships = relationships;
    }

    @Schema(description = "The dynamic relationships of the extension")
    public DynamicRelationship getDynamicRelationship() {
        return dynamicRelationship;
    }

    public void setDynamicRelationship(DynamicRelationship dynamicRelationship) {
        this.dynamicRelationship = dynamicRelationship;
    }

    @Schema(description = "The attributes read from flow files by the extension")
    public List<Attribute> getReadsAttributes() {
        return readsAttributes;
    }

    public void setReadsAttributes(List<Attribute> readsAttributes) {
        this.readsAttributes = readsAttributes;
    }

    @Schema(description = "The attributes written to flow files by the extension")
    public List<Attribute> getWritesAttributes() {
        return writesAttributes;
    }

    public void setWritesAttributes(List<Attribute> writesAttributes) {
        this.writesAttributes = writesAttributes;
    }

    @Schema(description = "The information about how the extension stores state")
    public Stateful getStateful() {
        return stateful;
    }

    public void setStateful(Stateful stateful) {
        this.stateful = stateful;
    }

    @Schema(description = "The restrictions of the extension")
    public Restricted getRestricted() {
        return restricted;
    }

    public void setRestricted(Restricted restricted) {
        this.restricted = restricted;
    }

    @Schema(description = "The input requirement of the extension")
    public InputRequirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @Schema(description = "The resource considerations of the extension")
    public List<SystemResourceConsideration> getSystemResourceConsiderations() {
        return systemResourceConsiderations;
    }

    public void setSystemResourceConsiderations(List<SystemResourceConsideration> systemResourceConsiderations) {
        this.systemResourceConsiderations = systemResourceConsiderations;
    }

    @Schema(description = "The names of other extensions to see")
    public List<String> getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(List<String> seeAlso) {
        this.seeAlso = seeAlso;
    }

    @Schema(description = "The service APIs provided by this extension")
    public List<ProvidedServiceAPI> getProvidedServiceAPIs() {
        return providedServiceAPIs;
    }

    public void setProvidedServiceAPIs(List<ProvidedServiceAPI> providedServiceAPIs) {
        this.providedServiceAPIs = providedServiceAPIs;
    }

    @Schema(description = "The default settings for a processor")
    public DefaultSettings getDefaultSettings() {
        return defaultSettings;
    }

    public void setDefaultSettings(DefaultSettings defaultSettings) {
        this.defaultSettings = defaultSettings;
    }

    @Schema(description = "The default schedule for a processor reporting task")
    public DefaultSchedule getDefaultSchedule() {
        return defaultSchedule;
    }

    public void setDefaultSchedule(DefaultSchedule defaultSchedule) {
        this.defaultSchedule = defaultSchedule;
    }

    @Schema(description = "Indicates that a processor should be triggered serially")
    public boolean getTriggerSerially() {
        return triggerSerially;
    }

    public void setTriggerSerially(boolean triggerSerially) {
        this.triggerSerially = triggerSerially;
    }

    @Schema(description = "Indicates that a processor should be triggered when the incoming queues are empty")
    public boolean getTriggerWhenEmpty() {
        return triggerWhenEmpty;
    }

    public void setTriggerWhenEmpty(boolean triggerWhenEmpty) {
        this.triggerWhenEmpty = triggerWhenEmpty;
    }

    @Schema(description = "Indicates that a processor should be triggered when any destinations have space for flow files")
    public boolean getTriggerWhenAnyDestinationAvailable() {
        return triggerWhenAnyDestinationAvailable;
    }

    public void setTriggerWhenAnyDestinationAvailable(boolean triggerWhenAnyDestinationAvailable) {
        this.triggerWhenAnyDestinationAvailable = triggerWhenAnyDestinationAvailable;
    }

    @Schema(description = "Indicates that a processor supports batching")
    public boolean getSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    @Schema(description = "Indicates that a processor should be scheduled only on the primary node")
    public boolean getPrimaryNodeOnly() {
        return primaryNodeOnly;
    }

    public void setPrimaryNodeOnly(boolean primaryNodeOnly) {
        this.primaryNodeOnly = primaryNodeOnly;
    }

    @Schema(description = "Indicates that a processor is side effect free")
    public boolean getSideEffectFree() {
        return sideEffectFree;
    }

    public void setSideEffectFree(boolean sideEffectFree) {
        this.sideEffectFree = sideEffectFree;
    }

    @Schema(description = "Zero or more documented use cases for how the extension may be used")
    public List<UseCase> getUseCases() {
        return useCases;
    }

    public void setUseCases(final List<UseCase> useCases) {
        this.useCases = useCases;
    }

    @Schema(description = "Zero or more documented use cases for how the processor may be used in conjunction with other processors")
    public List<MultiProcessorUseCase> getMultiProcessorUseCases() {
        return multiProcessorUseCases;
    }

    public void setMultiProcessorUseCases(final List<MultiProcessorUseCase> multiProcessorUseCases) {
        this.multiProcessorUseCases = multiProcessorUseCases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Extension extension = (Extension) o;
        return Objects.equals(name, extension.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
