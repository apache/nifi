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
package org.apache.nifi.registry.extension.component.manifest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.Valid;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.util.List;
import java.util.Objects;

@ApiModel
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


    @ApiModelProperty(value = "The name of the extension")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty(value = "The type of the extension")
    public ExtensionType getType() {
        return type;
    }

    public void setType(ExtensionType type) {
        this.type = type;
    }

    @ApiModelProperty(value = "The deprecation notice of the extension")
    public DeprecationNotice getDeprecationNotice() {
        return deprecationNotice;
    }

    public void setDeprecationNotice(DeprecationNotice deprecationNotice) {
        this.deprecationNotice = deprecationNotice;
    }

    @ApiModelProperty(value = "The description of the extension")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "The tags of the extension")
    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @ApiModelProperty(value = "The properties of the extension")
    public List<Property> getProperties() {
        return properties;
    }

    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }

    @ApiModelProperty(value = "The dynamic properties of the extension")
    public List<DynamicProperty> getDynamicProperties() {
        return dynamicProperties;
    }

    public void setDynamicProperties(List<DynamicProperty> dynamicProperties) {
        this.dynamicProperties = dynamicProperties;
    }

    @ApiModelProperty(value = "The relationships of the extension")
    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
        this.relationships = relationships;
    }

    @ApiModelProperty(value = "The dynamic relationships of the extension")
    public DynamicRelationship getDynamicRelationship() {
        return dynamicRelationship;
    }

    public void setDynamicRelationship(DynamicRelationship dynamicRelationship) {
        this.dynamicRelationship = dynamicRelationship;
    }

    @ApiModelProperty(value = "The attributes read from flow files by the extension")
    public List<Attribute> getReadsAttributes() {
        return readsAttributes;
    }

    public void setReadsAttributes(List<Attribute> readsAttributes) {
        this.readsAttributes = readsAttributes;
    }

    @ApiModelProperty(value = "The attributes written to flow files by the extension")
    public List<Attribute> getWritesAttributes() {
        return writesAttributes;
    }

    public void setWritesAttributes(List<Attribute> writesAttributes) {
        this.writesAttributes = writesAttributes;
    }

    @ApiModelProperty(value = "The information about how the extension stores state")
    public Stateful getStateful() {
        return stateful;
    }

    public void setStateful(Stateful stateful) {
        this.stateful = stateful;
    }

    @ApiModelProperty(value = "The restrictions of the extension")
    public Restricted getRestricted() {
        return restricted;
    }

    public void setRestricted(Restricted restricted) {
        this.restricted = restricted;
    }

    @ApiModelProperty(value = "The input requirement of the extension")
    public InputRequirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @ApiModelProperty(value = "The resource considerations of the extension")
    public List<SystemResourceConsideration> getSystemResourceConsiderations() {
        return systemResourceConsiderations;
    }

    public void setSystemResourceConsiderations(List<SystemResourceConsideration> systemResourceConsiderations) {
        this.systemResourceConsiderations = systemResourceConsiderations;
    }

    @ApiModelProperty(value = "The names of other extensions to see")
    public List<String> getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(List<String> seeAlso) {
        this.seeAlso = seeAlso;
    }

    @ApiModelProperty(value = "The service APIs provided by this extension")
    public List<ProvidedServiceAPI> getProvidedServiceAPIs() {
        return providedServiceAPIs;
    }

    public void setProvidedServiceAPIs(List<ProvidedServiceAPI> providedServiceAPIs) {
        this.providedServiceAPIs = providedServiceAPIs;
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
