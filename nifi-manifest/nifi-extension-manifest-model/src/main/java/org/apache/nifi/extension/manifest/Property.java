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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
public class Property {

    private String name;
    private String displayName;
    private String description;
    private String defaultValue;
    private ControllerServiceDefinition controllerServiceDefinition;

    @XmlElementWrapper
    @XmlElement(name = "allowableValue")
    private List<AllowableValue> allowableValues;

    private boolean required;
    private boolean sensitive;

    private boolean expressionLanguageSupported;
    private ExpressionLanguageScope expressionLanguageScope;

    private boolean dynamicallyModifiesClasspath;
    private boolean dynamic;

    @XmlElementWrapper
    @XmlElement(name = "dependency")
    private List<Dependency> dependencies;

    private ResourceDefinition resourceDefinition;

    @Schema(description = "The name of the property")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The display name")
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Schema(description = "The description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The default value")
    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Schema(description = "The controller service required by this property, or null if none is required")
    public ControllerServiceDefinition getControllerServiceDefinition() {
        return controllerServiceDefinition;
    }

    public void setControllerServiceDefinition(ControllerServiceDefinition controllerServiceDefinition) {
        this.controllerServiceDefinition = controllerServiceDefinition;
    }

    @Schema(description = "The allowable values for this property")
    public List<AllowableValue> getAllowableValues() {
        return allowableValues;
    }

    public void setAllowableValues(List<AllowableValue> allowableValues) {
        this.allowableValues = allowableValues;
    }

    @Schema(description = "Whether or not the property is required")
    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    @Schema(description = "Whether or not the property is sensitive")
    public boolean isSensitive() {
        return sensitive;
    }

    public void setSensitive(boolean sensitive) {
        this.sensitive = sensitive;
    }

    @Schema(description = "Whether or not expression language is supported")
    public boolean isExpressionLanguageSupported() {
        return expressionLanguageSupported;
    }

    public void setExpressionLanguageSupported(boolean expressionLanguageSupported) {
        this.expressionLanguageSupported = expressionLanguageSupported;
    }

    @Schema(description = "The scope of expression language support")
    public ExpressionLanguageScope getExpressionLanguageScope() {
        return expressionLanguageScope;
    }

    public void setExpressionLanguageScope(ExpressionLanguageScope expressionLanguageScope) {
        this.expressionLanguageScope = expressionLanguageScope;
    }

    @Schema(description = "Whether or not the processor dynamically modifies the classpath")
    public boolean isDynamicallyModifiesClasspath() {
        return dynamicallyModifiesClasspath;
    }

    public void setDynamicallyModifiesClasspath(boolean dynamicallyModifiesClasspath) {
        this.dynamicallyModifiesClasspath = dynamicallyModifiesClasspath;
    }

    @Schema(description = "Whether or not the processor is dynamic")
    public boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    @Schema(description = "The properties that this property depends on")
    public List<Dependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<Dependency> dependencies) {
        this.dependencies = dependencies;
    }

    @Schema(description = "The optional resource definition")
    public ResourceDefinition getResourceDefinition() {
        return resourceDefinition;
    }

    public void setResourceDefinition(ResourceDefinition resourceDefinition) {
        this.resourceDefinition = resourceDefinition;
    }
}
