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

@XmlType(name = "rebaseChange")
public class RebaseChangeDTO {
    private String componentId;
    private String componentName;
    private String componentType;
    private String differenceType;
    private String fieldName;
    private String localValue;
    private String registryValue;
    private String classification;
    private String conflictCode;
    private String conflictDetail;

    @Schema(description = "The ID of the component that was changed")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @Schema(description = "The name of the component that was changed")
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(final String componentName) {
        this.componentName = componentName;
    }

    @Schema(description = "The type of the component that was changed")
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(final String componentType) {
        this.componentType = componentType;
    }

    @Schema(description = "The type of difference detected for this change")
    public String getDifferenceType() {
        return differenceType;
    }

    public void setDifferenceType(final String differenceType) {
        this.differenceType = differenceType;
    }

    @Schema(description = "The name of the field that was changed, or null if not applicable")
    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Schema(description = "The local value of the field, or null if not applicable")
    public String getLocalValue() {
        return localValue;
    }

    public void setLocalValue(final String localValue) {
        this.localValue = localValue;
    }

    @Schema(description = "The registry value of the field, or null if not applicable")
    public String getRegistryValue() {
        return registryValue;
    }

    public void setRegistryValue(final String registryValue) {
        this.registryValue = registryValue;
    }

    @Schema(description = "The classification of this change: COMPATIBLE, CONFLICTING, or UNSUPPORTED")
    public String getClassification() {
        return classification;
    }

    public void setClassification(final String classification) {
        this.classification = classification;
    }

    @Schema(description = "A code identifying the type of conflict, or null if the change is not conflicting")
    public String getConflictCode() {
        return conflictCode;
    }

    public void setConflictCode(final String conflictCode) {
        this.conflictCode = conflictCode;
    }

    @Schema(description = "A detailed description of the conflict, or null if the change is not conflicting")
    public String getConflictDetail() {
        return conflictDetail;
    }

    public void setConflictDetail(final String conflictDetail) {
        this.conflictDetail = conflictDetail;
    }
}
