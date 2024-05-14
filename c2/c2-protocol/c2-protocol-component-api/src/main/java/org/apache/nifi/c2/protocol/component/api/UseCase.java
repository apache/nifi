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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;

import java.io.Serializable;
import java.util.List;

public class UseCase implements Serializable {
    private String description;
    private String notes;
    private List<String> keywords;
    private String configuration;
    private InputRequirement.Requirement inputRequirement;

    @Schema(description = "A description of the use case")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Schema(description = "Any pertinent notes about the use case")
    public String getNotes() {
        return notes;
    }

    public void setNotes(final String notes) {
        this.notes = notes;
    }

    @Schema(description = "Keywords that pertain to the use case")
    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(final List<String> keywords) {
        this.keywords = keywords;
    }

    @Schema(description = "A description of how to configure the Processor to perform the task described in the use case")
    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(final String configuration) {
        this.configuration = configuration;
    }

    @Schema(description = "Specifies whether an incoming FlowFile is expected for this use case")
    public InputRequirement.Requirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(final InputRequirement.Requirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }
}
