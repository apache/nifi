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

/**
 * Component representing a Connector instance.
 */
@XmlType(name = "connector")
public class ConnectorDTO extends ComponentDTO {
    private String name;
    private String type;
    private BundleDTO bundle;
    private String state; // RUNNING, STOPPED, DISABLED
    private String managedProcessGroupId;
    private ConnectorConfigurationDTO activeConfiguration;
    private ConnectorConfigurationDTO workingConfiguration;

    private Collection<String> validationErrors;
    private String validationStatus;
    private Boolean multipleVersionsAvailable;
    private Boolean extensionMissing;

    private String configurationUrl;
    private String detailsUrl;

    @Schema(description = "The name of the Connector.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The fully qualified type of the Connector.")
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Schema(description = "The details of the artifact that bundled this Connector type.")
    public BundleDTO getBundle() {
        return bundle;
    }

    public void setBundle(final BundleDTO bundle) {
        this.bundle = bundle;
    }

    @Schema(description = "The state of the Connector.", allowableValues = {"RUNNING", "STOPPED", "DISABLED"})
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @Schema(description = "The identifier of the root Process Group managed by this Connector.")
    public String getManagedProcessGroupId() {
        return managedProcessGroupId;
    }

    public void setManagedProcessGroupId(final String managedProcessGroupId) {
        this.managedProcessGroupId = managedProcessGroupId;
    }

    @Schema(description = "The active configuration of the Connector.")
    public ConnectorConfigurationDTO getActiveConfiguration() {
        return activeConfiguration;
    }

    public void setActiveConfiguration(final ConnectorConfigurationDTO activeConfiguration) {
        this.activeConfiguration = activeConfiguration;
    }

    @Schema(description = "The working configuration of the Connector.")
    public ConnectorConfigurationDTO getWorkingConfiguration() {
        return workingConfiguration;
    }

    public void setWorkingConfiguration(final ConnectorConfigurationDTO workingConfiguration) {
        this.workingConfiguration = workingConfiguration;
    }

    @Schema(description = "The validation errors for the connector.")
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(final Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
    }

    @Schema(description = "Indicates whether the Connector is valid, invalid, or still in the process of validating")
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(final String validationStatus) {
        this.validationStatus = validationStatus;
    }

    @Schema(description = "Whether multiple versions of this connector are available.")
    public Boolean getMultipleVersionsAvailable() {
        return multipleVersionsAvailable;
    }

    public void setMultipleVersionsAvailable(final Boolean multipleVersionsAvailable) {
        this.multipleVersionsAvailable = multipleVersionsAvailable;
    }

    @Schema(description = "The URL for this connector's configuration/wizard custom UI, if applicable.")
    public String getConfigurationUrl() {
        return configurationUrl;
    }

    public void setConfigurationUrl(final String configurationUrl) {
        this.configurationUrl = configurationUrl;
    }

    @Schema(description = "The URL for this connector's details custom UI, if applicable.")
    public String getDetailsUrl() {
        return detailsUrl;
    }

    public void setDetailsUrl(final String detailsUrl) {
        this.detailsUrl = detailsUrl;
    }

    @Schema(description = "Whether the extension for this connector is missing.")
    public Boolean getExtensionMissing() {
        return extensionMissing;
    }

    public void setExtensionMissing(final Boolean extensionMissing) {
        this.extensionMissing = extensionMissing;
    }
}
