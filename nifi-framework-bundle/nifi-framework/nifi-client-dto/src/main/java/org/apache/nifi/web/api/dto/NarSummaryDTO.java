/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.Objects;

@XmlType(name = "narSummary")
public class NarSummaryDTO {

    private String identifier;
    private NarCoordinateDTO coordinate;
    private NarCoordinateDTO dependencyCoordinate;

    private String buildTime;
    private String createdBy;
    private String digest;
    private String sourceType;
    private String sourceIdentifier;
    private int extensionCount;

    private String state;
    private String failureMessage;
    private boolean installComplete;

    public NarSummaryDTO() {
    }

    public NarSummaryDTO(final String identifier) {
        this.identifier = identifier;
    }

    @Schema(description = "The identifier of the NAR.")
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(final String identifier) {
        this.identifier = identifier;
    }

    @Schema(description = "The coordinate of the NAR.")
    public NarCoordinateDTO getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(final NarCoordinateDTO coordinate) {
        this.coordinate = coordinate;
    }

    @Schema(description = "The coordinate of another NAR that the this NAR is dependent on, or null if not dependent on another NAR.")
    public NarCoordinateDTO getDependencyCoordinate() {
        return dependencyCoordinate;
    }

    public void setDependencyCoordinate(final NarCoordinateDTO dependencyCoordinate) {
        this.dependencyCoordinate = dependencyCoordinate;
    }

    @Schema(description = "The time the NAR was built according to it's MANIFEST")
    public String getBuildTime() {
        return buildTime;
    }

    public void setBuildTime(final String buildTime) {
        this.buildTime = buildTime;
    }

    @Schema(description = "The plugin that created the NAR according to it's MANIFEST")
    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(final String createdBy) {
        this.createdBy = createdBy;
    }

    @Schema(description = "The hex digest of the NAR contents")
    public String getDigest() {
        return digest;
    }

    public void setDigest(final String digest) {
        this.digest = digest;
    }

    @Schema(description = "The source of this NAR")
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(final String sourceType) {
        this.sourceType = sourceType;
    }

    @Schema(description = "The identifier of the source of this NAR")
    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public void setSourceIdentifier(final String sourceIdentifier) {
        this.sourceIdentifier = sourceIdentifier;
    }

    @Schema(description = "The number of extensions contained in this NAR")
    public int getExtensionCount() {
        return extensionCount;
    }

    public void setExtensionCount(final int extensionCount) {
        this.extensionCount = extensionCount;
    }

    @Schema(description = "The state of the NAR (i.e. Installed, or not)")
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @Schema(description = "Information about why the installation failed, only populated when the state is failed")
    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(final String failureMessage) {
        this.failureMessage = failureMessage;
    }

    @Schema(description = "Indicates if the install task has completed")
    public boolean isInstallComplete() {
        return installComplete;
    }

    public void setInstallComplete(final boolean installComplete) {
        this.installComplete = installComplete;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarSummaryDTO that = (NarSummaryDTO) o;
        return Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier);
    }
}
