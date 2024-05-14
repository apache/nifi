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

@XmlType(name = "narCoordinate")
public class NarCoordinateDTO {

    private String group;
    private String artifact;
    private String version;

    public NarCoordinateDTO() {
    }

    public NarCoordinateDTO(final String group, final String artifact, final String version) {
        this.group = group;
        this.artifact = artifact;
        this.version = version;
    }

    @Schema(description = "The group of the NAR.")
    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    @Schema(description = "The artifact id of the NAR.")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(final String artifact) {
        this.artifact = artifact;
    }

    @Schema(description = "The version of the NAR.")
    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarCoordinateDTO that = (NarCoordinateDTO) o;
        return Objects.equals(group, that.group) && Objects.equals(artifact, that.artifact) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version);
    }
}
