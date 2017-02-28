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

import javax.xml.bind.annotation.XmlType;
import java.util.Objects;

/**
 * Typed component within NiFi.
 */
@XmlType(name = "bundle")
public class BundleDTO {

    private String group;
    private String artifact;
    private String version;

    public BundleDTO() {
    }

    public BundleDTO(final String group, final String artifact, final String version) {
        this.group = group;
        this.artifact = artifact;
        this.version = version;
    }

    /**
     * The group of the bundle.
     *
     * @return bundle group
     */
    @ApiModelProperty(
            value = "The group of the bundle."
    )
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * The artifact of the bundle.
     *
     * @return bundle artifact
     */
    @ApiModelProperty(
            value = "The artifact of the bundle."
    )
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    /**
     * The version of the bundle.
     *
     * @return bundle version
     */
    @ApiModelProperty(
            value = "The version of the bundle."
    )
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final BundleDTO bundleDTO = (BundleDTO) o;
        return Objects.equals(group, bundleDTO.group) && Objects.equals(artifact, bundleDTO.artifact) && Objects.equals(version, bundleDTO.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version);
    }
}
