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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * A reference to a defined type identified by bundle and fully qualified class type identifiers
 */
@ApiModel
public class DefinedType implements Serializable {
    private static final long serialVersionUID = 1L;

    private String group;
    private String artifact;
    private String version;
    private String type;
    private String typeDescription;

    @ApiModelProperty("The group name of the bundle that provides the referenced type.")
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @ApiModelProperty("The artifact name of the bundle that provides the referenced type.")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    @ApiModelProperty("The version of the bundle that provides the referenced type.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @ApiModelProperty(
        value = "The fully-qualified class type",
        required = true,
        notes = "For example, 'org.apache.nifi.GetFile' or 'org::apache:nifi::minifi::GetFile'")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @ApiModelProperty("The description of the type.")
    public String getTypeDescription() {
        return typeDescription;
    }

    public void setTypeDescription(String typeDescription) {
        this.typeDescription = typeDescription;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DefinedType that = (DefinedType) o;

        return Objects.equals(group, that.group)
                && Objects.equals(artifact, that.artifact)
                && Objects.equals(version, that.version)
                && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version, type);
    }
}
