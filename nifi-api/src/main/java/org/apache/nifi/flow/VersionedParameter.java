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
package org.apache.nifi.flow;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Objects;

public class VersionedParameter {

    private String name;
    private String description;
    private boolean sensitive;
    private boolean provided;
    private String value;
    private List<VersionedAsset> referencedAssets;

    @Schema(description = "The name of the parameter")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The description of the param")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "Whether or not the parameter value is sensitive")
    public boolean isSensitive() {
        return sensitive;
    }

    public void setSensitive(boolean sensitive) {
        this.sensitive = sensitive;
    }

    @Schema(description = "Whether or not the parameter value is provided by a ParameterProvider")
    public boolean isProvided() {
        return provided;
    }

    public void setProvided(boolean provided) {
        this.provided = provided;
    }

    @Schema(description = "The value of the parameter")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Schema(description = "The ID's of assets that are referenced by this parameter")
    public List<VersionedAsset> getReferencedAssets() {
        return referencedAssets;
    }

    public void setReferencedAssets(final List<VersionedAsset> referencedAssets) {
        this.referencedAssets = referencedAssets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedParameter that = (VersionedParameter) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
