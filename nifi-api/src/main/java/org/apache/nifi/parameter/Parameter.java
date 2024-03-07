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
package org.apache.nifi.parameter;

import org.apache.nifi.asset.Asset;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Parameter {
    private final ParameterDescriptor descriptor;
    private final String value;
    private final String parameterContextId;
    private final boolean provided;
    private final List<Asset> referencedAssets;


    private Parameter(final Builder builder) {
        this.descriptor = new ParameterDescriptor.Builder()
            .name(builder.name)
            .description(builder.description)
            .sensitive(builder.sensitive)
            .build();

        this.parameterContextId = builder.parameterContextId;
        this.provided = builder.provided;

        this.referencedAssets = builder.referencedAssets;
        if (this.referencedAssets == null || this.referencedAssets.isEmpty()) {
            this.value = builder.value;
        } else {
            this.value = referencedAssets.stream()
                .map(Asset::getFile)
                .map(File::getAbsolutePath)
                .collect(Collectors.joining(","));
        }
    }

    public ParameterDescriptor getDescriptor() {
        return descriptor;
    }

    public String getValue() {
        return value;
    }

    public List<Asset> getReferencedAssets() {
        return referencedAssets;
    }

    public String getParameterContextId() {
        return parameterContextId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Parameter parameter = (Parameter) o;
        return Objects.equals(descriptor, parameter.descriptor)
               && Objects.equals(value, parameter.value)
               && Objects.equals(parameterContextId, parameter.parameterContextId)
               && Objects.equals(referencedAssets, parameter.referencedAssets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor, value);
    }

    /**
     *
     * @return True if this parameter is provided by a ParameterProvider.
     */
    public boolean isProvided() {
        return provided;
    }

    public static class Builder {
        private String name;
        private String description;
        private boolean sensitive;
        private String value;
        private String parameterContextId;
        private boolean provided;
        private List<Asset> referencedAssets = List.of();

        public Builder fromParameter(final Parameter parameter) {
            descriptor(parameter.getDescriptor());
            this.parameterContextId = parameter.getParameterContextId();
            this.provided = parameter.isProvided();
            this.referencedAssets = parameter.getReferencedAssets() == null ? List.of() : parameter.getReferencedAssets();
            if (this.referencedAssets.isEmpty()) {
                this.value = parameter.getValue();
            }

            return this;
        }

        public Builder descriptor(final ParameterDescriptor descriptor) {
            this.name = descriptor.getName();
            this.description = descriptor.getDescription();
            this.sensitive = descriptor.isSensitive();
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        public Builder value(final String value) {
            this.value = value;
            this.referencedAssets = List.of();
            return this;
        }

        public Builder parameterContextId(final String parameterContextId) {
            this.parameterContextId = parameterContextId;
            return this;
        }

        public Builder provided(final Boolean provided) {
            this.provided = provided;
            return this;
        }

        public Builder referencedAssets(final List<Asset> referencedAssets) {
            this.referencedAssets = referencedAssets == null ? List.of() : referencedAssets;
            if (!this.referencedAssets.isEmpty()) {
                this.value = null;
            }

            return this;
        }

        public Parameter build() {
            if (name == null) {
                throw new IllegalStateException("Name or Descriptor is required");
            }
            if (value != null && referencedAssets != null && !referencedAssets.isEmpty()) {
                throw new IllegalStateException("A Parameter's value or referenced assets may be set but not both");
            }

            return new Parameter(this);
        }
    }
}
