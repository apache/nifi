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

import java.util.Objects;

/**
 * An immutable class that holds information about a Parameter. Parameters are constructed through use of the {@link ParameterDescriptor.Builder} class.
 */
public class ParameterDescriptor {
    private final String name;
    private final String description;
    private final boolean sensitive;

    private ParameterDescriptor(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.sensitive = builder.sensitive;
    }

    /**
     * @return the name of the parameter
     */
    public String getName() {
        return name;
    }

    /**
     * @return a description of the parameter
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return whether or not the parameter is considered sensitive.
     */
    public boolean isSensitive() {
        return sensitive;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o){
            return true;
        }

        if (o == null || getClass() != o.getClass()){
            return false;
        }


        final ParameterDescriptor other = (ParameterDescriptor) o;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }


    public static class Builder {
        private String name;
        private String description = "";
        private boolean sensitive;

        public Builder name(final String name) {
            Objects.requireNonNull(name);

            this.name = name.trim();
            return this;
        }

        public Builder description(final String description) {
            this.description = description == null ? "" : description.trim();
            return this;
        }

        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        public Builder from(final ParameterDescriptor descriptor) {
            name(descriptor.getName());
            description(descriptor.getDescription());
            sensitive(descriptor.isSensitive());
            return this;
        }

        public ParameterDescriptor build() {
            if (name == null) {
                throw new IllegalStateException("Must specify Parameter Name");
            }

            return new ParameterDescriptor(this);
        }
    }
}
