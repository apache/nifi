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
package org.apache.nifi.registry;

/**
 * An immutable object for holding information about a type of processor
 * property.
 *
 */
public final class VariableDescriptor implements Comparable<VariableDescriptor> {

    /**
     * The name (or key) of the variable by which all access/lookups to the
     * value will occur. This is the mechanism of establishing identity and
     * comparing equality.
     */
    private final String name;

    /**
     * A brief description of the variable. This description is meant to be
     * displayed to a user or simply provide a mechanism of documenting intent.
     */
    private final String description;

    /**
     * indicates that the value for this variable should be considered sensitive
     * and protected whenever stored or represented
     */
    private final boolean sensitive;

    /**
     * Convenience constructor to create a descriptor based on name alone. To
     * include additional parameters use Builder instead.
     *
     * @param name name used as unique identifier for this descriptor
     */
    public VariableDescriptor(final String name) {
        this(new Builder(name));
    }

    protected VariableDescriptor(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.sensitive = builder.sensitive;
    }

    @Override
    public int compareTo(final VariableDescriptor o) {
        if (o == null) {
            return -1;
        }
        return getName().compareTo(o.getName());
    }

    public static final class Builder {

        private String name = null;
        private String description = "";
        private boolean sensitive = false;

        /**
         * Establishes the unique identifier or key name of the variable.
         *
         * @param name of the property
         */
        public Builder(final String name) {
            if (null == name || name.trim().isEmpty()) {
                throw new IllegalArgumentException("Name must not be null or empty");
            }
            this.name = name.trim();
        }

        /**
         * @param description of the variable
         * @return the builder
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        /**
         * @param sensitive true if sensitive; false otherwise
         * @return the builder
         */
        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        /**
         * @return a VariableDescriptor as configured
         *
         */
        public VariableDescriptor build() {
            return new VariableDescriptor(this);
        }
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof VariableDescriptor)) {
            return false;
        }
        if (this == other) {
            return true;
        }

        final VariableDescriptor desc = (VariableDescriptor) other;
        return this.name.equals(desc.name);
    }

    @Override
    public int hashCode() {
        return 797 + this.name.hashCode() * 97;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + name + "]";
    }

}
