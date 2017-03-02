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
package org.apache.nifi.processor;

/**
 * An immutable object for holding information about a type of relationship.
 *
 */
public final class Relationship implements Comparable<Relationship> {

    public static final Relationship SELF = new Relationship.Builder().build();
    public static final Relationship ANONYMOUS = new Relationship.Builder().name("").build();

    /**
     * The proper name of the relationship. Determines the relationship
     * 'identity'
     */
    private final String name;
    /**
     * A user displayable description of the purpose of this relationship.
     */
    private final String description;

    /**
     * The hash code, which is computed in the constructor because it is hashed
     * very frequently and the hash code is constant
     */
    private final int hashCode;

    /**
     * The flag which tells the controller to auto terminate this
     * relationship, so that the processor can be run even if it does
     * not have connections from this relationship
     */
    private final boolean isAutoTerminate;

    protected Relationship(final Builder builder) {
        this.name = builder.name == null ? null : builder.name.intern();
        this.description = builder.description;
        this.isAutoTerminate = builder.autoTerminate;
        this.hashCode = 301 + ( (name == null) ? 0 :this.name.hashCode() ); // compute only once, since it gets called a bunch and will never change
    }

    @Override
    public int compareTo(final Relationship o) {
        if (o == null) {
            return -1;
        }
        final String thisName = getName();
        final String thatName = o.getName();
        if (thisName == null && thatName == null) {
            return 0;
        }
        if (thisName == null) {
            return 1;
        }
        if (thatName == null) {
            return -1;
        }

        return thisName.compareTo(thatName);
    }

    public static final class Builder {

        private String name = "";
        private String description = "";
        private boolean autoTerminate = false;

        public Builder name(final String name) {
            if (null != name) {
                this.name = name;
            }
            return this;
        }

        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        public Builder autoTerminateDefault(boolean autoTerminate) {
            this.autoTerminate = autoTerminate;
            return this;
        }

        public Relationship build() {
            return new Relationship(this);
        }
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public boolean isAutoTerminated() {
        return this.isAutoTerminate;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof Relationship)) {
            return false;
        }
        if (this == other) {
            return true;
        }
        Relationship desc = (Relationship) other;
        return this.name.equals(desc.name);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
