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
package org.apache.nifi.components;

import java.util.Objects;

/**
 * <p>
 * Represents a valid value for a {@link PropertyDescriptor}
 * </p>
 */
public class AllowableValue {

    private final String value;
    private final String displayName;
    private final String description;

    /**
     * Constructs a new AllowableValue with the given value and and the same
     * display name and no description.
     *
     * @param value that is allowed
     */
    public AllowableValue(final String value) {
        this(value, value);
    }

    /**
     * Constructs a new AllowableValue with the given value and display name and
     * no description
     *
     * @param value that is allowed
     * @param displayName to display for the value
     *
     * @throws NullPointerException if either argument is null
     */
    public AllowableValue(final String value, final String displayName) {
        this(value, displayName, null);
    }

    /**
     * Constructs a new AllowableValue with the given value, display name, and
     * description
     *
     * @param value that is valid
     * @param displayName to show for the value
     * @param description of the value
     *
     * @throws NullPointerException if identifier or value is null
     */
    public AllowableValue(final String value, final String displayName, final String description) {
        this.value = Objects.requireNonNull(value);
        this.displayName = Objects.requireNonNull(displayName);
        this.description = description;
    }

    /**
     * @return the value of this AllowableValue
     */
    public String getValue() {
        return value;
    }

    /**
     * @return a human-readable name for this AllowableValue
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * @return a description for this value, or <code>null</code> if no
     * description was provided
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return true if <code>this</code> is equal to <code>obj</code> of <code>obj</code> is the
     * same object as <code>this</code> or if <code>obj</code> is an instance of
     * <code>AllowableValue</code> and both have the same value, or if
     * <code>obj</code> is a String and is equal to
     * {@link #getValue() this.getValue()}.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof AllowableValue) {
            final AllowableValue other = (AllowableValue) obj;
            return (this.value.equals(other.getValue()));
        } else if (obj instanceof String) {
            return this.value.equals(obj);
        }

        return false;
    }

    /**
     * @return based solely off of the value
     */
    @Override
    public int hashCode() {
        return 23984731 + 17 * value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
