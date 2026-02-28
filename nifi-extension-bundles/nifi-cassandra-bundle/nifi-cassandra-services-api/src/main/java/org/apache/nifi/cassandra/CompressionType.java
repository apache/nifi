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

package org.apache.nifi.cassandra;

import org.apache.nifi.components.DescribedValue;

public enum CompressionType implements DescribedValue {

    NONE("None", "Disable transport-level compression"),

    LZ4("LZ4", "Enable LZ4 transport-level compression"),

    SNAPPY("SNAPPY", "Enable Snappy transport-level compression");

    private final String displayName;
    private final String description;

    CompressionType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Safely resolves a CompressionType from a PropertyContext value.
     *
     * @param value property value
     * @return CompressionType
     */
    public static CompressionType fromValue(final String value) {
        return CompressionType.valueOf(value);
    }
}
