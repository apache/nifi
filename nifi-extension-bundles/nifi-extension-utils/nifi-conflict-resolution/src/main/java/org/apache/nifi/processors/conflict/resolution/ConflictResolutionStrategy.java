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
package org.apache.nifi.processors.conflict.resolution;

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.components.DescribedValue;

public enum ConflictResolutionStrategy implements DescribedValue {
    FAIL( "fail", "Handle file conflict as failure."),
    IGNORE("ignore", "Ignore conflict, do not change the original file."),
    REPLACE( "replace", "Replace existing file in case of conflict.");

    private static final Map<String, ConflictResolutionStrategy> ENUM_MAP = new HashMap<>();

    static {
        for (ConflictResolutionStrategy strategy : values()) {
            ENUM_MAP.put(strategy.getValue(), strategy);
        }
    }

    private final String value;
    private final String description;

    ConflictResolutionStrategy(final String value, String description) {
        this.value = value;
        this.description = description;
    }

    public static ConflictResolutionStrategy forValue(String value) {
        return ENUM_MAP.get(value);
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public String getDisplayName() {
        return this.value;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
