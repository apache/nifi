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
package org.apache.nifi.jolt.util;

import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;

public enum JoltTransformStrategy implements DescribedValue {
    SHIFTR("jolt-transform-shift", "Shift", "Shift input JSON/data to create the output JSON."),
    CHAINR("jolt-transform-chain", "Chain", "Execute list of Jolt transformations."),
    DEFAULTR("jolt-transform-default", "Default", "Apply default values to the output JSON."),
    REMOVR("jolt-transform-remove", "Remove", "Remove values from input data to create the output JSON."),
    CARDINALITY("jolt-transform-card", "Cardinality", "Change the cardinality of input elements to create the output JSON."),
    SORTR("jolt-transform-sort", "Sort", "Sort input json key values alphabetically. Any specification set is ignored."),
    CUSTOMR("jolt-transform-custom", "Custom", "Custom Transformation. Requires Custom Transformation Class Name"),
    MODIFIER_DEFAULTR("jolt-transform-modify-default", "Modify - Default", "Writes when key is missing or value is null"),
    MODIFIER_OVERWRITER("jolt-transform-modify-overwrite", "Modify - Overwrite", "Always overwrite value"),
    MODIFIER_DEFINER("jolt-transform-modify-define", "Modify - Define", "Writes when key is missing");

    JoltTransformStrategy(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    private final String value;
    private final String displayName;
    private final String description;

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static JoltTransformStrategy get(String value) {
        return Arrays.stream(values())
                .filter(strategy -> strategy.getValue().equals(value))
                .findFirst()
                .orElse(CHAINR);
    }
}
