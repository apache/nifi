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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * User-provided configuration for a group of parameters fetched from a ParameterProvider.
 */
public class ParameterGroupConfiguration implements Comparable<ParameterGroupConfiguration> {

    private final String groupName;

    private final String parameterContextName;

    private final Map<String, ParameterSensitivity> parameterSensitivities;

    private final Boolean isSynchronized;

    /**
     * Creates a named group of parameter names.
     * @param groupName The parameter group name
     * @param parameterContextName The parameter context name to which parameters will be applied
     * @param parameterSensitivities A map from parameter name to desired sensitivity.  Any parameter not included in this map will not be included
     *                               when applied to the parameter context.
     * @param isSynchronized If true, indicates that a ParameterContext should be created if not already existing, or updated if existing
     */
    public ParameterGroupConfiguration(final String groupName, final String parameterContextName, final Map<String, ParameterSensitivity> parameterSensitivities,
                                       final Boolean isSynchronized) {
        this.groupName = Objects.requireNonNull(groupName, "Parameter group name is required");
        this.parameterContextName = Optional.ofNullable(parameterContextName).orElse(groupName);
        this.parameterSensitivities = Collections.unmodifiableMap(Objects.requireNonNull(parameterSensitivities, "Parameter sensitivity map is required"));
        this.isSynchronized = isSynchronized;
    }

    /**
     * @return The external parameter group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * @return The name of the ParameterContext that maps to this group
     */
    public String getParameterContextName() {
        return parameterContextName;
    }

    /**
     * @return A map from parameter name to desired sensitivity.  If the sensitivity is null, this indicates that the parameter
     * has not yet been configured by the user.
     */
    public Map<String, ParameterSensitivity> getParameterSensitivities() {
        return parameterSensitivities;
    }

    /**
     * @return True if this group should be synchronized with a parameter context.  If null, this indicates that
     * it has not yet been configured by the user.
     */
    public Boolean isSynchronized() {
        return isSynchronized;
    }

    @Override
    public int compareTo(final ParameterGroupConfiguration other) {
        if (other == null) {
            return -1;
        }

        final String groupName = getGroupName();
        final String otherGroupName = other.getGroupName();

        if (groupName == null) {
            return otherGroupName == null ? 0 : -1;
        }
        if (otherGroupName == null) {
            return 1;
        }
        return groupName.compareTo(otherGroupName);
    }
}
