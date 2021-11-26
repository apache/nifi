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
import java.util.Set;

/**
 * Encapsulates a named group of externally fetched parameters names.
 */
public class ProvidedParameterNameGroup extends AbstractParameterGroup<String> implements Comparable<ProvidedParameterNameGroup> {

    /**
     * Creates a named group of parameter names with a specific sensitivity.
     * @param groupName The parameter group name
     * @param sensitivity The parameter sensitivity
     * @param parameterNames A set of parameter names
     */
    public ProvidedParameterNameGroup(final String groupName, final ParameterSensitivity sensitivity, final Set<String> parameterNames) {
        super(groupName, sensitivity, Collections.unmodifiableSet(parameterNames));
    }

    /**
     * Creates an unnamed group of parameter names with a specific sensitivity.
     * @param sensitivity The parameter sensitivity
     * @param parameterNames A set of parameter names
     */
    public ProvidedParameterNameGroup(final ParameterSensitivity sensitivity, final Set<String> parameterNames) {
        super(sensitivity, Collections.unmodifiableSet(parameterNames));
    }

    @Override
    public int compareTo(final ProvidedParameterNameGroup other) {
        if (other == null) {
            return -1;
        }

        final String groupName = getGroupKey().getGroupName();
        final String otherGroupName = other.getGroupKey().getGroupName();

        if (groupName == null) {
            return otherGroupName == null ? 0 : -1;
        }
        if (otherGroupName == null) {
            return 1;
        }
        return groupName.compareTo(otherGroupName);
    }
}
